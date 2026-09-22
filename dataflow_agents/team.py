"""Durable Codex jobs, handoffs and audit events; SQLite is the join authority."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


class TeamStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "team.sqlite"
        # Creation happens here, once, through the ordinary connector. Every
        # later connection opens read-write-only, so a writer that survives the
        # run being deleted cannot recreate an empty database.
        if not self.db_path.exists():
            with sqlite3.connect(self.db_path, timeout=30) as created:
                created.executescript("PRAGMA journal_mode=WAL;")
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, role TEXT, status TEXT,
                    attempt INTEGER, input_hash TEXT, result TEXT, updated REAL
                );
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL,
                    kind TEXT, role TEXT, job TEXT, detail TEXT
                );
            """)

    def connect(self):
        """Open the existing database, never create one.

        A background writer that finishes after the run directory was deleted
        must fail rather than recreate an empty database beside the history
        that is still being listed. ``mode=rw`` makes a missing file an error.
        """
        return sqlite3.connect(self.db_path.as_uri() + "?mode=rw", uri=True, timeout=30)

    def event(self, kind, role="leader", job="", **detail):
        with self.connect() as db:
            db.execute("INSERT INTO events(timestamp,kind,role,job,detail) VALUES(?,?,?,?,?)",
                       (time.time(), kind, role, job, canonical(detail)))

    def checkpoint(self, state, **detail):
        write_json(self.root / "status.json", {"state": state, "updated": time.time(), **detail})
        self.event("workflow.state", state=state, **detail)

    def cached(self, job, prompt):
        with self.connect() as db:
            row = db.execute("SELECT status,input_hash,result FROM jobs WHERE id=?", (job,)).fetchone()
        if row and row[0] == "completed" and row[1] == digest(prompt):
            return json.loads(row[2])
        return None

    def set_job(self, job, role, status, attempt, prompt, result=None):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO jobs VALUES(?,?,?,?,?,?,?)",
                       (job, role, status, attempt, digest(prompt), canonical(result), time.time()))

    def export(self):
        with self.connect() as db:
            rows = db.execute("SELECT seq,timestamp,kind,role,job,detail FROM events ORDER BY seq").fetchall()
            jobs = db.execute("SELECT id,role,status,attempt,updated FROM jobs ORDER BY id").fetchall()
        events = [{"seq": r[0], "timestamp": r[1], "event": r[2], "agent": r[3],
                   "job": r[4], "trace_id": self.root.name, **json.loads(r[5])} for r in rows]
        (self.root / "events.jsonl").write_text("".join(canonical(e) + "\n" for e in events), encoding="utf-8")
        write_json(self.root / "jobs.json", [dict(zip(("id", "role", "status", "attempt", "updated"), r)) for r in jobs])
        calls = [e for e in events if e["event"] == "agent.completed"]
        metrics = {"agent_calls": len(calls), "agent_latency_ms": sum(e.get("duration_ms", 0) for e in calls),
                   "failed_calls": sum(e["event"] == "agent.failed" for e in events),
                   "cache_hits": sum(e["event"] == "agent.cache_hit" for e in events)}
        metrics["input_tokens"] = 0
        metrics["output_tokens"] = 0
        for path in (self.root / "agents").glob("*/*/codex-events.jsonl"):
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get("type") == "turn.completed":
                    for key in ("input_tokens", "output_tokens"):
                        metrics[key] += event.get("usage", {}).get(key, 0)
        spec_path = self.root / "pipeline-spec.json"
        if spec_path.exists():
            spec = json.loads(spec_path.read_text())
            metrics["operator_reuse_ratio"] = sum(not s["proposal"] for s in spec["steps"]) / max(len(spec["steps"]), 1)
        spans = [{"trace_id":digest(self.root.name)[:32], "span_id":digest((e["job"],e["seq"]))[:16],
                  "name":"invoke_agent", "end_time_unix":e["timestamp"],
                  "duration_ms":e.get("duration_ms",0),
                  "attributes":{"gen_ai.agent.id":e["agent"], "gen_ai.operation.name":"invoke_agent", "dataflow.job.id":e["job"]}}
                 for e in calls]
        write_json(self.root / "traces.json", spans)
        write_json(self.root / "metrics.json", metrics)
        return metrics


class CodexTeamRuntime:
    """Local Codex workflow: leader, named jobs, mailbox events and barriers.

    Each role call is an independent Codex process. The controller owns routing,
    handoff, joins and state; no external multi-agent SDK is required.
    """
    def __init__(self, store, backend, attempts=2):
        self.store, self.backend, self.attempts = store, backend, attempts

    def ask(self, role, job, prompt, schema):
        import jsonschema
        cached = self.store.cached(job, prompt)
        if cached is not None:
            self.store.event("agent.cache_hit", role, job)
            return cached
        feedback = None
        for attempt in range(1, self.attempts + 1):
            payload = dict(prompt, repair_feedback=feedback, trace_id=self.store.root.name, job_id=job)
            directory = self.store.root / "agents" / job / str(attempt)
            directory.mkdir(parents=True, exist_ok=True)
            write_json(directory / "input.json", payload)
            self.store.set_job(job, role, "running", attempt, prompt)
            self.store.event("agent.started", role, job, attempt=attempt, **{"gen_ai.operation.name": "invoke_agent"})
            started = time.monotonic()
            try:
                answer = self.backend.ask(role, payload, schema=schema, directory=directory)
                jsonschema.validate(answer, schema)
                write_json(directory / "output.json", answer)
                self.store.set_job(job, role, "completed", attempt, prompt, answer)
                self.store.event("agent.completed", role, job, duration_ms=round((time.monotonic()-started)*1000), output_hash=digest(answer))
                return answer
            except Exception as exc:
                feedback = str(exc)[-1600:]
                self.store.set_job(job, role, "failed", attempt, prompt)
                self.store.event("agent.failed", role, job, error=feedback, attempt=attempt)
        raise RuntimeError(f"{role}/{job}: {feedback}")
