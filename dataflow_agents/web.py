"""HTTP API and small web host for the multi-Codex DataFlow workflow.

The API is deliberately a thin adapter around :class:`Orchestrator`: all
planning, operator binding, compilation, execution and verification remain in
the existing durable run workflow.
"""
from __future__ import annotations

import asyncio
import ast
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict
from pathlib import Path
from contextlib import contextmanager
from typing import Any

from .backend import build_backend
from .catalog import discover_operator_catalog, load_catalog, search_catalog
from .compiler import normalize_operator_defaults
from .contracts import SCHEMAS
from .codegen import RUNNER_FILENAME, write_pipeline_sources
from .diagnosis import (FAILURE_STATES, analysis_message, collect_evidence, failure_digest,
                        triage, triage_message)
from .serving import normalize_chat_url
from .execution import approve, execute
from .identities import IDENTITIES
from . import routing
from .orchestrator import Orchestrator, load_config
from .team import TeamStore, write_json
from .conversation import ConversationStore, classify_with_source, message as conversation_message
from .skills import SkillRegistry

try:  # Keep importing the core package possible without the optional web deps.
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
except ImportError as exc:  # pragma: no cover - exercised only in minimal installs
    raise RuntimeError("Install the web extras with: pip install -e '.[web]'") from exc


TERMINAL_STATES = {"READY", "VERIFIED", "EXECUTED", "REFUSED", "BLOCKED", "APPROVAL_REQUIRED", "RESOURCE_REQUIRED"}
ARTIFACTS = {
    "request.json", "status.json", "plan.json", "bindings.json", "pipeline-spec.json",
    "static-validation.json", "runtime-report.json", "verification.json", "metrics.json",
    "traces.json", "result.json", "jobs.json", "catalog.json", "retrieval.json",
    "approval-request.json", "approval.json", "integrity.json", "input.jsonl", "output.jsonl",
    "pipeline.py",
}


# Config directory holding the dataset registry and its row files. Module
# level so a test can redirect it instead of writing into the installed config.
_CONFIG_DIR = Path(__file__).parents[1] / "config"


def _dataset_registry_path() -> Path:
    return _CONFIG_DIR / "datasets.json"


def _dataset_dir() -> Path:
    return _CONFIG_DIR / "datasets"


# The secret registry is likewise read-modify-written, now from two places.
_SECRET_LOCK = threading.RLock()


# The registry is a single JSON file read and rewritten by every dataset call.
# FastAPI runs the synchronous handlers on a thread pool, so two concurrent
# registrations could interleave a read-modify-write and drop one entry.
_DATASET_LOCK = threading.RLock()


def _read_datasets() -> dict[str, Any]:
    with _DATASET_LOCK:
        return _read_json(_dataset_registry_path(), {}) or {}


def _update_datasets(mutate) -> dict[str, Any]:
    """Apply one atomic change to the dataset registry and return it."""
    with _DATASET_LOCK:
        registry = _read_json(_dataset_registry_path(), {}) or {}
        mutate(registry)
        write_json(_dataset_registry_path(), registry)
        return registry


def _write_secret_registry(path: Path, secrets: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(secrets, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _model_endpoint(url: str) -> list[str]:
    url = url.rstrip("/")
    if url.endswith("/chat/completions"):
        url = url[:-len("/chat/completions")]
    if url.endswith("/models") or url.endswith("/model"):
        return [url]
    return [url + "/models", url + "/model"]


def _fetch_models(url: str, api_key: str) -> list[dict[str, Any]]:
    last_error = None
    for endpoint in _model_endpoint(url):
        request = urllib.request.Request(endpoint, headers={"Accept": "application/json"})
        if api_key:
            request.add_header("Authorization", "Bearer " + api_key)
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
            items = payload.get("data", payload.get("models", payload)) if isinstance(payload, dict) else payload
            if isinstance(items, dict):
                items = items.get("data", [])
            result = []
            for item in items or []:
                if isinstance(item, str):
                    result.append({"id": item})
                elif isinstance(item, dict) and (item.get("id") or item.get("name")):
                    result.append({"id": item.get("id") or item.get("name"),
                                   "owned_by": item.get("owned_by"), "created": item.get("created")})
            return sorted(result, key=lambda item: item["id"])
        except (OSError, ValueError, urllib.error.URLError) as exc:
            last_error = exc
    raise ValueError(f"Model discovery failed: {last_error}")


def _read_rows(path: Path, limit: int) -> tuple[list[Any], int]:
    rows, total = [], 0
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                total += 1
                if len(rows) < limit:
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        return [], 0
    return rows, total


def _stage_snapshots(root: Path, limit: int = 30) -> list[dict[str, Any]]:
    """Read DataFlow's materialized per-call JSONL outputs for UI review.

    FileStorage writes one file per `storage.step()`, named after the
    pipeline's own file_name_prefix, so the prefix is not assumed here: any
    `<prefix>_step<N>.jsonl` directly in the cache directory is a stage.
    Fixture runs live in cache subdirectories and are left out.
    """
    cache = root / "cache"
    spec = _read_json(root / "pipeline-spec.json", {}) or {}
    call_names = []
    for step in spec.get("steps", []):
        for target, source in (step.get("prepare_fields") or {}).items():
            call_names.append(f"Copy {source} -> {target}")
        call_names.append(step.get("operator", ""))
    numbered = []
    if cache.is_dir():
        for path in cache.glob("*_step*.jsonl"):
            match = re.search(r"step(\d+)\.jsonl$", path.name)
            if match:
                numbered.append((int(match.group(1)), path))
    result = []
    for position, (number, path) in enumerate(sorted(numbered)):
        rows, total = _read_rows(path, limit)
        name = call_names[position] if position < len(call_names) else path.stem
        result.append({"stage_id": f"stage-{number:02d}", "index": position, "name": name,
                       "operator": name or None, "rows": rows, "row_count": total,
                       "fields": list(rows[0]) if rows and isinstance(rows[0], dict) else [],
                       "source": path.name})
    # The delivered file is the final_keys projection of the last stage, so it
    # is shown alongside the stages rather than instead of them.
    final = next((root / name for name in ("output.jsonl", "candidate.jsonl") if (root / name).exists()), None)
    if final is not None:
        rows, total = _read_rows(final, limit)
        result.append({"stage_id": "final", "index": len(result), "name": "Final output", "operator": None,
                       "rows": rows, "row_count": total,
                       "fields": list(rows[0]) if rows and isinstance(rows[0], dict) else [],
                       "source": final.name})
    return result[:limit]


def _safe_run(config: dict[str, Any], run_id: str, *, allow_missing: bool = False) -> Path:
    root = (Path(config["runs_root"]).resolve() / run_id).resolve()
    if root.parent != Path(config["runs_root"]).resolve() or not root.name.startswith("run-"):
        raise HTTPException(status_code=400, detail="Invalid run id")
    if not root.is_dir() and not allow_missing:
        raise HTTPException(status_code=404, detail="Run not found")
    return root


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _events(root: Path) -> list[dict[str, Any]]:
    # SQLite is the live join authority; events.jsonl is the durable export.
    db = root / "team.sqlite"
    if db.exists():
        try:
            # Readers must not recreate a run while it is being deleted.
            with sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True) as conn:
                rows = conn.execute("SELECT seq,timestamp,kind,role,job,detail FROM events ORDER BY seq").fetchall()
        except sqlite3.OperationalError:
            if not db.exists():
                return []
            raise
        return [{"seq": seq, "timestamp": timestamp, "event": kind, "agent": role,
                 "job": job, "trace_id": root.name, **json.loads(detail)}
                for seq, timestamp, kind, role, job, detail in rows]
    path = root / "events.jsonl"
    try:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except FileNotFoundError:
        return []


def _remove_run_tree(root: Path) -> None:
    def onerror(function, path, exc_info):
        # Python 3.10–3.12 rmtree can fail when a scanned child disappears.
        # Missing children are already removed; permission/IO errors are real.
        if not isinstance(exc_info[1], FileNotFoundError):
            raise exc_info[1]
    shutil.rmtree(root, onerror=onerror)


@contextmanager
def _existing_run_lock(root: Path, deletion_lock: threading.Lock):
    """Take a run's leader lock only while its directory still exists.

    Background work acquires this before creating any file, so a run deleted
    mid-work is not resurrected by the writer that was already in flight.
    """
    lock = None
    with deletion_lock:
        if root.is_dir():
            try:
                lock = (root / ".leader.lock").open("a")
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Another server may have removed the directory before we locked it.
                if not root.is_dir():
                    lock.close()
                    lock = None
            except (FileNotFoundError, BlockingIOError):
                if lock is not None:
                    lock.close()
                    lock = None
    try:
        yield lock is not None
    finally:
        if lock is not None:
            lock.close()


def _run_updated(root: Path) -> float:
    """Sort key for one run, tolerant of missing or malformed metadata.

    A run whose status.json was truncated, or that was deleted while the list
    was being built, must not break the ordering of every other run.
    """
    try:
        status = _read_json(root / "status.json", {})
        value = float(status.get("updated")) if isinstance(status, dict) else float("nan")
        if math.isfinite(value):
            return value
    except (OSError, ValueError, TypeError):
        pass
    try:
        return root.stat().st_mtime
    except OSError:
        return 0.0


def _run_summary(root: Path) -> dict[str, Any]:
    try:
        return _read_run_summary(root)
    except (OSError, sqlite3.DatabaseError, ValueError, TypeError, AttributeError, KeyError) as exc:
        # One damaged run stays visible, and stays identifiable, without
        # hiding the healthy runs beside it.
        try:
            request = _read_json(root / "request.json", {})
        except (OSError, ValueError):
            request = {}
        request = request if isinstance(request, dict) else {}
        reason = f"运行记录读取失败（{type(exc).__name__}），原始文件已保留，其他运行不受影响。"
        return {"run_id": root.name, "state": "BLOCKED", "backend": "unknown",
                "updated": _run_updated(root), "request": request.get("request", "") or "运行记录损坏",
                "input_keys": [], "operators": [], "result": {},
                "summary": reason, "reason": reason, "record_error": True,
                "latest_event": {}, "approval_required": False}


def _read_run_summary(root: Path) -> dict[str, Any]:
    request = _read_json(root / "request.json", {}) or {}
    status = _read_json(root / "status.json", {"state": "QUEUED"}) or {"state": "QUEUED"}
    result = _read_json(root / "result.json", {}) or {}
    spec = _read_json(root / "pipeline-spec.json", {}) or {}
    verification = _read_json(root / "verification.json", {}) or {}
    events = _events(root)
    latest = events[-1] if events else {}
    return {"run_id": root.name, "state": status.get("state", "QUEUED"),
            "backend": status.get("backend") or result.get("backend") or "unknown",
            "updated": _run_updated(root), "request": request.get("request", ""),
            "verification_mode": status.get("verification_mode") or verification.get("mode") or "semantic",
            "input_keys": request.get("input_keys", []), "operators": [s.get("operator") for s in spec.get("steps", [])],
            "result": result, "summary": status.get("summary") or result.get("summary"),
            "reason": status.get("reason") or result.get("reason") or status.get("error") or result.get("error"),
            "latest_event": latest, "approval_required": status.get("state") == "APPROVAL_REQUIRED"}


def _agent_outputs(root: Path) -> list[dict[str, Any]]:
    outputs = []
    paths = sorted((root / "agents").glob("*/*"), key=lambda p: p.stat().st_mtime)
    for directory in paths:
        if not directory.is_dir():
            continue
        path = directory / "last-message.json"
        event_path = directory / "codex-events.jsonl"
        try:
            text = path.read_text(encoding="utf-8")[-3000:] if path.exists() else (
                event_path.read_text(encoding="utf-8")[-3000:] if event_path.exists() else "")
            if text:
                outputs.append({"agent": directory.parent.name, "attempt": directory.name, "text": text})
        except OSError:
            continue
    return outputs


def create_app(config: dict[str, Any] | None = None) -> FastAPI:
    cfg = config or load_config()
    # auto_execute is a deployment choice, not a property of the web layer:
    # when it is on, a new task runs and is field-checked without a second
    # click; when it is off, generation stops at READY. Default off, so a
    # config that says nothing behaves like the manual workflow.
    cfg = dict(cfg)
    cfg.setdefault("auto_execute", False)
    resource_path = _CONFIG_DIR / "resources.json"
    secret_path = _CONFIG_DIR / "resource-secrets.json"
    runtime_path = _CONFIG_DIR / "runtime.json"
    cfg.setdefault("resource_secrets", {})
    if secret_path.exists() and not cfg["resource_secrets"]:
        cfg["resource_secrets"].update(_read_json(secret_path, {}) or {})
    runs_root = Path(cfg["runs_root"]).resolve()
    runs_root.mkdir(parents=True, exist_ok=True)
    conversation_store = ConversationStore(runs_root)
    deletion_lock = threading.Lock()
    workers = max(1, int(cfg.get("api_workers", 4)))
    executor = __import__("concurrent.futures", fromlist=["ThreadPoolExecutor"]).ThreadPoolExecutor(max_workers=workers)
    app = FastAPI(title="DataFlow Multi-Codex", version="1.0.0")
    app.state.config = cfg
    app.state.executor = executor
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    def report_failure(root: Path, run_id: str) -> None:
        """Explain a failed run in the conversation that started it.

        The deterministic triage is posted immediately so the user is never
        left with a raw traceback; the model analysis is a second, slower
        message and is allowed to be missing.
        """
        try:
            with _existing_run_lock(root, deletion_lock) as acquired:
                # The run may have been deleted while this was queued; do not
                # write a diagnosis into a directory the user removed.
                if not acquired:
                    return
                conversation = conversation_store.find_by_run(run_id)
                if not conversation:
                    return
                evidence = collect_evidence(root, cfg)
                verdict = triage(evidence)
                marker = _read_json(root / "failure-analysis.json", {}) or {}
                fingerprint = failure_digest(evidence)
                if marker.get("digest") == fingerprint:
                    return  # Same failure already explained.
                write_json(root / "failure-analysis.json",
                           {"digest": fingerprint, "triage": verdict, "evidence": evidence})
                revision = int(conversation.get("active_revision", 0))
                conversation_store.append(conversation["conversation_id"], conversation_message(
                    "controller", triage_message(verdict), "failure_triage", run_id, revision))
            executor.submit(analyse_failure, root, run_id, conversation["conversation_id"],
                            evidence, verdict, revision)
        except Exception:  # Diagnosis must never mask or replace the original failure.
            pass

    def analyse_failure(root: Path, run_id: str, conversation_id: str,
                        evidence: dict[str, Any], verdict: dict[str, Any], revision: int) -> None:
        import jsonschema

        with _existing_run_lock(root, deletion_lock) as acquired:
            if not acquired:
                return
            store = TeamStore(root)
            directory = root / "agents" / "failure-analyst" / str(int(time.time()))
            try:
                directory.mkdir(parents=True, exist_ok=True)
                store.event("agent.started", "failure_analyst", "failure-analyst")
                payload = {"evidence": evidence, "triage": verdict,
                           "request": evidence.get("request", ""), "trace_id": root.name}
                write_json(directory / "input.json", payload)
                analysis = build_backend(cfg).ask("failure_analyst", payload,
                                                  schema=SCHEMAS["failure_analyst"], directory=directory)
                jsonschema.validate(analysis, SCHEMAS["failure_analyst"])
                write_json(directory / "output.json", analysis)
                marker = _read_json(root / "failure-analysis.json", {}) or {}
                marker["analysis"] = analysis
                write_json(root / "failure-analysis.json", marker)
                store.event("agent.completed", "failure_analyst", "failure-analyst",
                            cause_category=analysis.get("cause_category"))
                conversation_store.append(conversation_id, conversation_message(
                    "controller", analysis_message(analysis, verdict), "failure_analysis",
                    run_id, revision))
            except Exception as exc:
                # The triage message already reached the user; say the deeper
                # analysis is unavailable rather than failing silently.
                store.event("agent.failed", "failure_analyst", "failure-analyst", error=str(exc)[-600:])
                conversation_store.append(conversation_id, conversation_message(
                    "controller", "（模型分析不可用，上面的判定来自确定性检查。）", "failure_analysis",
                    run_id, revision))

    def launch(run_dir: Path, request_text: str, input_keys: list[str] | None, input_file: Path,
               allow_custom: bool, source: dict[str, Any], constraints: dict[str, Any]) -> None:
        def work() -> None:
            try:
                Orchestrator(config=cfg).run(request_text, input_keys=input_keys, input_file=input_file,
                                              run_dir=run_dir, allow_custom=allow_custom,
                                              source=source, constraints=constraints)
            except Exception as exc:  # Orchestrator persists BLOCKED state itself.
                write_json(run_dir / "result.json", {"state": "BLOCKED", "run_dir": str(run_dir), "error": str(exc)})
            finally:
                state = (_read_json(run_dir / "status.json", {}) or {}).get("state")
                if state in FAILURE_STATES:
                    report_failure(run_dir, run_dir.name)
        executor.submit(work)

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        """Health, plus which UI build this server is serving.

        A stale cached bundle looks identical to a missing feature, so the
        build is identified rather than inferred.
        """
        frontend = Path(__file__).parents[1] / "frontend" / "dist"
        assets = sorted((frontend / "assets").glob("index-*.js")) if frontend.is_dir() else []
        index = frontend / "index.html"
        return {"ok": True, "backend": cfg.get("backend", "codex"), "dataflow_root": cfg["dataflow_root"],
                "verification_mode": cfg.get("verification_mode", "semantic"),
                "ui": {"js": assets[0].name if assets else None,
                       "built_at": int(index.stat().st_mtime) if index.exists() else None}}

    def routing_settings() -> dict[str, Any]:
        """Current intent-routing state, with the key never echoed back."""
        key = routing.jev_key(cfg)
        env_forced = os.getenv(routing.JEV_ENABLE_ENV) is not None
        return {"routing": {
            "enabled": routing.jev_enabled(cfg),
            "enabled_by": "env" if env_forced else "config",
            "has_key": bool(key),
            "key_hint": f"…{key[-6:]}" if key else "",
            "key_source": ("env" if os.getenv(routing.JEV_KEY_ENV)
                           else "registry" if (cfg.get("resource_secrets") or {}).get(routing.JEV_KEY_NAME)
                           else "none"),
            "model": routing.JEV_MODEL,
            "endpoint": routing.JEV_ENDPOINT,
            # The UI has to know what happens when the model is unreachable.
            "effective": "jev" if (routing.jev_enabled(cfg) and key) else "rules",
        }}

    @app.get("/api/v1/settings")
    def read_settings() -> dict[str, Any]:
        return routing_settings()

    @app.post("/api/v1/settings")
    def write_settings(payload: dict[str, Any]):
        """Turn the decision model on or off, and store its credential.

        The key goes to the same 0600 registry the pipeline credentials use and
        is never returned; the toggle goes to the runtime config so it survives
        a restart. A missing key is not an error: routing degrades to the rules.
        """
        if "enabled" in payload:
            enabled = bool(payload["enabled"])
            cfg["use_jev_routing"] = enabled
            runtime = _read_json(runtime_path, {}) or {}
            runtime["use_jev_routing"] = enabled
            write_json(runtime_path, runtime)
        if "api_key" in payload:
            api_key = str(payload.get("api_key") or "").strip()
            # Read-modify-write the registry from disk rather than trusting
            # cfg: the running process loaded it at startup, but another
            # writer (or an earlier run of the same server) may have changed
            # it since. Writing back cfg wholesale is how an unrelated
            # credential gets dropped.
            with _SECRET_LOCK:
                # Read-modify-write from disk: cfg was loaded at startup, and
                # writing it back wholesale is how an unrelated credential
                # (a pipeline serving key, say) gets dropped.
                registry = _read_json(secret_path, {}) or {}
                registry.update(cfg.get("resource_secrets") or {})
                if api_key:
                    if len(api_key) < 16:
                        raise HTTPException(status_code=422, detail="API key looks too short")
                    registry[routing.JEV_KEY_NAME] = api_key
                else:
                    # An empty string clears the stored credential.
                    registry.pop(routing.JEV_KEY_NAME, None)
                _write_secret_registry(secret_path, registry)
                cfg["resource_secrets"] = dict(registry)
        return routing_settings()

    @app.post("/api/v1/settings/test")
    def test_settings(payload: dict[str, Any] | None = None):
        """Ask the model one question so the key can be verified from the UI."""
        payload = payload or {}
        key = str(payload.get("api_key") or "").strip() or routing.jev_key(cfg)
        if not key:
            raise HTTPException(status_code=422, detail="No API key configured")
        sample = str(payload.get("sample") or "现在进度怎么样了？")
        started = time.monotonic()
        try:
            intent, confidence = routing.ask_jev(sample, key)
        except routing.JevUnavailable as exc:
            return {"ok": False, "error": str(exc)[:300],
                    "latency_ms": round((time.monotonic() - started) * 1000)}
        return {"ok": True, "intent": intent, "confidence": round(confidence, 3),
                "rules_intent": routing.classify_by_rules(sample),
                "latency_ms": round((time.monotonic() - started) * 1000)}

    @app.get("/api/v1/resources")
    @app.get("/api/v1/servings")
    def resources() -> dict[str, Any]:
        return {"resources": [{"name": name, "type": value.get("type"),
                                "api_url": value.get("args", {}).get("api_url"),
                                "model_name": value.get("args", {}).get("model_name"),
                                "key_name_of_api_key": value.get("args", {}).get("key_name_of_api_key"),
                                "configured": bool(os.getenv(value.get("args", {}).get("key_name_of_api_key", "")) or cfg.get("resource_secrets", {}).get(name))}
                              for name, value in cfg.get("resources", {}).items()]}

    @app.get("/api/v1/resources/classes")
    @app.get("/api/v1/servings/classes")
    def serving_classes() -> dict[str, Any]:
        """Expose constructor metadata in the same shape needed by a serving form."""
        dataflow_root = str(Path(cfg["dataflow_root"]).resolve())
        source_path = Path(dataflow_root) / "dataflow/serving/api_llm_serving_request.py"
        try:
            tree = ast.parse(source_path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError) as exc:
            raise HTTPException(status_code=500, detail=f"Cannot inspect serving class: {exc}") from exc
        init = next((node for node in ast.walk(tree)
                     if isinstance(node, ast.FunctionDef) and node.name == "__init__"), None)
        params = []
        if init is None:
            raise HTTPException(status_code=500, detail="APILLMServing_request.__init__ not found")
        arguments = list(init.args.posonlyargs) + list(init.args.args) + list(init.args.kwonlyargs)
        defaults = [None] * (len(arguments) - len(init.args.defaults)) + list(init.args.defaults)
        for parameter, default_node in zip(arguments, defaults):
            if parameter.arg == "self":
                continue
            try:
                default = ast.literal_eval(default_node) if default_node is not None else None
            except (ValueError, TypeError):
                default = ast.unparse(default_node) if default_node is not None else None
            params.append({"name": parameter.arg, "required": default_node is None,
                           "default_value": default,
                           "type": ast.unparse(parameter.annotation) if parameter.annotation else "Any"})
        return {"classes": [{"cls_name": "APILLMServing_request", "params": params}]}

    @app.post("/api/v1/resources")
    @app.post("/api/v1/servings")
    def register_resource(payload: dict[str, Any]):
        name = str(payload.get("name", "")).strip()
        api_url = str(payload.get("api_url", "")).strip()
        model_name = str(payload.get("model_name", "")).strip()
        key_name = str(payload.get("key_name_of_api_key", "")).strip()
        if not name or not name.replace("_", "a").isalnum() or not api_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=422, detail="name must be alphanumeric and api_url must use HTTP or HTTPS")
        api_key = str(payload.get("api_key", "")).strip()
        if api_key and not key_name:
            key_name = "DF_PIPELINE_" + re.sub(r"[^A-Z0-9]", "_", name.upper()) + "_API_KEY"
        if not key_name.startswith("DF_PIPELINE_"):
            raise HTTPException(status_code=422, detail="key_name_of_api_key must start with DF_PIPELINE_")
        cfg.setdefault("resources", {})[name] = {"type": "api_llm", "args": {
            "api_url": normalize_chat_url(api_url), "key_name_of_api_key": key_name, "model_name": model_name,
            "temperature": float(payload.get("temperature", 0.2)), "max_workers": max(1, int(payload.get("max_workers", 2))),
            "max_tokens": max(1, int(payload.get("max_tokens", 1024))),
            "max_retries": int(payload.get("max_retries", 2)), "connect_timeout": 10, "read_timeout": 120}}
        if api_key:
            cfg.setdefault("resource_secrets", {})[name] = api_key
        write_json(resource_path, cfg["resources"])
        if api_key or name in (cfg.get("resource_secrets") or {}):
            with _SECRET_LOCK:
                # Merge into what is on disk rather than replacing it with the
                # snapshot cfg was started with; registering one serving must
                # never drop another service's credential.
                registry = _read_json(secret_path, {}) or {}
                registry.update(cfg.get("resource_secrets") or {})
                _write_secret_registry(secret_path, registry)
                cfg["resource_secrets"] = dict(registry)
        return {"name": name, "resource": cfg["resources"][name],
                "configured": bool(os.getenv(key_name) or cfg.get("resource_secrets", {}).get(name))}

    @app.delete("/api/v1/resources/{name}")
    @app.delete("/api/v1/servings/{name}")
    def delete_resource(name: str):
        if name not in cfg.get("resources", {}):
            raise HTTPException(status_code=404, detail="Resource not found")
        del cfg["resources"][name]
        cfg.get("resource_secrets", {}).pop(name, None)
        write_json(resource_path, cfg["resources"])
        if cfg.get("resource_secrets"):
            _write_secret_registry(secret_path, cfg["resource_secrets"])
        elif secret_path.exists():
            secret_path.unlink()
        return {"deleted": name}

    @app.post("/api/v1/servings/models")
    @app.post("/api/v1/resources/models")
    @app.post("/api/v1/models")
    def discover_models(payload: dict[str, Any]):
        resource_name = str(payload.get("resource_name", "")).strip()
        configured = cfg.get("resources", {}).get(resource_name, {}) if resource_name else {}
        args = configured.get("args", {})
        api_url = str(payload.get("api_url", "")).strip() or args.get("api_url", "")
        api_key = str(payload.get("api_key", "")).strip() or os.getenv(args.get("key_name_of_api_key", ""), "") or cfg.get("resource_secrets", {}).get(resource_name, "")
        if not api_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=422, detail="api_url must use HTTP or HTTPS")
        try:
            return {"models": _fetch_models(api_url, api_key)}
        except ValueError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/api/v1/servings/models")
    @app.get("/api/v1/resources/models")
    @app.get("/api/v1/models")
    def discover_models_get(api_url: str = "", resource_name: str = ""):
        return discover_models({"api_url": api_url, "resource_name": resource_name})

    @app.get("/api/v1/datasets")
    def list_datasets() -> dict[str, Any]:
        registry = _read_datasets()
        return {"datasets": list(registry.values())}

    @app.post("/api/v1/datasets")
    def register_dataset(payload: dict[str, Any]):
        name = str(payload.get("name", "")).strip()
        rows = payload.get("rows")
        if not name or not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
            raise HTTPException(status_code=422, detail="name and a non-empty rows array of objects are required")
        if len(rows) > cfg.get("max_input_rows", 10000):
            raise HTTPException(status_code=422, detail="dataset exceeds the configured row limit")
        dataset_id = "ds-" + uuid.uuid4().hex[:12]
        dataset_dir = _dataset_dir()
        dataset_dir.mkdir(parents=True, exist_ok=True)
        path = dataset_dir / f"{dataset_id}.jsonl"
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        os.chmod(path, 0o600)
        item = {"id": dataset_id, "name": name, "path": str(path), "rows": len(rows),
                "input_keys": list(rows[0]), "sample": rows[:5]}
        _update_datasets(lambda registry: registry.__setitem__(dataset_id, item))
        return item

    @app.get("/api/v1/datasets/{dataset_id}/preview")
    def dataset_preview(dataset_id: str, limit: int = 5):
        item = _read_datasets().get(dataset_id)
        if not item:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return {"dataset": item, "rows": item.get("sample", [])[:max(1, min(limit, 100))]}

    @app.delete("/api/v1/datasets/{dataset_id}")
    def delete_dataset(dataset_id: str):
        removed: list[dict[str, Any]] = []
        registry = _update_datasets(lambda items: removed.append(items.pop(dataset_id, None)))
        item = removed[0]
        if not item:
            raise HTTPException(status_code=404, detail="Dataset not found")
        path = Path(item.get("path", ""))
        if path.is_file() and path.parent == _dataset_dir():
            path.unlink()
        return {"deleted": dataset_id}

    @app.get("/api/v1/agents")
    def agents() -> list[dict[str, Any]]:
        return [asdict(identity) for identity in IDENTITIES]

    @app.get("/api/v1/runs")
    def list_runs() -> list[dict[str, Any]]:
        # _run_updated tolerates missing or malformed timestamps, and a run
        # deleted between the glob and the read is skipped rather than raising.
        roots = sorted((p for p in runs_root.glob("run-*") if p.is_dir()),
                       key=lambda p: (_run_updated(p), p.name), reverse=True)
        return [_run_summary(root) for root in roots[:100] if root.is_dir()]

    @app.post("/api/v1/conversations")
    def create_conversation(payload: dict[str, Any] | None = None):
        return conversation_store.create(str((payload or {}).get("title", "")).strip())

    @app.get("/api/v1/conversations")
    def list_conversations():
        return {"conversations": conversation_store.list()}

    @app.get("/api/v1/conversations/{conversation_id}")
    def get_conversation(conversation_id: str):
        item = conversation_store.get(conversation_id)
        if not item:
            raise HTTPException(status_code=404, detail="Conversation not found")
        return item

    @app.post("/api/v1/conversations/{conversation_id}/messages")
    async def post_conversation_message(conversation_id: str, payload: dict[str, Any]):
        item = conversation_store.get(conversation_id)
        if not item:
            raise HTTPException(status_code=404, detail="Conversation not found")
        text = str(payload.get("content", payload.get("message", ""))).strip()
        if not text:
            raise HTTPException(status_code=422, detail="content is required")
        # The decision model answers in a few hundred ms; the rules are the
        # fallback when it is unreachable. Both paths record how they decided.
        intent, routing_meta = classify_with_source(text, bool(item.get("active_run_id")), config=cfg)
        user_msg = conversation_message("user", text, intent, item.get("active_run_id"), item.get("active_revision", 0))
        user_msg["routing"] = routing_meta
        item = conversation_store.append(conversation_id, user_msg)
        run_id = item.get("active_run_id")
        response = ""
        if intent == "new_task":
            run_payload = dict(payload)
            run_payload["request"] = text
            created = await create_run(run_payload)
            data = json.loads(created.body.decode("utf-8"))
            run_id = data["run_id"]
            item["active_run_id"] = run_id
            item["active_revision"] = int(item.get("active_revision", 0)) + 1
            item["status"] = "running"
            response = f"已创建 Run {run_id}，正在调度 Planner、算子专家、Integrator 和 Verifier。"
        elif intent == "status_query" and run_id:
            summary = _run_summary(_safe_run(cfg, run_id))
            response = f"当前 Run {run_id} 状态：{summary['state']}。" + (f" {summary['summary']}" if summary.get("summary") else "")
        elif intent == "artifact_query" and run_id:
            response = f"已定位到 Run {run_id} 的 Pipeline、Operator、事件和验证证据，可在工作台查看。"
        elif intent == "revision" and run_id:
            base = _safe_run(cfg, run_id)
            original = _read_json(base / "request.json", {}) or {}
            input_path = base / "input.jsonl"
            rows = [json.loads(line) for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()] if input_path.exists() else None
            run_payload = {"request": text, "input_rows": rows, "input_keys": original.get("input_keys"), "allow_custom": True}
            created = await create_run(run_payload)
            data = json.loads(created.body.decode("utf-8"))
            new_run = data["run_id"]
            write_json(runs_root / new_run / "revision.json", {"parent_run_id": run_id, "revision_number": int(item.get("active_revision", 0)) + 1, "change_request": text})
            item.setdefault("revisions", []).append({"revision": int(item.get("active_revision", 0)) + 1, "run_id": new_run,
                                                       "parent_run_id": run_id, "change_request": text, "created_at": time.time()})
            item["active_run_id"] = new_run
            item["active_revision"] = int(item.get("active_revision", 0)) + 1
            item["status"] = "running"
            run_id = new_run
            response = f"已创建 revision {item['active_revision']}（Run {new_run}），旧 Run 保留不变。"
        else:
            response = ("这个对话还没有运行记录。直接把要处理的数据和要求发给我，"
                        "我会拆解步骤、选择算子并生成 pipeline。"
                        if intent in {"status_query", "artifact_query", "revision"} else
                        "我需要一个具体的数据处理需求，或请先创建任务。")
        controller_msg = conversation_message("controller", response, intent, run_id, item.get("active_revision", 0))
        item["status"] = "running" if run_id else "awaiting_user"
        item.setdefault("messages", []).append(controller_msg)
        conversation_store.save(item)
        return {"conversation": item, "message": controller_msg, "intent": intent, "run_id": run_id}

    @app.get("/api/v1/conversations/{conversation_id}/stream")
    async def conversation_stream(conversation_id: str, after: int = 0):
        if not conversation_store.get(conversation_id):
            raise HTTPException(status_code=404, detail="Conversation not found")
        async def generator():
            cursor = after
            idle = 0
            while idle < 300:
                item = conversation_store.get(conversation_id) or {}
                messages = item.get("messages", [])
                for index, msg in enumerate(messages[cursor:], start=cursor + 1):
                    yield f"id: {index}\ndata: {json.dumps(msg, ensure_ascii=False)}\n\n"
                    cursor = index
                run_id = item.get("active_run_id")
                if run_id:
                    root = runs_root / run_id
                    for event in _events(root) if root.is_dir() else []:
                        key = int(event.get("seq", 0))
                        if key > cursor:
                            yield f"event: run\nid: {key}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                            cursor = key
                idle += 1
                await asyncio.sleep(0.5)
        return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.delete("/api/v1/runs/{run_id}")
    def delete_run(run_id: str):
        with deletion_lock:
            root = _safe_run(cfg, run_id, allow_missing=True)
            try:
                if root.exists():
                    state = (_read_json(root / "status.json", {}) or {}).get("state")
                    if state and state not in TERMINAL_STATES:
                        raise HTTPException(status_code=409, detail="Running or queued runs cannot be deleted")
                    if not state and not (root / ".leader.lock").exists():
                        raise HTTPException(status_code=409, detail="Run is initializing; try again after it finishes")
                    with (root / ".leader.lock").open("a") as lock:
                        try:
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            raise HTTPException(status_code=409, detail="Run is still busy; try again after it finishes")
                        _remove_run_tree(root)
                (runs_root / ".api-inputs" / f"{run_id}.jsonl").unlink(missing_ok=True)
            except OSError as exc:
                raise HTTPException(status_code=409, detail="Could not finish removing run files; check filesystem permissions and retry") from exc
        return {"deleted": run_id}

    @app.post("/api/v1/runs")
    async def create_run(payload: dict[str, Any]) -> JSONResponse:
        request_text = str(payload.get("request", "")).strip()
        if not request_text:
            raise HTTPException(status_code=422, detail="request is required")
        dataset_id = str(payload.get("dataset_id", "")).strip()
        dataset_item = _read_datasets().get(dataset_id) if dataset_id else None
        if dataset_id and not dataset_item:
            raise HTTPException(status_code=404, detail="Dataset not found")
        rows = None if dataset_item else payload.get("input_rows")
        input_keys = payload.get("input_keys")
        if dataset_item:
            rows = [json.loads(line) for line in Path(dataset_item["path"]).read_text(encoding="utf-8").splitlines() if line.strip()]
            input_keys = input_keys or dataset_item.get("input_keys") or list(rows[0])
        if rows is not None:
            if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
                raise HTTPException(status_code=422, detail="input_rows must be a non-empty list of objects")
            if input_keys is None:
                input_keys = list(rows[0])
            if any(set(input_keys) - set(row) for row in rows):
                raise HTTPException(status_code=422, detail="input rows do not contain input_keys")
        else:
            default = Path(__file__).parents[1] / "examples/input.jsonl"
            rows = [json.loads(line) for line in default.read_text().splitlines() if line.strip()]
            input_keys = input_keys or list(rows[0])
        run_id = "run-" + uuid.uuid4().hex[:12]
        input_dir = runs_root / ".api-inputs"
        input_dir.mkdir(mode=0o700, exist_ok=True)
        input_file = input_dir / f"{run_id}.jsonl"
        input_file.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        run_dir = runs_root / run_id
        run_dir.mkdir(mode=0o700, exist_ok=False)
        launch(run_dir, request_text, input_keys, input_file, bool(payload.get("allow_custom", True)),
               payload.get("source") or {"type": "web"}, payload.get("constraints") or {})
        return JSONResponse({"run_id": run_id, "state": "QUEUED", "request": request_text}, status_code=202)

    @app.get("/api/v1/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        root = _safe_run(cfg, run_id)
        summary = _run_summary(root)
        summary["status"] = _read_json(root / "status.json", {})
        summary["metrics"] = _read_json(root / "metrics.json", {})
        summary["verification"] = _read_json(root / "verification.json")
        summary["pipeline"] = _read_json(root / "pipeline-spec.json")
        return summary

    @app.get("/api/v1/runs/{run_id}/events")
    def get_events(run_id: str, after: int = 0) -> list[dict[str, Any]]:
        return [event for event in _events(_safe_run(cfg, run_id)) if event.get("seq", 0) > after]

    @app.get("/api/v1/runs/{run_id}/stages")
    def stages(run_id: str, limit: int = 30):
        return {"stages": _stage_snapshots(_safe_run(cfg, run_id), max(1, min(limit, 100)))}

    @app.get("/api/v1/runs/{run_id}/agent-outputs")
    def agent_outputs(run_id: str) -> list[dict[str, Any]]:
        return _agent_outputs(_safe_run(cfg, run_id))

    @app.get("/api/v1/runs/{run_id}/collaboration")
    def collaboration(run_id: str):
        root = _safe_run(cfg, run_id)
        events = _events(root)
        latest = {}
        agents = {}
        for event in events:
            agent = event.get("agent") or "system"
            entry = agents.setdefault(agent, {"agent": agent, "state": "idle", "events": 0, "jobs": [], "skills": []})
            entry["events"] += 1
            entry["last_event"] = event.get("event")
            entry["updated"] = event.get("timestamp")
            if event.get("job") and event["job"] not in entry["jobs"]:
                entry["jobs"].append(event["job"])
            if event.get("skill") and event["skill"] not in entry["skills"]:
                entry["skills"].append(event["skill"])
            if event.get("event") in {"agent.started", "workflow.state"}:
                entry["state"] = "running"
            elif event.get("event") == "agent.completed":
                entry["state"] = "completed"
            elif event.get("event") == "agent.failed":
                entry["state"] = "failed"
            latest[agent] = event
        identity_map = {identity.agent_id: identity for identity in IDENTITIES}
        for agent, entry in agents.items():
            identity = identity_map.get(agent)
            if identity:
                entry.update({"name": identity.name, "purpose": identity.purpose, "tools": list(identity.tools), "forbidden": list(identity.forbidden)})
        return {"run_id": run_id, "state": _run_summary(root)["state"], "agents": list(agents.values()), "events": events,
                "outputs": _agent_outputs(root), "latest": latest}

    @app.get("/api/v1/runs/{run_id}/skills")
    def run_skills(run_id: str):
        root = _safe_run(cfg, run_id)
        registry = SkillRegistry()
        skill_root = Path(__file__).parents[1] / ".agents" / "skills"
        calls = [event for event in _events(root) if event.get("event") in {"skill.invoked", "tool.called"}]
        result = []
        for name in registry.names():
            skill = registry.get(name)
            path = skill_root / name.replace("_", "-") / "SKILL.md"
            raw = path.read_bytes() if path.exists() else b""
            result.append({"name": name, "purpose": skill.purpose, "input_schema": skill.input_schema,
                           "output_schema": skill.output_schema, "dependencies": list(skill.dependencies),
                           "security_boundary": skill.security_boundary, "hash": hashlib.sha256(raw).hexdigest() if raw else None,
                           "calls": [call for call in calls if str(call.get("skill", "")).replace("-", "_") == name or str(call.get("job", "")).replace("-", "_").startswith(name)]})
        return {"run_id": run_id, "skills": result}

    @app.get("/api/v1/runs/{run_id}/evidence")
    def run_evidence(run_id: str):
        root = _safe_run(cfg, run_id)
        names = ("status.json", "static-validation.json", "runtime-report.json", "verification.json", "metrics.json", "integrity.json", "traces.json")
        return {"run_id": run_id, "artifacts": [{"name": name, "available": (root / name).exists(), "value": _read_json(root / name)} for name in names]}

    @app.get("/api/v1/runs/{run_id}/revisions")
    def run_revisions(run_id: str):
        _safe_run(cfg, run_id)
        items = []
        for candidate in sorted(runs_root.glob("run-*")):
            metadata = _read_json(candidate / "revision.json") or {}
            if candidate.name == run_id or metadata.get("parent_run_id") == run_id:
                items.append({"run_id": candidate.name, **metadata, **_run_summary(candidate)})
        return {"run_id": run_id, "revisions": items}

    @app.get("/api/v1/runs/{run_id}/stream")
    async def stream_events(run_id: str, after: int = 0) -> StreamingResponse:
        root = _safe_run(cfg, run_id)
        async def generator():
            cursor = after
            idle = 0
            while idle < 300:
                if not root.is_dir():
                    yield 'event: done\ndata: {"state":"DELETED"}\n\n'
                    return
                events = [event for event in _events(root) if event.get("seq", 0) > cursor]
                for event in events:
                    cursor = max(cursor, event.get("seq", cursor))
                    yield f"id: {cursor}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                state = (_read_json(root / "status.json", {}) or {}).get("state")
                if state in TERMINAL_STATES and not events:
                    yield f"event: done\ndata: {json.dumps({'state': state})}\n\n"
                    return
                idle += 1
                await asyncio.sleep(0.5)
        return StreamingResponse(generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.get("/api/v1/runs/{run_id}/artifact/{name}")
    def artifact(run_id: str, name: str):
        root = _safe_run(cfg, run_id)
        if name not in ARTIFACTS or Path(name).name != name:
            raise HTTPException(status_code=400, detail="Artifact is not available")
        path = root / name
        if not path.exists():
            raise HTTPException(status_code=404, detail="Artifact not found")
        if path.suffix == ".jsonl" or path.suffix == ".py":
            return FileResponse(path)
        return JSONResponse(_read_json(path, {}))

    @app.get("/api/v1/runs/{run_id}/pipeline-code")
    def pipeline_code(run_id: str):
        """Return the complete generated pipeline source for in-browser review."""
        root = _safe_run(cfg, run_id)
        path = root / "pipeline.py"
        if not path.exists():
            raise HTTPException(status_code=404, detail="Pipeline code is not generated yet")
        try:
            operators = []
            spec = _read_json(root / "pipeline-spec.json", {}) or {}
            for step in spec.get("steps", []):
                source_file = step.get("source_file", "")
                custom = bool(step.get("proposal"))
                base = root if custom else Path(cfg["dataflow_root"]).resolve()
                item = {"id": step["step_id"], "name": step["operator"],
                        "filename": source_file, "kind": "custom" if custom else "dataflow",
                        "code": "", "error": None, "changed": False}
                candidate = (base / source_file).resolve()
                # Never serve arbitrary files through a modified spec or symlink.
                allowed = (base / ("custom" if custom else "dataflow/operators")).resolve()
                if (not source_file or Path(source_file).is_absolute()
                        or not candidate.is_relative_to(allowed)
                        or not allowed.is_relative_to(base) or candidate.suffix != ".py"):
                    item["error"] = "Operator source path is outside its source directory"
                else:
                    try:
                        raw = candidate.read_bytes()
                        item["code"] = raw.decode("utf-8")
                        item["sha256"] = hashlib.sha256(raw).hexdigest()
                        expected = step.get("source_sha256")
                        item["changed"] = bool(expected and expected != item["sha256"])
                    except (OSError, UnicodeError):
                        item["error"] = "Operator source file is missing or unreadable"
                operators.append(item)
            runner = root / RUNNER_FILENAME
            return {"run_id": run_id, "filename": path.name, "code": path.read_text(encoding="utf-8"),
                    "operators": operators, "spec": _read_json(root / "pipeline-spec.json", {}) or {},
                    "runner": {"filename": RUNNER_FILENAME,
                               "code": runner.read_text(encoding="utf-8") if runner.exists() else ""}}
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Cannot read pipeline code: {exc}") from exc

    @app.post("/api/v1/runs/{run_id}/approve")
    def approve_run(run_id: str, payload: dict[str, Any] | None = None):
        root = _safe_run(cfg, run_id)
        try:
            grant = approve(root, int((payload or {}).get("ttl_seconds", 3600)))
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        TeamStore(root).event("approval.granted", "human", **grant)
        return {"run_id": run_id, "approval": grant}

    @app.post("/api/v1/runs/{run_id}/resume")
    def resume_run(run_id: str):
        root = _safe_run(cfg, run_id)
        state = (_read_json(root / "status.json", {}) or {}).get("state")
        if state not in {"APPROVAL_REQUIRED", "RESOURCE_REQUIRED"}:
            raise HTTPException(status_code=409, detail=f"Run is {state or 'unknown'}, not awaiting approval or resource configuration")
        executor.submit(lambda: Orchestrator(config=cfg).resume(root))
        return {"run_id": run_id, "state": "RESUMING"}

    @app.post("/api/v1/runs/{run_id}/rerun")
    def rerun_with_input(run_id: str, payload: dict[str, Any] | None = None):
        """Execute an existing pipeline against different data.

        A run is an immutable evidence bundle: its input.jsonl is the snapshot
        the plan was built from, and Run pipeline deliberately replays it.
        Pointing the same pipeline at new data therefore produces a new run,
        but reuses the validated spec instead of paying for the agents again.
        """
        source = _safe_run(cfg, run_id)
        spec = _read_json(source / "pipeline-spec.json")
        if not spec:
            raise HTTPException(status_code=409, detail="This run has no generated pipeline yet")
        payload = payload or {}
        dataset_id = str(payload.get("dataset_id", "")).strip()
        dataset_item = _read_datasets().get(dataset_id) if dataset_id else None
        if dataset_id and not dataset_item:
            raise HTTPException(status_code=404, detail="Dataset not found")
        if dataset_item:
            rows = [json.loads(line) for line in
                    Path(dataset_item["path"]).read_text(encoding="utf-8").splitlines() if line.strip()]
        else:
            rows = payload.get("input_rows")
        if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
            raise HTTPException(status_code=422, detail="input_rows must be a non-empty list of objects")
        # The pipeline was compiled for specific columns; refuse data it cannot read
        # rather than failing later inside an operator.
        required = set(spec.get("initial_keys") or [])
        missing = sorted(required - set.intersection(*(set(row) for row in rows)))
        if missing:
            raise HTTPException(status_code=422,
                                detail=f"这条 pipeline 需要字段 {', '.join(sorted(required))}，新数据缺少：{', '.join(missing)}")

        request_text = (_read_json(source / "request.json", {}) or {}).get("request", "")
        new_id = "run-" + uuid.uuid4().hex[:12]
        root = runs_root / new_id
        root.mkdir(mode=0o700, exist_ok=False)
        (root / "input.jsonl").write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        write_json(root / "request.json", {"request": request_text, "input_keys": sorted(required),
                                           "allow_custom": False, "source": {"type": "rerun", "parent_run_id": run_id}})
        write_json(root / "pipeline-spec.json", spec)
        for name in ("plan.json", "bindings.json", "static-validation.json", "catalog.json"):
            if (source / name).exists():
                shutil.copyfile(source / name, root / name)
        write_pipeline_sources(root, spec, request_text)
        write_json(root / "revision.json", {"parent_run_id": run_id, "reused_pipeline": True,
                                            "rows": len(rows), "dataset_id": dataset_id or None})
        store = TeamStore(root)
        store.checkpoint("READY", summary=f"复用 {run_id} 的 pipeline，待对 {len(rows)} 行新数据执行")
        store.event("pipeline.reused", "human", parent_run_id=run_id, rows=len(rows))
        conversation = conversation_store.find_by_run(run_id)
        if conversation:
            conversation_store.append(conversation["conversation_id"], conversation_message(
                "controller",
                f"已复用 Run {run_id} 的 pipeline 创建 {new_id}，对 {len(rows)} 行新数据执行，未重新调用 Agent。",
                "pipeline_reuse", new_id, int(conversation.get("active_revision", 0))))
            conversation["active_run_id"] = new_id
            conversation_store.save(conversation)
        execute_pipeline(new_id)
        return {"run_id": new_id, "parent_run_id": run_id, "state": "RUNNING", "rows": len(rows)}

    @app.post("/api/v1/runs/{run_id}/execute")
    @app.post("/api/v1/runs/{run_id}/run")
    def execute_pipeline(run_id: str):
        root = _safe_run(cfg, run_id)
        if not (root / "pipeline-spec.json").exists() or not (root / "pipeline.py").exists():
            raise HTTPException(status_code=409, detail="This run has no generated pipeline yet")
        state = (_read_json(root / "status.json", {}) or {}).get("state")
        if state not in TERMINAL_STATES:
            raise HTTPException(status_code=409, detail="This run is already busy")
        lock = (root / ".leader.lock").open("a")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise HTTPException(status_code=409, detail="This run is already busy")
        def work() -> None:
            store = TeamStore(root)
            try:
                # Bind the latest serving configuration without re-planning.
                spec = normalize_operator_defaults(_read_json(root / "pipeline-spec.json"))
                for name in spec.get("resources", {}):
                    if name in cfg.get("resources", {}):
                        spec["resources"][name] = cfg["resources"][name]
                spec["servings"] = dict(spec.get("resources", {}))
                write_json(root / "pipeline-spec.json", spec)
                write_pipeline_sources(root, spec, _read_json(root / "request.json", {}).get("request"))
                store.event("execution.requested", "human", confirmation="Run pipeline")
                runtime = execute(root, cfg, user_requested=True)
                write_json(root / "runtime-report.json", runtime)
                if runtime["status"] == "resource_required":
                    store.checkpoint("RESOURCE_REQUIRED", resources=runtime["resources"], summary="等待注册或配置 DataFlow API resource")
                elif runtime["status"] == "approval_required":
                    store.checkpoint("APPROVAL_REQUIRED", reasons=runtime["operators"])
                elif runtime["status"] == "passed":
                    candidate = root / "candidate.jsonl"
                    if candidate.exists():
                        shutil.copyfile(candidate, root / "output.jsonl")
                    store.checkpoint("EXECUTED", summary=f"Pipeline 执行完成，输出 {runtime.get('rows', 0)} 行")
                    write_json(root / "result.json", {"state": "EXECUTED", "run_dir": str(root), "rows": runtime.get("rows", 0)})
                else:
                    store.checkpoint("BLOCKED", error=runtime.get("error", "Pipeline execution failed"))
            except Exception as exc:
                store.checkpoint("BLOCKED", error=str(exc)[-2400:])
            finally:
                try:
                    store.export()
                finally:
                    lock.close()
                state = (_read_json(root / "status.json", {}) or {}).get("state")
                if state in FAILURE_STATES:
                    report_failure(root, run_id)
        try:
            TeamStore(root).checkpoint("RUNNING", summary="正在执行已生成的 DataFlow pipeline")
            executor.submit(work)
        except Exception as exc:
            lock.close()
            TeamStore(root).checkpoint("BLOCKED", error=str(exc)[-2400:])
            raise HTTPException(status_code=503, detail="Could not start pipeline execution") from exc
        return {"run_id": run_id, "state": "RUNNING"}

    @app.get("/api/v1/operators")
    def operators(query: str = "", limit: int = 50):
        try:
            catalog = load_catalog()
            if not catalog:
                catalog = discover_operator_catalog(cfg["dataflow_root"])
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        matches = search_catalog(query, catalog, limit=max(1, min(limit, 200))) if query else catalog[:max(1, min(limit, 200))]
        return {"count": len(matches), "operators": matches}

    frontend = Path(__file__).parents[1] / "frontend" / "dist"
    if frontend.is_dir():
        from fastapi.staticfiles import StaticFiles

        # Vite fingerprints every asset filename, so those are safe to cache
        # forever; index.html is not, and a browser that caches it heuristically
        # keeps loading a stale bundle after a rebuild — which is exactly how a
        # shipped feature appears to be missing from the UI.
        class FingerprintedAssets(StaticFiles):
            def file_response(self, *args, **kwargs):
                response = super().file_response(*args, **kwargs)
                response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
                return response

        app.mount("/assets", FingerprintedAssets(directory=frontend / "assets"), name="assets")

        @app.get("/{path:path}")
        def spa(path: str):
            candidate = frontend / path
            served = candidate if candidate.is_file() else frontend / "index.html"
            response = FileResponse(served)
            if served.name == "index.html":
                response.headers["Cache-Control"] = "no-cache, must-revalidate"
            return response

    return app


app = create_app()


def main() -> None:
    import uvicorn
    uvicorn.run("dataflow_agents.web:app", host=os.getenv("DF_WEB_HOST", "127.0.0.1"),
                port=int(os.getenv("DF_WEB_PORT", "8000")), reload=False)
