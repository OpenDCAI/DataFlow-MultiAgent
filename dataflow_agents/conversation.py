"""Small durable conversation store and deterministic controller routing."""
from __future__ import annotations

import json
import re
import threading
import time
import uuid
from pathlib import Path


class ConversationStore:
    def __init__(self, root: Path):
        self.root = Path(root) / "conversations"
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _path(self, cid: str) -> Path:
        if not re.fullmatch(r"conv-[a-f0-9]{12}", cid):
            raise ValueError("invalid conversation id")
        return self.root / f"{cid}.json"

    def create(self, title: str = "") -> dict:
        now = time.time()
        item = {"conversation_id": "conv-" + uuid.uuid4().hex[:12], "title": title or "New conversation",
                "created_at": now, "updated_at": now, "active_run_id": None, "active_revision": 0,
                "status": "new", "messages": [], "revisions": [], "artifacts": [], "preferences": {}}
        self.save(item)
        return item

    def get(self, cid: str) -> dict | None:
        try:
            return json.loads(self._path(cid).read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    def save(self, item: dict) -> dict:
        with self._lock:
            item["updated_at"] = time.time()
            path = self._path(item["conversation_id"])
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(item, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            tmp.replace(path)
        return item

    def list(self) -> list[dict]:
        return sorted((x for p in self.root.glob("conv-*.json") if (x := self.get(p.stem))),
                      key=lambda x: x.get("updated_at", 0), reverse=True)

    def find_by_run(self, run_id: str) -> dict | None:
        """The conversation a run belongs to, current or historical."""
        for item in self.list():
            if item.get("active_run_id") == run_id:
                return item
            if any(message.get("run_id") == run_id for message in item.get("messages", [])):
                return item
            if any(revision.get("run_id") == run_id for revision in item.get("revisions", [])):
                return item
        return None

    def append(self, cid: str, message: dict) -> dict:
        item = self.get(cid)
        if not item:
            raise KeyError(cid)
        item.setdefault("messages", []).append(message)
        return self.save(item)


def classify_message(text: str, has_run: bool = False) -> str:
    value = text.lower().strip()
    if any(x in value for x in ("进度", "到哪", "现在怎么样", "status", "progress")):
        return "status_query"
    if has_run and any(x in value for x in ("修改", "改成", "换成", "不要", "增加", "调整", "不满意", "重新")):
        return "revision"
    if any(x in value for x in ("代码", "证据", "结果", "pipeline", "operator")) and has_run:
        return "artifact_query"
    return "new_task"


def message(role: str, content: str, intent: str, run_id: str | None = None, revision: int = 0) -> dict:
    return {"message_id": "msg-" + uuid.uuid4().hex[:12], "role": role, "content": content,
            "created_at": time.time(), "intent": intent, "run_id": run_id, "revision": revision}
