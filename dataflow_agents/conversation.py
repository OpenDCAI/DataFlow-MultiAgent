"""Small durable conversation store and deterministic controller routing."""
from __future__ import annotations

import json
import re
import threading
import time
import uuid
from pathlib import Path

# Words that ask about progress or a result rather than describing work.
STATUS_WORDS = ("进度", "到哪", "现在怎么样", "怎么样了", "跑完了吗", "状态", "status", "progress")
# A field name in a spec is an ordinary word: a request that lists `status`
# among its columns is still a request, so these decide the tie.
#
# Transformation verbs say "do this to my data". On their own they are enough:
# "dedupe these rows by status" is a request while "what is the status" is not.
TASK_VERBS = re.compile(
    r"(清洗|去重|过滤|筛选|生成|提取|转换|合并|拆分|统计|排序|输出|保留|"
    r"deduplicat|filter|clean|extract|generat|normaliz|merg|sort|transform|aggregat)",
    re.IGNORECASE)
# Structure says "here is a specification". These count as a pair so a single
# stray icon or bracket cannot reclassify a genuine question.
TASK_STRUCTURE = (
    re.compile(r"^\s*\d+[.、)]", re.MULTILINE),           # a numbered step list
    re.compile(r"^\s*[-*>]\s+\S", re.MULTILINE),           # a bullet list
    re.compile(r"\{.*\}|\[.*\]", re.DOTALL),               # sample rows
    re.compile(r"(字段|列|columns?|fields?)", re.IGNORECASE),
    re.compile(r"(保留|仅输出|keep only|retain)", re.IGNORECASE),
)
# Phrases that only make sense as a question about an existing run.
QUESTION_SIGNALS = re.compile(r"(吗|呢|怎样|如何|为什么|什么时候|\?|？|what|when|how|why|is it|are we)",
                              re.IGNORECASE)
# A specification is long; a question about one is not.
SPECIFICATION_LENGTH = 240


def looks_like_task(text: str) -> bool:
    """True when the message describes work, even if it names a status field."""
    structural = sum(1 for pattern in TASK_STRUCTURE if pattern.search(text))
    if len(text) >= SPECIFICATION_LENGTH:
        structural += 1
    if structural < 2:
        # A transformation verb on its own still describes work when the
        # message is not phrased as a question.
        return bool(TASK_VERBS.search(text)) and not QUESTION_SIGNALS.search(text)
    # Status wording inside a short question stays a question.
    return not (QUESTION_SIGNALS.search(text) and len(text) < SPECIFICATION_LENGTH)


def classify_message(text: str, has_run: bool = False) -> str:
    value = text.lower().strip()
    mentions_status = any(word in value for word in STATUS_WORDS)
    if mentions_status and looks_like_task(text):
        # The word appears, but the message describes work. With a run already
        # open a reworded request is a revision, not a fresh task.
        if has_run and any(x in value for x in ("修改", "改成", "换成", "不要", "增加", "调整", "不满意", "重新")):
            return "revision"
        return "new_task"
    if mentions_status:
        return "status_query"
    if has_run and any(x in value for x in ("修改", "改成", "换成", "不要", "增加", "调整", "不满意", "重新")):
        return "revision"
    if any(x in value for x in ("代码", "证据", "结果", "pipeline", "operator")) and has_run:
        return "artifact_query"
    return "new_task"


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


def message(role: str, content: str, intent: str, run_id: str | None = None, revision: int = 0) -> dict:
    return {"message_id": "msg-" + uuid.uuid4().hex[:12], "role": role, "content": content,
            "created_at": time.time(), "intent": intent, "run_id": run_id, "revision": revision}
