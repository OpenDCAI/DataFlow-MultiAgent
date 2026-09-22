"""Intent routing for the conversation controller.

Two layers, because they fail in opposite directions.

The keyword rules are instant, deterministic and testable, but they only see
words: a request that lists `status` among its output columns reads as a
progress question, which is exactly the bug that made this module necessary.

A TypeSafe Jev decision model answers the same question semantically in a few
hundred milliseconds — no token generation, a typed choice with a probability
distribution — so the controller can afford to ask on every message, unlike a
chat model that would cost 20-60 seconds per Codex process.

The model decides when it is reachable; the rules decide when it is not, and
also cover the one case rules are actually better at: a message that is
nothing but a question is never a data-processing request, whatever a model
says about it.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

JEV_ENDPOINT = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-latest"
JEV_KEY_ENV = "TYPESAFE_API_KEY"
JEV_KEY_NAME = "typesafe"
# The model is opt-in: unit tests and offline use must reach only the rules,
# and a deployment enables it deliberately.
JEV_ENABLE_ENV = "DF_USE_JEV_ROUTING"
JEV_TIMEOUT_SECONDS = 8
# Below this the model is not confident enough to override the rules.
JEV_MIN_CONFIDENCE = 0.5

STATUS_WORDS = ("进度", "到哪", "现在怎么样", "怎么样了", "跑完了吗", "状态", "status", "progress")
# A field name in a spec is an ordinary word: a request that lists `status`
# among its columns is still a request, so these decide the tie.
TASK_VERBS = re.compile(
    r"(清洗|去重|过滤|筛选|生成|提取|转换|合并|拆分|统计|排序|输出|保留|"
    r"deduplicat|filter|clean|extract|generat|normaliz|merg|sort|transform|aggregat)",
    re.IGNORECASE)
TASK_STRUCTURE = (
    re.compile(r"^\s*\d+[.、)]", re.MULTILINE),           # a numbered step list
    re.compile(r"^\s*[-*>]\s+\S", re.MULTILINE),           # a bullet list
    re.compile(r"\{.*\}|\[.*\]", re.DOTALL),               # sample rows
    re.compile(r"(字段|列|columns?|fields?)", re.IGNORECASE),
    re.compile(r"(保留|仅输出|keep only|retain)", re.IGNORECASE),
)
QUESTION_SIGNALS = re.compile(r"(吗|呢|怎样|如何|为什么|什么时候|\?|？|what|when|how|why|is it|are we)",
                              re.IGNORECASE)
REVISION_WORDS = ("修改", "改成", "换成", "不要", "增加", "调整", "不满意", "重新")
ARTIFACT_WORDS = ("代码", "证据", "结果", "pipeline", "operator")
SPECIFICATION_LENGTH = 240

INTENTS = ("new_task", "status_query", "revision", "artifact_query")

JEV_QUESTIONS = {
    "intent": {
        "type": "choice",
        "instructions": "The user is talking to a data-processing workbench. Decide what this message asks for.",
        "criteria": {
            "new_task": "Describes data work to carry out: cleaning, filtering, deduplicating, "
                        "generating, or naming the columns the result must contain. Column names such as "
                        "status or progress appearing inside a specification are field names, not a question.",
            "status_query": "Asks only about the progress or current state of work already started, "
                            "and asks for no data processing.",
            "revision": "Asks to change requirements of work already started, and states what to change.",
            "artifact_query": "Asks to see the generated code, pipeline, evidence or results.",
        },
    },
}


class JevUnavailable(RuntimeError):
    """The decision model could not be reached; callers fall back to the rules."""


def jev_key(config=None):
    """Read the Jev credential from the environment, then the secret registry."""
    key = os.getenv(JEV_KEY_ENV)
    if key:
        return key
    registry = (config or {}).get("resource_secrets")
    if registry is None:
        path = Path(__file__).parents[1] / "config" / "resource-secrets.json"
        try:
            registry = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            registry = {}
    return (registry or {}).get(JEV_KEY_NAME) or ""


def looks_like_task(text: str) -> bool:
    """True when the message describes work, even if it names a status field."""
    structural = sum(1 for pattern in TASK_STRUCTURE if pattern.search(text))
    if len(text) >= SPECIFICATION_LENGTH:
        structural += 1
    if structural < 2:
        return bool(TASK_VERBS.search(text)) and not QUESTION_SIGNALS.search(text)
    return not (QUESTION_SIGNALS.search(text) and len(text) < SPECIFICATION_LENGTH)


def classify_by_rules(text: str, has_run: bool = False) -> str:
    value = text.lower().strip()
    mentions_status = any(word in value for word in STATUS_WORDS)
    if mentions_status and looks_like_task(text):
        if has_run and any(word in value for word in REVISION_WORDS):
            return "revision"
        return "new_task"
    if mentions_status:
        return "status_query"
    if has_run and any(word in value for word in REVISION_WORDS):
        return "revision"
    if any(word in value for word in ARTIFACT_WORDS) and has_run:
        return "artifact_query"
    return "new_task"


def ask_jev(text: str, key: str, endpoint: str = JEV_ENDPOINT,
            timeout: int = JEV_TIMEOUT_SECONDS) -> tuple[str, float]:
    """Return (intent, confidence) from the decision model, or raise."""
    if not key:
        raise JevUnavailable("no Jev credential configured")
    payload = json.dumps({"state": text, "model": JEV_MODEL, "questions": JEV_QUESTIONS}).encode("utf-8")
    request = urllib.request.Request(endpoint, data=payload, method="POST", headers={
        "Content-Type": "application/json", "Authorization": "Bearer " + key})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise JevUnavailable(f"Jev HTTP {exc.code}") from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise JevUnavailable(f"Jev unreachable: {type(exc).__name__}") from exc
    answer = ((body or {}).get("answers") or {}).get("intent") or {}
    choice = answer.get("choice")
    if choice not in INTENTS:
        raise JevUnavailable(f"Jev returned an unknown intent: {choice!r}")
    return choice, float(answer.get("confidence") or 0.0)


def jev_enabled(config=None):
    """Whether the decision model should be consulted at all."""
    value = os.getenv(JEV_ENABLE_ENV)
    if value is not None:
        return value.strip().lower() not in ("", "0", "false", "no", "off")
    return bool((config or {}).get("use_jev_routing"))


def classify_message(text: str, has_run: bool = False, *, config=None, use_model=None) -> str:
    """Route one message to an intent, preferring the model and degrading to rules."""
    return classify_with_source(text, has_run, config=config, use_model=use_model)[0]


def classify_with_source(text: str, has_run: bool = False, *, config=None,
                         use_model=None) -> tuple[str, dict]:
    """Route one message, and report how the decision was reached.

    The provenance is recorded so a disagreement between the two layers can be
    reviewed later instead of being invisible.
    """
    rules = classify_by_rules(text, has_run)
    if use_model is None:
        use_model = jev_enabled(config)
    if not use_model:
        return rules, {"by": "rules", "intent": rules}
    try:
        intent, confidence = ask_jev(text, jev_key(config))
    except JevUnavailable as exc:
        return rules, {"by": "rules", "intent": rules, "model_unavailable": str(exc)[:200]}
    if confidence < JEV_MIN_CONFIDENCE:
        # Too close to call; the rules are the safer tie-break.
        return rules, {"by": "rules", "intent": rules, "model_intent": intent,
                       "model_confidence": round(confidence, 3), "deferred": "low_confidence"}
    return intent, {"by": "jev", "intent": intent, "model_confidence": round(confidence, 3),
                    "rules_intent": rules}


def main(argv=None):
    """Check the live decision model without touching the rest of the app.

        python -m dataflow_agents.routing "清洗 status 字段并按 status 去重"
        python -m dataflow_agents.routing --samples
    """
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    samples = [
        "清洗 raw_content 的多余空格并按清洗结果去重，输出 cleaned_content",
        "7. 对 tracking_no、status 相同的记录去重，保留 created_at 最早的记录。\n8. 最终仅输出 order_id, status, carrier。",
        "现在进度怎么样了？",
        "改成小写输出",
        "Deduplicate rows with the same tracking_no and status, then output order_id and status.",
    ]
    texts = samples if (not args or args[0] == "--samples") else [" ".join(args)]
    key = jev_key()
    print(f"credential: {'found' if key else 'MISSING'} | model routing: "
          f"{'enabled' if jev_enabled() else 'disabled (set DF_USE_JEV_ROUTING=1)'}\n")
    for text in texts:
        rules = classify_by_rules(text)
        started = time.monotonic()
        try:
            intent, confidence = ask_jev(text, key)
            elapsed = f"{(time.monotonic() - started) * 1000:.0f} ms"
            mark = "same" if intent == rules else f"rules said {rules}"
            print(f"  jev={intent:<15} conf={confidence:<5} {elapsed:>8}  ({mark})")
        except JevUnavailable as exc:
            print(f"  jev unavailable -> {exc}; rules said {rules}")
        print(f"    {text.strip().splitlines()[0][:78]}")


if __name__ == "__main__":
    main()
