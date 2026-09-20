"""Turn a failed run into an explanation the user can act on.

A failure reaches the user as whatever exception happened to surface first,
which rarely says whether the fault is the submitted data, an unconfigured
serving, or the generated pipeline itself. This module collects the run's
evidence, classifies it deterministically, and hands both to the controller
so the conversation can answer "what went wrong and what do I do now".

The deterministic triage is the part that must always work; the model
analysis layered on top of it is allowed to be unavailable.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

from .team import digest

CATEGORY_LABELS = {
    "input_data": "输入数据不匹配",
    "serving": "Serving / API 配置",
    "generation": "Pipeline 生成",
    "generated_operator": "本次生成的算子",
    "timeout": "执行超时",
    "refused": "需求超出支持范围",
    "other": "未分类失败",
}

FAILURE_STATES = {"BLOCKED", "REFUSED", "RESOURCE_REQUIRED"}

_SERVING_PATTERNS = re.compile(
    r"LLM serving|APILLMServing|api_url|401|403|404|429|5\d{2} Server|Connection|ConnectTimeout|"
    r"ReadTimeout|SSL|Lack of `DF_PIPELINE|Invalid URL|api[_ ]key",
    re.IGNORECASE)


def _read(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _redact(text):
    return re.sub(r"sk-[A-Za-z0-9_-]+", "[REDACTED]", str(text or ""))


def _redact_deep(value):
    """Evidence is sent to a model and shown in the chat; keys travel in errors."""
    if isinstance(value, str):
        return _redact(value)
    if isinstance(value, dict):
        return {key: _redact_deep(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_deep(item) for item in value]
    return value


def _tail(path, limit=1200):
    try:
        return _redact(Path(path).read_text(encoding="utf-8"))[-limit:]
    except OSError:
        return ""


def _input_shape(root):
    path = Path(root) / "input.jsonl"
    try:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except OSError:
        return {"rows": 0, "fields": [], "sample": None}
    first = _read_line(lines[0]) if lines else None
    return {"rows": len(lines), "fields": sorted(first) if isinstance(first, dict) else [],
            "sample": _redact(lines[0])[:240] if lines else None}


def _read_line(line):
    try:
        return json.loads(line)
    except ValueError:
        return None


def _resource_state(spec, config):
    """Which servings the pipeline references, and whether each can be used."""
    secrets = (config or {}).get("resource_secrets", {})
    states = []
    for name, resource in (spec.get("resources") or {}).items():
        args = resource.get("args", {}) or {}
        key = args.get("key_name_of_api_key", "")
        placeholder = str(args.get("api_url", "")).startswith("https://configure-resource.example.invalid")
        states.append({"name": name, "model": args.get("model_name"), "api_url": args.get("api_url"),
                       "registered": not placeholder,
                       "credential": bool(os.getenv(key) or secrets.get(name))})
    return states


def collect_evidence(root, config=None):
    root = Path(root)
    status = _read(root / "status.json", {}) or {}
    runtime = _read(root / "runtime-report.json", {}) or {}
    spec = _read(root / "pipeline-spec.json", {}) or {}
    request = _read(root / "request.json", {}) or {}
    validation = _read(root / "static-validation.json", {}) or {}
    verification = _read(root / "verification.json", {}) or {}
    return {
        "run_id": root.name,
        "state": status.get("state"),
        "request": request.get("request", ""),
        "reason": _redact(status.get("reason") or status.get("error") or ""),
        "input": _input_shape(root),
        "operators": [step.get("operator") for step in spec.get("steps", [])],
        "final_keys": spec.get("final_keys", []),
        "initial_keys": spec.get("initial_keys", []),
        "has_pipeline": (root / "pipeline.py").exists(),
        "resources": _resource_state(spec, config),
        "runtime": _redact_deep({key: runtime.get(key) for key in
                                 ("status", "error", "error_code", "underlying_error", "stage_rows",
                                  "custom_tests", "compile", "executed", "rows", "resources", "duration_ms")
                                 if runtime.get(key) is not None}),
        "static_validation": _redact_deep({"passed": validation.get("passed"),
                                           "errors": validation.get("errors", [])[:6]}),
        "verification": _redact_deep({key: verification.get(key) for key in ("verdict", "reason", "issues")
                                      if key in verification}),
        "stderr_tail": _tail(root / "runtime.stderr.log", 900),
    }


def _empty_stage(evidence):
    rows = evidence["runtime"].get("stage_rows") or []
    return next((item for item in rows if item.get("rows") == 0), None)


def triage(evidence):
    """Classify a failure from evidence alone. Never raises, never guesses wildly."""
    runtime = evidence["runtime"]
    error = str(runtime.get("error") or evidence.get("reason") or "")
    unconfigured = [item for item in evidence["resources"] if not (item["registered"] and item["credential"])]

    if evidence["state"] == "REFUSED":
        return _result("refused", "Planner 判断这个需求超出当前支持范围",
                       evidence.get("reason") or "Planner 没有给出可执行的步骤拆解。",
                       [("revise_request", "换一种表述重新描述需求",
                         "把目标字段、输入字段和每一步要做的事写清楚，避免依赖工作台尚未支持的能力。")])

    if evidence["state"] == "RESOURCE_REQUIRED" or runtime.get("status") == "resource_required" or unconfigured:
        names = "、".join(item["name"] for item in unconfigured) or "、".join(runtime.get("resources") or [])
        missing_credential = [item["name"] for item in unconfigured if item["registered"] and not item["credential"]]
        detail = (f"资源 {names} 已登记但缺少密钥。" if missing_credential
                  else f"Pipeline 引用了尚未注册的 serving：{names or '未知'}。")
        return _result("serving", "缺少可用的 LLM serving", detail,
                       [("configure_serving", "在 Serving / API 中注册并填写密钥",
                         "地址需兼容 Chat Completions；注册后直接点 Run pipeline，无需重新生成。")])

    if runtime.get("error_code") == "EMPTY_STAGE":
        stage = _empty_stage(evidence) or {}
        index = stage.get("step", 0) - 1
        operator = evidence["operators"][index] if 0 <= index < len(evidence["operators"]) else "某个算子"
        fields = "、".join(evidence["input"]["fields"]) or "未知"
        return _result("input_data", f"{operator} 把所有数据行都过滤掉了",
                       f"输入有 {evidence['input']['rows']} 行，字段为 {fields}。"
                       f"第 {stage.get('step', '?')} 步之后没有任何行留下，后续算子读到空表。"
                       "通常是提交的数据和需求不是同一类数据。",
                       [("open_input", "换成与需求匹配的输入数据",
                         "在发送框下方的“输入”里替换成真实数据，或选择已注册的数据集。"),
                        ("revise_request", "或放宽过滤条件后重新生成", "例如去掉质量过滤这一步。")])

    if runtime.get("error_code") == "RUNTIME_TIMEOUT":
        return _result("timeout", "执行超过时间上限", error,
                       [("reduce_input", "先用更少的数据行验证流程", "确认可行后再跑完整数据集。"),
                        ("configure_serving", "或提高 serving 并发度", "并发度太低时，大批量 LLM 调用很容易超时。")])

    failed_fixture = "Custom fixture" in error or "custom_tests" in error
    if failed_fixture or (evidence["operators"] and "proposal" in error.lower()):
        return _result("generated_operator", "本次生成的算子没有通过自带用例", error,
                       [("revise_request", "重新生成并说明该步骤的预期输入输出",
                         "也可以关闭“允许生成新算子”，强制只用 DataFlow 已注册的算子。")])

    if _SERVING_PATTERNS.search(error) or _SERVING_PATTERNS.search(evidence.get("stderr_tail", "")):
        return _result("serving", "调用 LLM serving 失败", error,
                       [("configure_serving", "检查 serving 地址、模型名和密钥",
                         "工作台会把失败的空响应直接报出来；具体 HTTP 错误见 runtime.stderr.log。")])

    if evidence["static_validation"].get("passed") is False or not evidence["has_pipeline"]:
        errors = "；".join(evidence["static_validation"].get("errors") or []) or error
        return _result("generation", "Pipeline 没有通过静态契约检查", errors,
                       [("revise_request", "补充字段说明后重新生成",
                         "写明输入字段、每一步产出的字段和最终要保留的列。")])

    return _result("other", "执行失败", error or "没有捕获到结构化的失败原因。",
                   [("inspect_evidence", "查看运行证据", "右栏“证据”标签和 runtime.stderr.log 保留了完整报错。")])


def _result(category, title, summary, actions):
    return {"category": category, "label": CATEGORY_LABELS[category], "title": title,
            "summary": str(summary)[:800],
            "actions": [{"kind": kind, "label": label, "detail": detail} for kind, label, detail in actions]}


def failure_digest(evidence):
    """Identity of this failure, so the same one is not explained twice."""
    return digest({"state": evidence.get("state"), "reason": evidence.get("reason"),
                   "error": evidence["runtime"].get("error")})[:16]


def triage_message(verdict):
    lines = [f"运行失败：{verdict['title']}（{verdict['label']}）", "", verdict["summary"], "", "建议的处理方式："]
    lines += [f"{index}. {action['label']} —— {action['detail']}"
              for index, action in enumerate(verdict["actions"], start=1)]
    return "\n".join(lines)


def analysis_message(analysis, verdict):
    lines = [f"失败分析（{CATEGORY_LABELS.get(analysis.get('cause_category'), verdict['label'])}）", "",
             analysis.get("diagnosis", "").strip()]
    actions = analysis.get("next_actions") or []
    if actions:
        lines += ["", "下一步："]
        lines += [f"{index}. {item.get('action', '')} —— {item.get('detail', '')}"
                  for index, item in enumerate(actions, start=1)]
    if analysis.get("regenerate_recommended") and analysis.get("suggested_request"):
        lines += ["", "可以直接发送这段修改后的需求重新生成：", analysis["suggested_request"].strip()]
    return "\n".join(line for line in lines if line is not None)
