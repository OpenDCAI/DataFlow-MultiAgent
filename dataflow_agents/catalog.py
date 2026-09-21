"""Read DataFlow source contracts without importing heavy optional dependencies."""
from __future__ import annotations

import ast
import hashlib
import json
import re
import warnings
from pathlib import Path
from .team import digest, write_json

SAFE_CPU = {"RemoveExtraSpacesRefiner", "LowercaseRefiner", "TextNormalizationRefiner",
            "HashDeduplicateFilter", "HtmlEntityRefiner"}

def signature(fn):
    if fn is None:
        return {}
    args = fn.args.posonlyargs + fn.args.args
    defaults = [None] * (len(args) - len(fn.args.defaults)) + list(fn.args.defaults)
    pairs = list(zip(args, defaults)) + list(zip(fn.args.kwonlyargs, fn.args.kw_defaults))
    result = {}
    for arg, default in pairs:
        if arg.arg in {"self", "cls", "storage"}:
            continue
        entry = {"required": default is None, "annotation": ast.unparse(arg.annotation) if arg.annotation else ""}
        if default is not None:
            try:
                entry["default"] = ast.literal_eval(default)
            except (ValueError, TypeError):
                entry["expression"] = ast.unparse(default)
        result[arg.arg] = entry
    return result

def extract_source(source, module, source_file, digest=None):
    """Describe the registered operators in one module.

    ``source`` is decoded text for parsing; ``digest`` is the SHA-256 of the
    raw bytes, which is what execution re-checks. Hashing the decoded text
    instead would disagree with that check on any checkout where reading
    translates line endings (CRLF), and every unchanged operator would look
    modified. Callers that only have text may omit the digest, and the text is
    hashed as a last resort.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SyntaxWarning)
        tree = ast.parse(source)
    digest = digest or hashlib.sha256(source.encode()).hexdigest()
    found = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not any("OPERATOR_REGISTRY.register" in ast.unparse(d) for d in node.decorator_list):
            continue
        methods = {m.name: m for m in node.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))}
        if "run" not in methods:
            continue
        desc = methods.get("get_desc")
        strings = [n.value for n in ast.walk(desc) if isinstance(n, ast.Constant) and isinstance(n.value, str)] if desc else []
        description = " ".join(strings) or ast.get_docstring(node) or node.name
        run = signature(methods["run"])
        found.append({"name": node.name, "module": module, "source_file": source_file,
                      "source_sha256": digest,
                      "description": description[:6000], "category": ".".join(module.split(".")[2:-1]),
                      "init_signature": signature(methods.get("__init__")), "run_signature": run,
                      "input_parameters": [k for k in run if k.startswith("input_")],
                      "output_parameters": [k for k in run if k.startswith("output_")],
                      "risk": "low" if node.name in SAFE_CPU else "review",
                      "registered": True})
    return found

def public_import_path(root, rel_path, name, cache=None):
    """Return the shallowest package that re-exports ``name``.

    DataFlow examples import operators from the package root
    (``from dataflow.operators.general_text import HashDeduplicateFilter``)
    rather than the defining module. Generated pipelines follow the same
    convention, so the public path is resolved against the real ``__init__``
    files and falls back to the defining module when nothing re-exports it.
    """
    cache = {} if cache is None else cache
    parts = Path(rel_path).with_suffix("").parts
    for depth in range(3, len(parts)):
        package = ".".join(parts[:depth])
        if package not in cache:
            init = Path(root).joinpath(*parts[:depth], "__init__.py")
            cache[package] = init.read_text(encoding="utf-8") if init.is_file() else ""
        if re.search(rf"\b{re.escape(name)}\b", cache[package]):
            return package
    return ".".join(parts)

def discover_operator_catalog(dataflow_root):
    root = Path(dataflow_root).resolve()
    if not (root / "dataflow/operators").is_dir():
        raise ValueError(f"DataFlow repository not found: {root}")
    result, cache = [], {}
    for path in sorted((root / "dataflow/operators").rglob("*.py")):
        if path.name == "__init__.py":
            continue
        rel = path.relative_to(root)
        raw = path.read_bytes()
        for op in extract_source(raw.decode("utf-8"), ".".join(rel.with_suffix("").parts), rel.as_posix(),
                                 digest=hashlib.sha256(raw).hexdigest()):
            op["import_path"] = public_import_path(root, rel, op["name"], cache)
            result.append(op)
    return result

def catalog_version(catalog):
    return "cat-" + digest(catalog)[:16]

def save_catalog(catalog, path):
    payload = {"catalog_version": catalog_version(catalog), "operators": catalog}
    write_json(path, payload)
    return payload

def load_catalog(path=None):
    path = Path(path or Path(__file__).parents[1] / "catalog/operators.json")
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, list) else data["operators"]

def search_catalog(query, catalog, limit=8):
    aliases = {"清洗": "spaces clean refine", "去重": "hash deduplicate", "规范化": "normalization",
               "空格": "spaces", "小写": "lowercase", "语言": "language", "脱敏": "pii anonymize",
               "日期": "date normalization", "问答": "question answer qa", "分块": "chunk"}
    expanded = query
    for word, terms in aliases.items():
        if word in query:
            expanded += " " + terms
    terms = set(re.findall(r"[a-zA-Z0-9]+|[\\u4e00-\\u9fff]", expanded.lower()))
    ranked = []
    for op in catalog:
        name = op["name"].lower()
        doc = (op["description"] + " " + op["category"]).lower()
        score = sum(5 * (t in name) + (t in doc) for t in terms if len(t) > 1 or ord(t[0]) > 127)
        if score:
            ranked.append((score, op))
    ranked.sort(key=lambda pair: (-pair[0], pair[1]["name"]))
    return [op for _, op in ranked[:limit]]

def with_sources(matches, dataflow_root):
    result = []
    for op in matches:
        raw = (Path(dataflow_root) / op["source_file"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != op["source_sha256"]:
            raise ValueError(f"Stale catalog: {op['name']}")
        result.append(dict(op, source=raw.decode("utf-8")[:32000]))
    return result
