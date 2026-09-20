"""Render a validated pipeline spec as idiomatic DataFlow source.

The emitted ``pipeline.py`` follows the conventions of the pipelines DataFlow
ships in ``dataflow/statics/pipelines``: operators are imported from their
public package, constructed as named attributes in ``__init__`` with literal
arguments, and invoked one by one in ``forward``. Nothing about the workbench
leaks into that file, so a reviewer can copy it into a DataFlow checkout and
run it unchanged.

Workbench execution needs more than ``forward()`` (fixtures for generated
operators, a machine readable report, a projected output file). That harness
lives in the separate ``run_pipeline.py`` runner, which imports the pipeline
class instead of inlining a generic interpreter.
"""
from __future__ import annotations

import ast
import keyword
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

from .prompt_templates import PROMPT_CLASSES, OPERATOR_PROMPTS, normalize_prompt

# Classes that exist only to be subclassed, so they must never be instantiated.
ABSTRACT_PROMPTS = {"PromptABC", "DIYPromptABC"}

from .serving import normalize_chat_url

RUNNER_FILENAME = "run_pipeline.py"
DEFAULT_INPUT_FILE = "./input.jsonl"
DEFAULT_CACHE_PATH = "./cache"

SERVING_CLASSES = {"api_llm": ("dataflow.serving", "APILLMServing_request")}

COPY_OPERATOR = '''class CopyFieldRefiner(OperatorABC):
    """Duplicate a column so in-place refiners keep the original content."""

    @staticmethod
    def get_desc(lang: str = "zh"):
        return "复制字段到新列。" if lang == "zh" else "Copy one column into a new column."

    def run(self, storage: DataFlowStorage, input_key: str, output_key: str):
        dataframe = storage.read("dataframe").copy()
        dataframe[output_key] = dataframe[input_key]
        storage.write(dataframe)
        return [output_key]
'''


def snake(name):
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower()
    return text or "operator"


def identifier(name, fallback="value"):
    text = re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")
    if not text or text[0].isdigit() or keyword.iskeyword(text):
        text = fallback + "_" + text if text else fallback
    return text


def camel(name):
    return "".join(part[:1].upper() + part[1:] for part in snake(name).split("_") if part)


def literal(value, indent=0):
    """Render a JSON-compatible spec value as readable Python source."""
    pad = " " * indent
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'
    if isinstance(value, bool) or value is None:
        return {True: "True", False: "False", None: "None"}[value]
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, list):
        if not value:
            return "[]"
        inline = "[" + ", ".join(literal(item) for item in value) + "]"
        if len(inline) + indent <= 96 and "\n" not in inline:
            return inline
        items = ",\n".join(pad + "    " + literal(item, indent + 4) for item in value)
        return "[\n" + items + ",\n" + pad + "]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        inline = "{" + ", ".join(f"{literal(k)}: {literal(v)}" for k, v in value.items()) + "}"
        if len(inline) + indent <= 96 and "\n" not in inline:
            return inline
        items = ",\n".join(pad + "    " + f"{literal(k)}: {literal(v, indent + 4)}" for k, v in value.items())
        return "{\n" + items + ",\n" + pad + "}"
    raise ValueError(f"Cannot render {type(value).__name__} in generated source")


def _is_reference(value, *keys):
    return isinstance(value, dict) and len(value) == 1 and set(value) <= set(keys)


def serving_attribute(name):
    return identifier(snake(name), "serving")


def discover_prompt_classes(dataflow_root):
    """Map every concrete PromptABC subclass in DataFlow to its import path.

    Operators accept a prompt *object*, and the class a spec names is what the
    generated code has to construct. Modules are scanned rather than imported:
    a prompt module can pull in optional dependencies the workbench does not
    need, and the source is what the catalog already treats as authoritative.
    """
    return dict(_prompt_class_index(str(Path(dataflow_root).resolve())))


@lru_cache(maxsize=8)
def _prompt_class_index(root_text):
    root = Path(root_text)
    classes = {}
    for path in sorted((root / "dataflow" / "prompts").rglob("*.py")):
        if path.name == "__init__.py":
            continue
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text)
        except (OSError, SyntaxError):
            continue
        parts = path.relative_to(root).with_suffix("").parts
        module = ".".join(parts)
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or node.name in ABSTRACT_PROMPTS:
                continue
            bases = [ast.unparse(base).split(".")[-1] for base in node.bases]
            if not any("Prompt" in base for base in bases):
                continue
            init = next((item for item in node.body
                         if isinstance(item, ast.FunctionDef) and item.name == "__init__"), None)
            args = []
            if init is not None:
                pairs = list(zip(init.args.args, [None] * (len(init.args.args) - len(init.args.defaults))
                                 + list(init.args.defaults)))
                args = [arg.arg for arg, default in pairs
                        if arg.arg != "self" and default is not None]
                for arg, default in pairs:
                    if arg.arg == "self" or default is not None:
                        continue
                    args.append(arg.arg + "?")
            classes[node.name] = {"module": module, "required": [a for a in args if a.endswith("?")],
                                  "accepted": [a.replace("?", "") for a in args]}
    return classes


def prompt_class_import(name, dataflow_root, imports):
    """Register ``name`` for import, preferring the shallowest public module."""
    classes = discover_prompt_classes(dataflow_root) if dataflow_root else {}
    entry = classes.get(name)
    if entry is None:
        raise ValueError(
            f"prompt_template names {name!r}, which is not a PromptABC subclass in this DataFlow "
            "checkout. Use a known prompt class, or a {'$format': {'f_str_template': ...}} reference.")
    imports.setdefault(entry["module"], set()).add(name)
    return entry


def format_str_expression(operator, value, imports, dataflow_root):
    """Render a ``$format`` reference as a real FormatStrPrompt instance."""
    from .prompt_templates import FORMAT_PROMPT_CLASS

    prompt_class_import(FORMAT_PROMPT_CLASS, dataflow_root, imports)
    # value is the argument mapping itself, e.g. {"f_str_template": "..."},
    # with an optional "args" wrapper for symmetry with the $prompt form.
    nested = value.get("args")
    settings = dict(nested) if isinstance(nested, dict) else dict(value)
    if not settings.get("f_str_template"):
        raise ValueError(f"{operator}.prompt_template requires a non-empty f_str_template")
    unknown = sorted(set(settings) - {"f_str_template", "on_missing"})
    if unknown:
        raise ValueError(f"{operator}.prompt_template: unknown FormatStrPrompt arguments {unknown}")
    template = settings["f_str_template"]
    if not isinstance(template, str) or not template.strip():
        raise ValueError(f"{operator}.prompt_template requires a non-empty f_str_template")
    rendered = ", ".join(f"{key}={literal(item)}" for key, item in settings.items())
    return f"{FORMAT_PROMPT_CLASS}({rendered})"


def _prompt_expression(operator, value, imports, dataflow_root=None):
    reference = normalize_prompt(operator, value)
    name = OPERATOR_PROMPTS[operator][0] if reference is None else reference["$prompt"]
    args = {} if reference is None else reference["args"]
    imports.setdefault(PROMPT_CLASSES[name], set()).add(name)
    rendered = ", ".join(f"{key}={literal(item)}" for key, item in args.items())
    return f"{name}({rendered})"


def argument_expression(operator, key, value, imports, proposal=None, dataflow_root=None):
    if _is_reference(value, "$resource", "$serving"):
        return "self." + serving_attribute(next(iter(value.values())))
    if key == "prompt_template" and not proposal:
        return prompt_expression(operator, value, imports, dataflow_root)
    return literal(value, 12)


# Spec markers, spelled from chr(36) so a shell can never eat the leading character.
MARKER_PREFIX = chr(36)
MARKER_RESOURCE = MARKER_PREFIX + "resource"
MARKER_SERVING = MARKER_PREFIX + "serving"
MARKER_PROMPT = MARKER_PREFIX + "prompt"
MARKER_FORMAT = MARKER_PREFIX + "format"


def format_settings(value):
    """Extract the FormatStrPrompt keyword arguments from a marker reference.

    Two shapes are accepted, because both read naturally in a spec: the
    arguments inline beside the marker, or an explicit ``args`` wrapper.
    """
    nested = value.get("args")
    if isinstance(nested, dict):
        return dict(nested)
    settings = {}
    inline = value.get(MARKER_FORMAT)
    if isinstance(inline, dict):
        return dict(inline)
    return settings


def prompt_expression(operator, value, imports, dataflow_root=None):
    """Resolve any prompt_template the spec may carry into a real instance.

    DataFlow operators take a prompt *object*. Anything else — a raw dict, a
    JSON string — fails inside the operator with a type error that says
    nothing about where it came from, so every accepted form is resolved here.
    """
    if value is None:
        return "None"
    if isinstance(value, dict) and MARKER_FORMAT in value:
        settings = format_settings(value)
        if not settings:
            raise ValueError(
                f"{operator}.prompt_template: {MARKER_FORMAT} needs the FormatStrPrompt arguments, "
                f'e.g. {{"{MARKER_FORMAT}": {{"f_str_template": "..."}}}}')
        return format_str_expression(operator, settings, imports, dataflow_root)
    if operator in OPERATOR_PROMPTS:
        return _prompt_expression(operator, value, imports)
    if isinstance(value, str):
        value = {"$prompt": value, "args": {}}
    if isinstance(value, dict) and "$prompt" in value:
        name, args = value["$prompt"], value.get("args") or {}
        if not isinstance(name, str) or not isinstance(args, dict):
            raise ValueError(f"{operator}.prompt_template: $prompt must name a class and args must be an object")
        entry = prompt_class_import(name, dataflow_root, imports)
        required = [arg for arg in entry["required"]]
        missing = [arg for arg in required if arg not in args]
        if missing:
            raise ValueError(f"{operator}.prompt_template: {name} requires {missing}")
        unknown = set(args) - set(entry["accepted"])
        if unknown:
            raise ValueError(f"{operator}.prompt_template: {name} does not accept {sorted(unknown)}")
        rendered = ", ".join(f"{key}={literal(item)}" for key, item in args.items())
        return f"{name}({rendered})"
    raise ValueError(
        f"{operator}.prompt_template: use null, a prompt class name, "
        '{' + '"$prompt": "<ClassName>", "args": {...}' + '}, or '
        '{' + '"$format": {"f_str_template": "..."}' + '}')


def _call(target, arguments, indent):
    pad = " " * indent
    if not arguments:
        return f"{pad}{target}()\n"
    body = "".join(f"{pad}    {key}={value},\n" for key, value in arguments)
    return f"{pad}{target}(\n{body}{pad})\n"


def pipeline_class_name(spec):
    """Name the class the way DataFlow names its own example pipelines."""
    packages = Counter()
    for step in spec["steps"]:
        if step.get("proposal"):
            packages["custom"] += 1
            continue
        parts = (step.get("import_path") or step.get("module") or "").split(".")
        packages[parts[2] if len(parts) > 2 else "dataflow"] += 1
    domain = camel(packages.most_common(1)[0][0]) if packages else "DataFlow"
    return f"{domain}_{'API' if spec.get('servings') or spec.get('resources') else 'CPU'}Pipeline"


def plan_attributes(spec):
    """Assign one readable attribute name per copy helper and per operator."""
    plan, used = [], set()
    for index, step in enumerate(spec["steps"], start=1):
        for target, source in (step.get("prepare_fields") or {}).items():
            name = identifier(f"copy_{snake(target)}_step{index}", "copy")
            plan.append({"kind": "copy", "attribute": name, "step": step, "index": index,
                         "run_args": {"input_key": source, "output_key": target}})
            used.add(name)
        name = identifier(f"{snake(step['operator'])}_step{index}", "operator")
        while name in used:
            name += "_x"
        used.add(name)
        plan.append({"kind": "operator", "attribute": name, "step": step, "index": index,
                     "run_args": step.get("run_args") or {}})
    return plan


def render_dataflow_pipeline(spec, request=None, input_file=DEFAULT_INPUT_FILE, cache_path=DEFAULT_CACHE_PATH,
                            dataflow_root=None):
    steps = spec["steps"]
    attributes = plan_attributes(spec)
    needs_copy = any(item["kind"] == "copy" for item in attributes)
    imports = {"dataflow.pipeline": {"PipelineABC"}, "dataflow.utils.storage": {"FileStorage"}}
    if needs_copy:
        imports.setdefault("dataflow.core", set()).add("OperatorABC")
        imports.setdefault("dataflow.utils.storage", set()).add("DataFlowStorage")

    declared = spec.get("servings") or spec.get("resources") or {}
    for step in steps:
        for value in (step.get("init_args") or {}).values():
            if _is_reference(value, "$resource", "$serving"):
                name = next(iter(value.values()))
                if name not in declared:
                    raise ValueError(f"Pipeline spec references undeclared serving resource {name!r}")

    servings = []
    for name, resource in declared.items():
        module, class_name = SERVING_CLASSES.get(resource.get("type"), SERVING_CLASSES["api_llm"])
        imports.setdefault(module, set()).add(class_name)
        args = dict(resource.get("args") or {})
        if args.get("api_url"):
            args["api_url"] = normalize_chat_url(args["api_url"])
        servings.append((serving_attribute(name), class_name, args))

    constructors, calls = [], []
    for item in attributes:
        step, attribute = item["step"], item["attribute"]
        if item["kind"] == "copy":
            constructors.append(_call(f"self.{attribute} = CopyFieldRefiner", [], 8))
        else:
            operator = step["operator"]
            if step.get("proposal"):
                imports.setdefault("custom." + operator, set()).add(operator)
            else:
                module = step.get("import_path") or step.get("module")
                if not module:
                    raise ValueError(f"Pipeline spec step {step.get('step_id')!r} has no operator module to import")
                imports.setdefault(module, set()).add(operator)
            init_args = dict(step.get("init_args") or {})
            if operator in OPERATOR_PROMPTS and not step.get("proposal"):
                # An absent or null prompt_template would reach the operator as
                # None, which it rejects. Name the operator's first supported
                # template instead, matching DataFlow's own default.
                reference = normalize_prompt(operator, init_args.get("prompt_template"))
                init_args["prompt_template"] = reference or {
                    "$prompt": OPERATOR_PROMPTS[operator][0], "args": {}}
            arguments = [(key, argument_expression(operator, key, value, imports, step.get("proposal"),
                                                  dataflow_root))
                         for key, value in init_args.items()]
            constructors.append(_call(f"self.{attribute} = {operator}", arguments, 8))
        run_arguments = [("storage", "self.storage.step()")]
        run_arguments += [(key, literal(value, 16)) for key, value in item["run_args"].items()]
        calls.append(_call(f"self.{attribute}.run", run_arguments, 8))

    import_lines = []
    for module in sorted(imports):
        names = ", ".join(sorted(imports[module]))
        import_lines.append(f"from {module} import {names}\n")

    class_name = pipeline_class_name(spec)
    header = ['"""DataFlow pipeline generated by the multi-agent workbench.\n']
    if request:
        header.append("\nRequest: " + " ".join(str(request).split())[:400] + "\n")
    header.append("\nOperators and arguments come from the validated pipeline-spec.json next to this\n"
                  "file. Regenerate the spec instead of hand editing generated code.\n"
                  '"""\n')

    serving_source = ""
    for attribute, class_name_, args in servings:
        serving_source += _call(f"self.{attribute} = {class_name_}",
                                [(key, literal(value, 12)) for key, value in args.items()], 8)

    source = ("".join(header) + "\n" + "".join(import_lines) + "\n\n"
              + (COPY_OPERATOR + "\n\n" if needs_copy else "")
              + f"class {class_name}(PipelineABC):\n"
              + f'    def __init__(self, input_file: str = {literal(input_file)},\n'
              + f'                 cache_path: str = {literal(cache_path)}):\n'
              + "        super().__init__()\n"
              + "        self.storage = FileStorage(\n"
              + "            first_entry_file_name=input_file,\n"
              + "            cache_path=cache_path,\n"
              + '            file_name_prefix="dataflow_cache_step",\n'
              + '            cache_type="jsonl",\n'
              + "        )\n"
              + serving_source
              + "".join(constructors)
              + "\n    def forward(self):\n"
              + ("".join(calls) if calls else "        pass\n")
              + "\n\nif __name__ == \"__main__\":\n"
              + f"    pipeline = {class_name}()\n"
              + "    pipeline.compile()\n"
              + "    pipeline.forward()\n")
    ast.parse(source)
    return source


def render_pipeline_runner():
    """Return the workbench runner that executes a generated pipeline."""
    return (Path(__file__).with_name("pipeline_runner.py")).read_text(encoding="utf-8")


def write_pipeline_sources(root, spec, request=None, dataflow_root=None):
    """Write pipeline.py, the runner and any generated operator module.

    Generated operators are imported by ``pipeline.py`` as ``custom.<Name>``.
    The package marker keeps that import working for both ``python
    pipeline.py`` and the runner, because the run directory is on ``sys.path``.
    """
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    (root / "pipeline.py").write_text(
        render_dataflow_pipeline(spec, request=request, dataflow_root=dataflow_root), encoding="utf-8")
    (root / RUNNER_FILENAME).write_text(render_pipeline_runner(), encoding="utf-8")
    custom = [step for step in spec["steps"] if step.get("proposal")]
    if custom:
        (root / "custom").mkdir(exist_ok=True)
        (root / "custom/__init__.py").write_text(
            '"""Operators generated for this run."""\n', encoding="utf-8")
        for step in custom:
            (root / step["source_file"]).write_text(step["proposal"]["source"], encoding="utf-8")
    return root / "pipeline.py"
