"""Workbench runner for a generated DataFlow pipeline.

This file is copied next to ``pipeline.py`` inside a run directory. It keeps
every workbench-only concern — fixtures for generated operators, the machine
readable report, the projected output file — out of the pipeline itself, so
the reviewed pipeline stays plain DataFlow source.

    python run_pipeline.py --input input.jsonl --cache cache \\
        --output candidate.jsonl --report runtime-report.json --execute
"""
from __future__ import annotations

import argparse
import importlib.util
import inspect
import json
import re
import sys
from pathlib import Path

RUN_DIR = Path(__file__).resolve().parent


def load_pipeline_class(path):
    """Import ``pipeline.py`` and return its single PipelineABC subclass."""
    from dataflow.pipeline import PipelineABC

    if str(RUN_DIR) not in sys.path:
        sys.path.insert(0, str(RUN_DIR))
    spec = importlib.util.spec_from_file_location("generated_pipeline", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    classes = [value for value in vars(module).values()
               if inspect.isclass(value) and issubclass(value, PipelineABC) and value is not PipelineABC]
    if len(classes) != 1:
        raise ValueError(f"Expected exactly one pipeline class in {path}, found {len(classes)}")
    return classes[0]


def operator_instances(pipeline):
    """Operator attributes in declaration order, before compile() wraps them."""
    from dataflow.core import OperatorABC

    return [(name, value) for name, value in vars(pipeline).items() if isinstance(value, OperatorABC)]


def guard_servings(pipeline):
    """Turn silent empty API responses into an actionable runtime error.

    ``APILLMServing_request`` returns ``None`` for failed requests, which
    surfaces much later as an unrelated schema error. The check is attached to
    the instance so the generated pipeline needs no workbench-specific class.
    """
    from dataflow.core import LLMServingABC

    for name, serving in vars(pipeline).items():
        if not isinstance(serving, LLMServingABC) or not hasattr(serving, "_run_threadpool"):
            continue
        original = serving._run_threadpool

        def checked(task_args_list, desc, _original=original, _serving=serving, _name=name):
            responses = _original(task_args_list, desc)
            if len(responses) != len(task_args_list) or any(
                response is None or (isinstance(response, str) and not response.strip())
                for response in responses
            ):
                raise RuntimeError(
                    f"LLM serving {_name}: request failed or returned empty content "
                    f"(model={getattr(_serving, 'model_name', '?')}, endpoint={getattr(_serving, 'api_url', '?')}). "
                    "Check serving URL, credentials and model; see runtime.stderr.log for HTTP errors."
                )
            return responses

        serving._run_threadpool = checked


def read_last(pipeline):
    operators = [node for node in pipeline.op_nodes_list if node.op_obj is not None]
    storage = operators[-1].storage
    # FileStorage.read reads this stage's input, so read the next stage instead.
    return storage.step().read("dataframe")


def self_test(spec, instances, cache):
    """Run the fixtures a specialist shipped with a generated operator."""
    from dataflow.utils.storage import FileStorage

    pending = list(instances)
    reports = []
    for step in spec["steps"]:
        proposal = step.get("proposal")
        operator = None
        for index, (_, candidate) in enumerate(pending):
            if type(candidate).__name__ == step["operator"]:
                operator = pending.pop(index)[1]
                break
        if not proposal or operator is None:
            continue
        for index, fixture in enumerate(proposal.get("tests") or []):
            directory = Path(cache) / ("test_" + step["step_id"] + "_" + str(index))
            directory.mkdir(parents=True, exist_ok=True)
            input_file = directory / "input.jsonl"
            input_file.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in fixture["input"]),
                                  encoding="utf-8")
            storage = FileStorage(first_entry_file_name=str(input_file), cache_path=str(directory), cache_type="jsonl")
            stage = storage.step()
            operator.run(storage=stage, **step["run_args"])
            actual = json.loads(stage.step().read("dataframe").to_json(orient="records"))
            expected = fixture["expected"]
            # LLM-backed operators are intentionally semantic: equivalent
            # labels can vary between valid model responses. Fixtures verify
            # deterministic structure, input preservation, and non-empty
            # declared outputs; opt into exact values with strict=true.
            if fixture.get("strict"):
                if actual != expected:
                    raise AssertionError(f"Custom fixture failed: {step['step_id']}/{index}: {actual} != {expected}")
            else:
                if len(actual) != len(expected):
                    raise AssertionError(f"Custom fixture row count failed: {step['step_id']}/{index}")
                for actual_row, expected_row in zip(actual, expected):
                    for key, value in expected_row.items():
                        if key in step.get("input_keys", []) and actual_row.get(key) != value:
                            raise AssertionError(f"Custom fixture input preservation failed: {step['step_id']}/{index}/{key}")
                    for key in step.get("output_keys", []):
                        value = actual_row.get(key)
                        if value is None or (isinstance(value, str) and not value.strip()):
                            raise AssertionError(f"Custom fixture output missing: {step['step_id']}/{index}/{key}")
            reports.append({"step": step["step_id"], "fixture": index, "status": "passed",
                            "strict": bool(fixture.get("strict"))})
    return reports


def clear_stage_files(cache):
    """Remove stage files from a previous execution of this run.

    DataFlow writes one file per operator and never cleans up, so a re-run
    would leave the files it did not reach. Those stale files are then read
    back as the current output: the stage panel shows the earlier attempt's
    rows, and a run that failed at step 1 still appears to have produced
    every later step. Fixture directories are left alone; they are keyed by
    step and overwritten on each run.
    """
    directory = Path(cache)
    if not directory.is_dir():
        return
    for path in directory.glob("*_step*.jsonl"):
        try:
            path.unlink()
        except OSError:
            pass


def stage_rows(cache):
    """Rows each operator wrote, in execution order.

    FileStorage names every stage ``<prefix>_step<N>.jsonl``, so the files
    give a per-step row count without instrumenting the pipeline itself.
    """
    counts = []
    for path in Path(cache).glob("*_step*.jsonl"):
        match = re.search(r"step(\d+)\.jsonl$", path.name)
        if not match:
            continue
        rows = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
        counts.append({"step": int(match.group(1)), "rows": rows, "file": path.name})
    return sorted(counts, key=lambda item: item["step"])


def explain_empty_stage(stages, operators):
    """Turn 'a filter removed every row' into an error that says so.

    An emptied stage surfaces downstream as a missing-column error from the
    next operator, which reads like a pipeline defect rather than what it is:
    no row survived, usually because the input data does not match the request.
    """
    empty = next((item for item in stages if item["rows"] == 0), None)
    if empty is None:
        return None
    index = empty["step"] - 1
    name = operators[index] if 0 <= index < len(operators) else f"step {empty['step']}"
    return (f"Step {empty['step']} ({name}) wrote 0 rows: every row was filtered out, so the next "
            "operator finds none of its input columns. Check that the submitted input data matches "
            "the request — a filter that rejects the whole dataset is the usual cause.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--spec", default=str(RUN_DIR / "pipeline-spec.json"))
    parser.add_argument("--pipeline", default=str(RUN_DIR / "pipeline.py"))
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    report = {"status": "blocked", "compile": False, "executed": False, "custom_tests": []}
    try:
        spec = json.loads(Path(args.spec).read_text(encoding="utf-8"))
        pipeline = load_pipeline_class(args.pipeline)(input_file=args.input, cache_path=args.cache)
        report["pipeline_class"] = type(pipeline).__name__
        instances = operator_instances(pipeline)
        guard_servings(pipeline)
        pipeline.compile()
        report["compile"] = True
        report["compiled_fields"] = pipeline.final_keys
        report["operators"] = [name for name, _ in instances]
        if args.execute:
            clear_stage_files(args.cache)
            report["custom_tests"] = self_test(spec, instances, args.cache)
            pipeline.forward()
            data = read_last(pipeline)
            missing = set(spec["final_keys"]) - set(data.columns)
            if missing:
                raise AssertionError(f"Output fields missing: {missing}")
            data[spec["final_keys"]].to_json(args.output, orient="records", lines=True, force_ascii=False)
            report.update(status="passed", executed=True, rows=len(data), fields=list(data.columns))
    except Exception as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        explanation = explain_empty_stage(stage_rows(args.cache), report.get("operators", []))
        if explanation:
            report.update(error=explanation, error_code="EMPTY_STAGE",
                          underlying_error=f"{type(exc).__name__}: {exc}")
    report["stage_rows"] = stage_rows(args.cache)
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if report["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
