import copy
import asyncio
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from dataflow_agents.backend import CodexBackend, DeterministicBackend
from dataflow_agents.catalog import discover_operator_catalog, extract_source
from dataflow_agents.compiler import compile_spec, ordered_steps
from dataflow_agents.execution import approve, approval_valid, promote, rollback, execute
from dataflow_agents.mcp_contract import dispatch
from dataflow_agents.orchestrator import Orchestrator, load_config
from dataflow_agents.team import TeamStore, CodexTeamRuntime, write_json
from dataflow_agents.contracts import SCHEMAS

CUSTOM = """from dataflow.core import OperatorABC
from dataflow.utils.registry import OPERATOR_REGISTRY

@OPERATOR_REGISTRY.register()
class CountUpperA(OperatorABC):
    @staticmethod
    def get_desc(lang='en'):
        return 'Count uppercase A characters.'
    def run(self, storage, input_key, output_key):
        df = storage.read('dataframe').copy()
        df[output_key] = df[input_key].fillna('').map(lambda value: str(value).count('A'))
        storage.write(df)
        return [output_key]
"""

class CustomBackend(DeterministicBackend):
    def ask(self, role, prompt, **kwargs):
        if role == "planner":
            return {"supported":True,"reason":"test", "steps":[{"step_id":"step-1",
                "objective":"Count uppercase A","query":"CountUpperA","depends_on":[],
                "input_keys":["raw_content"],"output_keys":["a_count"]}], "final_keys":["raw_content","a_count"]}
        if role == "operator_specialist":
            return {"step_id":"step-1","operator":"CountUpperA","init_args":{},
                "run_args":{"input_key":"raw_content","output_key":"a_count"},"prepare_fields":{},
                "rationale":"No exact operator","proposal":{"name":"CountUpperA","source":CUSTOM,"reason":"Missing",
                "tests":[{"input":[{"raw_content":"AAB"},{"raw_content":""}],
                          "expected":[{"raw_content":"AAB","a_count":2},{"raw_content":"","a_count":0}]}]}}
        return super().ask(role,prompt,**kwargs)

class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = load_config(backend="offline", runs_root=str(self.root), auto_execute=True)
        self.engine = Orchestrator(config=self.config)
    def tearDown(self):
        self.tmp.cleanup()

    def test_generation_and_resume_never_execute_by_default(self):
        for custom in (False, True):
            cfg = dict(self.config)
            cfg.pop("auto_execute")
            engine = Orchestrator(config=cfg, backend=CustomBackend() if custom else DeterministicBackend())
            root = self.root / ("generated-custom" if custom else "generated")
            with patch("dataflow_agents.orchestrator.execute", side_effect=AssertionError("Unexpected execution")):
                result = engine.run("Count A" if custom else "清洗空格", run_dir=root)
                self.assertEqual(result["state"], "READY", result)
                self.assertEqual(engine.resume(root)["state"], "READY")
            self.assertTrue((root / "pipeline.py").exists())
            self.assertFalse((root / "approval-request.json").exists())
            self.assertFalse((root / "candidate.jsonl").exists())

    def test_explicit_execution_does_not_require_separate_approval(self):
        cfg = dict(self.config, auto_execute=False)
        root = self.root / "explicit"
        Orchestrator(config=cfg, backend=CustomBackend()).run("Count A", run_dir=root)
        report = execute(root, cfg, user_requested=True)
        self.assertEqual(report["status"], "passed", report)
        self.assertFalse((root / "approval-request.json").exists())
    def test_real_dataflow_clean_dedup(self):
        result = self.engine.run("清洗空格并去重，输出 cleaned_content", run_dir=self.root/"first")
        self.assertEqual(result["state"],"VERIFIED", result)
        rows = [json.loads(x) for x in Path(result["output"]).read_text().splitlines()]
        self.assertEqual(rows,[{"cleaned_content":"Alpha service alert"},{"cleaned_content":"Beta billing event"}])
        report = json.loads((self.root/"first/runtime-report.json").read_text())
        self.assertTrue(report["compile"] and report["executed"])
        jobs = json.loads((self.root/"first/jobs.json").read_text())
        self.assertEqual({j["role"] for j in jobs},{"planner","operator_specialist","pipeline_integrator","verifier"})
    def test_unsupported_is_refused(self):
        result = self.engine.run("发送邮件给客户",run_dir=self.root/"refused")
        self.assertEqual(result["state"],"REFUSED")
        self.assertFalse((self.root/"refused/pipeline.py").exists())
    def test_custom_operator_approval_execution_and_fixtures(self):
        engine = Orchestrator(config=self.config, backend=CustomBackend())
        result = engine.run("Count A", run_dir=self.root/"custom")
        self.assertEqual(result["state"],"APPROVAL_REQUIRED",result)
        self.assertFalse((self.root/"custom/candidate.jsonl").exists())
        approve(self.root/"custom")
        result = engine.resume(self.root/"custom")
        self.assertEqual(result["state"],"VERIFIED",result)
        report = json.loads((self.root/"custom/runtime-report.json").read_text())
        self.assertEqual(report["custom_tests"][0]["status"],"passed")
    def test_approval_invalidated_by_source_change(self):
        engine = Orchestrator(config=self.config, backend=CustomBackend())
        engine.run("Count A",run_dir=self.root/"custom")
        approve(self.root/"custom")
        self.assertTrue(approval_valid(self.root/"custom"))
        path = self.root/"custom/custom/CountUpperA.py"
        path.write_text(path.read_text()+"\n# changed")
        self.assertFalse(approval_valid(self.root/"custom"))
    def test_verifier_rejects_machine_pass(self):
        class RejectingBackend(DeterministicBackend):
            def ask(self,role,prompt,**kw):
                if role == "verifier":
                    return {"verdict":"fail","reason":"Wrong transformation","issues":["Incorrect semantics"]}
                return super().ask(role,prompt,**kw)
        engine = Orchestrator(config=self.config,backend=RejectingBackend())
        result = engine.run("清洗空格",run_dir=self.root/"reject")
        self.assertEqual(result["state"],"BLOCKED")
        self.assertFalse((self.root/"reject/output.jsonl").exists())
    def test_resume_reuses_completed_agent_jobs(self):
        result = self.engine.run("清洗空格",run_dir=self.root/"resume")
        result = self.engine.resume(self.root/"resume")
        self.assertEqual(result["state"],"VERIFIED",result)
        events=(self.root/"resume/events.jsonl").read_text()
        self.assertIn("agent.cache_hit",events)
    def test_changed_input_blocks_resume(self):
        self.engine.run("清洗空格",run_dir=self.root/"changed")
        (self.root/"changed/input.jsonl").write_text('{"raw_content":"tampered"}\n')
        result=self.engine.resume(self.root/"changed")
        self.assertEqual(result["state"],"BLOCKED")
    def test_promote_and_rollback_restore_prior_verified_version(self):
        one=self.engine.run("清洗空格",run_dir=self.root/"one")
        two=self.engine.run("小写 lowercase",run_dir=self.root/"two")
        promote(one["run_dir"],self.root/"deployment")
        promote(two["run_dir"],self.root/"deployment")
        restored=rollback(self.root/"deployment")
        self.assertEqual(restored["run"],one["run_dir"])
    def test_no_custom_flag_is_enforced(self):
        result=Orchestrator(config=self.config,backend=CustomBackend()).run("Count A",run_dir=self.root/"disabled",allow_custom=False)
        self.assertEqual(result["state"],"BLOCKED")

class WebExecutionTests(unittest.TestCase):
    def test_web_generation_requires_explicit_run_and_uses_latest_serving(self):
        try:
            from dataflow_agents.web import create_app, HTTPException
        except RuntimeError as exc:
            self.skipTest(str(exc))
        with tempfile.TemporaryDirectory() as directory:
            # The manual workflow is what this test exercises, so auto_execute
            # is off — the web layer now honours that setting rather than
            # forcing generation to stop at READY.
            cfg = load_config(backend="offline", runs_root=directory, auto_execute=False)
            cfg.update(resources={}, resource_secrets={"test": "unused-test-secret"})
            app = create_app(cfg)
            routes = {(route.path, method): route.endpoint for route in app.routes
                      for method in getattr(route, "methods", [])}
            executor = app.state.executor
            queued = []
            try:
                with patch.object(executor, "submit", side_effect=lambda fn: queued.append(fn)):
                    response = asyncio.run(routes["/api/v1/runs", "POST"]({"request": "Count A"}))
                    run_id = json.loads(response.body)["run_id"]
                    root = Path(directory) / run_id
                    with patch("dataflow_agents.web.Orchestrator", side_effect=lambda config: Orchestrator(config=config, backend=CustomBackend())), \
                         patch("dataflow_agents.orchestrator.execute", side_effect=AssertionError("Generation executed pipeline")):
                        queued.pop(0)()
                    self.assertEqual(routes["/api/v1/runs/{run_id}", "GET"](run_id)["state"], "READY")
                    self.assertFalse((root / "candidate.jsonl").exists())
                    run = routes["/api/v1/runs/{run_id}/execute", "POST"]
                    self.assertEqual(run(run_id)["state"], "RUNNING")
                    with self.assertRaises(HTTPException) as busy:
                        run(run_id)
                    self.assertEqual(busy.exception.status_code, 409)
                    with patch("dataflow_agents.web.Orchestrator", side_effect=AssertionError("Manual run re-planned")):
                        queued.pop(0)()
                    self.assertEqual(routes["/api/v1/runs/{run_id}", "GET"](run_id)["state"], "EXECUTED")
                    self.assertTrue((root / "output.jsonl").exists())
                    self.assertFalse((root / "approval-request.json").exists())

                    spec = json.loads((root / "pipeline-spec.json").read_text())
                    spec["resources"] = {"llm_default": {"type": "api_llm", "args": {"model_name": "old"}}}
                    write_json(root / "pipeline-spec.json", spec)
                    # Keep registration isolated from the user's persistent registries.
                    with patch("dataflow_agents.web.write_json"), patch("dataflow_agents.web._write_secret_registry"):
                        routes["/api/v1/servings", "POST"]({"name": "llm_default", "api_url": "http://localhost:12345/v1",
                                                           "api_key": "test-only", "model_name": "new"})
                    self.assertEqual(queued, [])
                    self.assertEqual(routes["/api/v1/runs/{run_id}", "GET"](run_id)["state"], "EXECUTED")

                    def inspect_execution(run_root, config, *, user_requested=False):
                        self.assertTrue(user_requested)
                        updated = json.loads((run_root / "pipeline-spec.json").read_text())
                        self.assertEqual(updated["resources"]["llm_default"]["args"]["model_name"], "new")
                        self.assertEqual(updated["servings"], updated["resources"])
                        self.assertNotIn("test-only", (run_root / "pipeline.py").read_text())
                        # A stand-in for a real report: manual execution now
                        # field-checks its own output, so the stub has to write
                        # the rows a real run would have written. A report that
                        # omits compile/executed/fields, or a pass with no
                        # output file, is a failed run.
                        rows = [dict.fromkeys(updated["final_keys"], "x")] * 2
                        (run_root / "candidate.jsonl").write_text(
                            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
                        return {"status": "passed", "compile": True, "executed": True,
                                "rows": len(rows), "fields": updated["final_keys"]}

                    run(run_id)
                    with patch("dataflow_agents.web.execute", side_effect=inspect_execution) as execution:
                        queued.pop(0)()
                        execution.assert_called_once()
                    self.assertEqual(routes["/api/v1/runs/{run_id}", "GET"](run_id)["state"], "EXECUTED")
            finally:
                executor.shutdown(wait=True)


class ContractTests(unittest.TestCase):
    def test_codex_jsonl_actual_shape(self):
        result={"supported":False,"reason":"gap","steps":[],"final_keys":[]}
        events='{"type":"thread.started","thread_id":"abc"}\n'+json.dumps({"type":"item.completed","item":{"type":"agent_message","text":json.dumps(result)}})+'\n{"type":"turn.completed","usage":{"input_tokens":12}}'
        self.assertEqual(CodexBackend._parse_jsonl(events),result)
    def test_no_model_answer_cannot_be_success(self):
        with self.assertRaises(ValueError):
            CodexBackend._parse_jsonl('{"type":"turn.completed"}')
    def test_dag_cycles_and_missing_ids(self):
        for dependencies in (["missing"],["a"]):
            with self.assertRaises(ValueError):
                ordered_steps({"steps":[{"step_id":"a","depends_on":dependencies}]})
    def test_signature_and_field_mismatch(self):
        op=extract_source(CUSTOM,"custom.CountUpperA","custom/CountUpperA.py")[0]
        plan={"steps":[{"step_id":"a","depends_on":[],"output_keys":["result"]}], "final_keys":["result"]}
        binding={"step_id":"a","operator":"CountUpperA","proposal":None,"init_args":{},"run_args":{"wrong":"text"},"prepare_fields":{}}
        spec,errors=compile_spec(plan,{"bindings":[binding],"final_keys":["result"]},[op],["text"])
        self.assertTrue(any("unknown parameter" in error for error in errors))
        self.assertTrue(any("missing required" in error for error in errors))
        self.assertTrue(any("not produced" in error for error in errors))

    def test_serving_is_deferred_and_unreferenced_resources_are_ignored(self):
        catalog = discover_operator_catalog(load_config(backend="offline")["dataflow_root"])
        op = next(item for item in catalog if item["name"] == "ReasoningAnswerGenerator")
        plan = {"steps": [{"step_id": "answer", "depends_on": [], "output_keys": ["generated_cot"]}],
                "final_keys": ["generated_cot"]}
        binding = {"step_id": "answer", "operator": op["name"], "proposal": None,
                   "init_args": {}, "run_args": {"input_key": "text", "output_key": "generated_cot"},
                   "prepare_fields": {}}
        spec, errors = compile_spec(plan, {"bindings": [binding], "final_keys": ["generated_cot"]},
                                    [op], ["text"], resources={
                                        "unused": {"type": "api_llm", "args": {
                                            "api_url": "https://example.invalid/v1",
                                            "key_name_of_api_key": "DF_PIPELINE_UNUSED", "model_name": "unused"}}
                                    })
        self.assertEqual(errors, [])
        self.assertIn("llm_default", spec["resources"])
        self.assertNotIn("unused", spec["resources"])
        self.assertEqual(spec["steps"][0]["init_args"]["llm_serving"], {"$resource": "llm_default"})
    def test_mcp_protocol_and_validation(self):
        cfg=load_config(backend="offline")
        response=dispatch({"jsonrpc":"2.0","id":1,"method":"initialize"},cfg)
        self.assertIn("tools",response["result"]["capabilities"])
        response=dispatch({"id":2,"method":"tools/list"},cfg)
        self.assertEqual(len(response["result"]["tools"]),4)
        response=dispatch({"id":3,"method":"tools/call","params":{"name":"evidence.get","arguments":{"run_id":"../secret"}}},cfg)
        self.assertTrue(response["result"]["isError"])

if __name__ == "__main__":
    unittest.main()
