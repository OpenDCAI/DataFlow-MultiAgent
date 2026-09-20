"""Prompt templates must resolve to prompt objects, or fail with a reason.

DataFlow operators take a prompt *instance*. A raw dict that reaches one fails
inside the operator with "Invalid prompt template type: builtins.dict", which
names neither the step nor the field, so every accepted spelling is resolved at
render time and every other one is rejected with a message that names the
operator and says what to write instead.

This was written after a real failure: a spec carried a prompt reference for
FormatStrPromptedGenerator, the generator emitted that dict verbatim, and the
pipeline only failed when the operator was constructed at run time.
"""
import ast
import unittest
from pathlib import Path

from dataflow_agents.codegen import (discover_prompt_classes, prompt_class_import,
                                     prompt_expression, render_dataflow_pipeline)

# The runner is invoked from tests/, so resolve the root absolutely.
REPO = Path(__file__).resolve().parents[1]
ROOT = str(REPO / "external" / "DataFlow")

D = chr(36)
FORMAT, PROMPT, RESOURCE = D + "format", D + "prompt", D + "resource"

SERVINGS = {"llm_default": {"type": "api_llm", "args": {
    "api_url": "http://127.0.0.1:1/v1", "model_name": "m", "key_name_of_api_key": "DF_PIPELINE_X"}}}
FORMAT_OPERATOR = "FormatStrPromptedGenerator"
FORMAT_IMPORT = "dataflow.operators.core_text"
FORMAT_RUN_ARGS = {"output_key": "out", "input_text": "raw_content"}


def spec(operator, prompt, run_args, import_path):
    return {"initial_keys": ["raw_content"], "final_keys": ["raw_content", "out"], "servings": SERVINGS,
            "steps": [{"step_id": "s1", "operator": operator, "import_path": import_path, "proposal": None,
                       "prepare_fields": {}, "run_args": run_args,
                       "init_args": {"llm_serving": {RESOURCE: "llm_default"},
                                     "prompt_template": prompt}}]}


def render(operator, prompt, run_args, import_path):
    return render_dataflow_pipeline(spec(operator, prompt, run_args, import_path), dataflow_root=ROOT)


def prompt_line(source):
    return next((line.strip() for line in source.splitlines() if "prompt_template" in line), "")


def render_format(prompt):
    source = render(FORMAT_OPERATOR, prompt, FORMAT_RUN_ARGS, FORMAT_IMPORT)
    return source, prompt_line(source)


class FormatPromptTests(unittest.TestCase):
    """The form that caused the field failure."""

    def test_inline_format_arguments_become_an_instance(self):
        source, line = render_format({FORMAT: {"f_str_template": "Summarize {input_text}"}})
        self.assertEqual(line, 'prompt_template=FormatStrPrompt(f_str_template="Summarize {input_text}"),')
        self.assertIn("from dataflow.prompts.core_text import FormatStrPrompt", source)

    def test_an_args_wrapper_is_accepted_too(self):
        _, line = render_format({FORMAT: {"args": {"f_str_template": "Summarize {input_text}"}}})
        self.assertIn('FormatStrPrompt(f_str_template="Summarize {input_text}")', line)

    def test_other_format_arguments_travel_through(self):
        _, line = render_format({FORMAT: {"f_str_template": "x", "on_missing": "empty"}})
        self.assertIn('on_missing="empty"', line)

    def test_naming_the_class_instantiates_it(self):
        _, line = render_format("FormatStrPrompt")
        self.assertEqual(line, "prompt_template=FormatStrPrompt(),")

    def test_null_stays_null(self):
        _, line = render_format(None)
        self.assertEqual(line, "prompt_template=None,")

    def test_rendered_prompt_is_never_a_literal(self):
        for prompt in ({FORMAT: {"f_str_template": "x"}}, "FormatStrPrompt"):
            source, line = render_format(prompt)
            ast.parse(source)
            self.assertNotIn("prompt_template={", line)
            self.assertNotIn("prompt_template=[", line)


class ReasoningPromptTests(unittest.TestCase):
    def test_the_bare_class_name_resolves_for_reasoning_operators(self):
        source = render("ReasoningAnswerGenerator", "MathAnswerGeneratorPrompt",
                        {"input_key": "raw_content", "output_key": "out"}, "dataflow.operators.reasoning")
        self.assertEqual(prompt_line(source), "prompt_template=MathAnswerGeneratorPrompt(),")
        self.assertIn("from dataflow.prompts.reasoning.math import MathAnswerGeneratorPrompt", source)


class RejectedPromptTests(unittest.TestCase):
    def reject(self, value):
        with self.assertRaises(ValueError) as caught:
            render_format(value)
        return str(caught.exception)

    def test_a_raw_mapping_is_refused_with_the_operator_named(self):
        message = self.reject({"f_str_template": "x"})
        self.assertIn("FormatStrPromptedGenerator", message)
        self.assertIn(PROMPT, message)

    def test_format_without_a_template(self):
        self.assertIn("f_str_template", self.reject({FORMAT: {}}))
        self.assertIn("f_str_template", self.reject({FORMAT: {"args": {}}}))

    def test_unknown_format_argument_and_unknown_class(self):
        self.assertIn("unknown FormatStrPrompt", self.reject({FORMAT: {"f_str_template": "x", "oops": 1}}))
        unknown_class = dict(args={})
        unknown_class[PROMPT] = "NopePrompt"
        self.assertIn("not a PromptABC subclass", self.reject(unknown_class))

    def test_non_prompt_types(self):
        for value in (5, ["a"], True):
            self.assertIn("prompt_template", self.reject(value))


class DiscoveryTests(unittest.TestCase):
    def test_concrete_prompt_classes_are_discovered_from_source(self):
        classes = discover_prompt_classes(ROOT)
        for name in ("FormatStrPrompt", "MathAnswerGeneratorPrompt", "CondorQuestionPrompt"):
            self.assertIn(name, classes, f"{name} was not discovered")
        # Abstract bases must never be instantiated.
        self.assertNotIn("PromptABC", classes)
        self.assertNotIn("DIYPromptABC", classes)
        self.assertGreater(len(classes), 50)

    def test_discovered_signatures_carry_their_keyword_arguments(self):
        entry = discover_prompt_classes(ROOT)["FormatStrPrompt"]
        self.assertEqual(entry["module"], "dataflow.prompts.core_text")
        self.assertIn("f_str_template", entry["accepted"])
        self.assertIn("on_missing", entry["accepted"])

    def test_an_unknown_class_names_the_alternatives(self):
        with self.assertRaises(ValueError) as caught:
            prompt_class_import("NopePrompt", ROOT, {})
        self.assertIn("NopePrompt", str(caught.exception))
        self.assertIn(FORMAT, str(caught.exception))

    def test_none_resolves_without_a_dataflow_root(self):
        self.assertEqual(prompt_expression("SomeGenerator", None, {}), "None")


if __name__ == "__main__":
    unittest.main()
