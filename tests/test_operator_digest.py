"""Operator digests must mean the same thing at generation and at execution.

The catalog hashes each operator when the pipeline is generated; execution
re-checks the file before running it. Both must hash the same bytes, or an
untouched operator is reported as modified and the pipeline cannot run.

The two paths used to disagree: the catalog hashed ``source.encode()`` after
``read_text``, which applies universal-newline translation, while execution
hashed ``read_bytes``. On a checkout with CRLF line endings every operator
looked modified. These tests pin the invariant by comparing the two digests
directly rather than by asserting a particular hashing call.
"""
import hashlib
import tempfile
import unittest
from pathlib import Path

from dataflow_agents.catalog import discover_operator_catalog, with_sources, search_catalog

REGISTERED = '''from dataflow.utils.registry import OPERATOR_REGISTRY
from dataflow.core import OperatorABC
from dataflow.utils.storage import DataFlowStorage


@OPERATOR_REGISTRY.register()
class SampleRefiner(OperatorABC):
    """Trim whitespace from one column."""

    @staticmethod
    def get_desc(lang="zh"):
        return "Trim whitespace."

    def run(self, storage: DataFlowStorage, input_key: str, output_key: str):
        dataframe = storage.read("dataframe").copy()
        dataframe[output_key] = dataframe[input_key].str.strip()
        storage.write(dataframe)
        return [output_key]
'''


def build_checkout(root, newline):
    """A minimal DataFlow checkout whose operator file uses `newline`."""
    operators = Path(root) / "dataflow" / "operators" / "sample"
    operators.mkdir(parents=True)
    (operators / "__init__.py").write_text("from .refine import SampleRefiner\n", encoding="utf-8")
    (operators / "refine.py").write_bytes(REGISTERED.replace("\n", newline).encode("utf-8"))
    return Path(root)


def hash_as_execution_does(root, source_file):
    """The digest execution checks, expressed independently of the catalog."""
    return hashlib.sha256((Path(root) / source_file).read_bytes()).hexdigest()


class DigestAgreementTests(unittest.TestCase):
    """The generation digest and the execution digest must be the same value."""

    def check(self, newline, label):
        with tempfile.TemporaryDirectory() as directory:
            root = build_checkout(directory, newline)
            catalog = discover_operator_catalog(root)
            self.assertEqual(len(catalog), 1, f"{label}: operator was not indexed")
            entry = catalog[0]
            expected = hash_as_execution_does(root, entry["source_file"])
            self.assertEqual(entry["source_sha256"], expected,
                             f"{label}: catalog digest differs from the execution digest, "
                             "so an unchanged operator fails check_sources()")
            # The same value drives the review panel and the staleness guard.
            self.assertEqual(with_sources(catalog, root)[0]["source_sha256"], expected,
                             f"{label}: re-reading the source for prompts changed the digest")
            return entry

    def test_lf_checkout(self):
        self.check("\n", "LF")

    def test_crlf_checkout(self):
        # A Windows working copy, or any checkout with autocrlf enabled.
        self.check("\r\n", "CRLF")

    def test_crlf_checkout_survives_a_round_trip_through_saved_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            root = build_checkout(directory, "\r\n")
            entry = discover_operator_catalog(root)[0]
            # Generation stores the digest in the spec; execution recomputes it.
            stored = entry["source_sha256"]
            self.assertEqual(stored, hash_as_execution_does(root, entry["source_file"]))

    def test_editing_the_file_still_invalidates_it(self):
        """The check must stay meaningful: a real edit has to be caught."""
        for newline, label in (("\n", "LF"), ("\r\n", "CRLF")):
            with tempfile.TemporaryDirectory() as directory:
                root = build_checkout(directory, newline)
                entry = discover_operator_catalog(root)[0]
                path = Path(root) / entry["source_file"]
                path.write_bytes(path.read_bytes() + b"# changed\n")
                self.assertNotEqual(entry["source_sha256"], hash_as_execution_does(root, entry["source_file"]),
                                    f"{label}: an edited operator was not detected")
                with self.assertRaises(ValueError) as caught:
                    with_sources([entry], root)
                self.assertIn("Stale catalog", str(caught.exception))


class CatalogWideDigestTests(unittest.TestCase):
    """Every operator in the vendored checkout must agree, not just samples."""

    def setUp(self):
        self.root = Path(__file__).resolve().parents[1] / "external" / "DataFlow"
        if not (self.root / "dataflow" / "operators").is_dir():
            self.skipTest("vendored DataFlow checkout is not present")

    def test_catalog_digests_match_the_execution_digest(self):
        catalog = discover_operator_catalog(self.root)
        self.assertGreater(len(catalog), 100)
        mismatched = [entry["name"] for entry in catalog
                      if entry["source_sha256"] != hash_as_execution_does(self.root, entry["source_file"])]
        self.assertEqual(mismatched, [],
                         "these operators would be reported as modified before execution")

    def test_frozen_runs_still_pass_the_pre_execution_check(self):
        """A pipeline generated earlier must not be invalidated by this change."""
        from dataflow_agents.execution import check_sources

        runs = Path(__file__).resolve().parents[1] / "runs"
        checked = 0
        for spec_path in sorted(runs.glob("*/pipeline-spec.json")):
            import json
            spec = json.loads(spec_path.read_text(encoding="utf-8"))
            try:
                check_sources(spec, self.root)
            except ValueError as exc:
                # Custom proposals live in the run directory and are skipped by
                # check_sources; anything else is a genuine disagreement.
                self.fail(f"{spec_path.parent.name}: {exc}")
            checked += 1
        self.assertGreater(checked, 0, "no frozen run specs were found to check")

    def test_search_returns_only_entries_that_pass_the_staleness_check(self):
        catalog = discover_operator_catalog(self.root)
        matches = search_catalog("spaces clean refine deduplicate", catalog, limit=6)
        self.assertTrue(matches)
        for entry in with_sources(matches, self.root):
            self.assertEqual(entry["source_sha256"], hash_as_execution_does(self.root, entry["source_file"]))


if __name__ == "__main__":
    unittest.main()
