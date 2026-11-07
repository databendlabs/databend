"""Offline runner/dependency tests; subprocesses and downloads are mocked, not TLC."""

import contextlib
import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import noxfile

from tla import run

SUCCESS = (
    "Model checking completed. No error has been found.\n"
    "10 states generated, 8 distinct states found, 0 states left on queue.\n"
)
TRACE = (
    "Error: Invariant Safe is violated.\n"
    "Error: The behavior up to this point is:\nState 1: <Initial predicate>\n"
)
SPEC = {
    "module": "Model",
    "constants": {"N": 1, "Flag": True, "Mode": "fast"},
    "invariants": ["Safe"],
    "matrix": {"N": [1, 2], "Mode": ["fast", "slow"]},
    "cases": {
        "no_flag": {"constants": {"Flag": False}, "violation": "Safe"},
        "reachable": {"witness": "NeverDone"},
    },
}


class RunnerTests(unittest.TestCase):
    def test_verdict_rejects_incomplete_or_wrong_failure(self):
        for code, text, expected, passed in [
            (0, SUCCESS, None, True),
            (0, "Starting...", None, False),
            (0, SUCCESS.replace("0 states left", "10 states left"), None, False),
            (-9, SUCCESS, None, False),
            (12, TRACE, "Safe", True),
            (0, TRACE, "Safe", False),
            (1, TRACE, "Safe", False),
            (12, TRACE, "Other", False),
            (12, TRACE, None, False),
            (12, TRACE.replace("State 1:", "No trace:"), "Safe", False),
            (12, TRACE + "Error: Unexpected exception\n", "Safe", False),
        ]:
            with self.subTest(code=code, text=text, expected=expected):
                self.assertEqual(run.verdict(code, text, expected), passed)

    def test_matrix_mutation_and_witness(self):
        generated = run.cases(SPEC)
        self.assertEqual(len(generated), 6)
        self.assertIn("N=2,Mode=slow", generated)
        constants, invariants, expected = generated["reachable"]
        self.assertEqual((invariants, expected), (["Safe", "NeverDone"], "NeverDone"))
        self.assertFalse(generated["no_flag"][0]["Flag"])
        text = run.config_text(constants, invariants)
        for line in [
            "Flag = TRUE",
            'Mode = "fast"',
            "INVARIANTS Safe NeverDone",
            "CHECK_DEADLOCK FALSE",
        ]:
            self.assertIn(line, text)

    def test_invalid_specs(self):
        for spec in [
            {k: v for k, v in SPEC.items() if k != "module"},
            {**SPEC, "invariants": []},
            {**SPEC, "matrix": {"Unknown": [1]}},
            {**SPEC, "matrix": {"N": []}},
            {**SPEC, "matrix": {"Mode": ["a/b"]}},
            {**SPEC, "cases": {"bad name": {}}},
            {**SPEC, "cases": {"x": {"typo": 1}}},
            {**SPEC, "cases": {"x": {"constants": {"Unknown": 1}}}},
            {**SPEC, "cases": {"x": {"witness": "W", "invariants": []}}},
            {**SPEC, "cases": {"x": {"violation": "Other"}}},
        ]:
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                run.cases(spec)
        for value in [1.5, None, 'quote"d', [1]]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                run.literal(value)

    def test_list_needs_no_java_or_jar(self):
        with tempfile.TemporaryDirectory() as directory:
            model = Path(directory)
            (model / "model.json").write_text(json.dumps(SPEC))
            output = io.StringIO()
            with (
                mock.patch.object(run.subprocess, "run") as execute,
                mock.patch.object(run, "verify_jar") as verify,
                contextlib.redirect_stdout(output),
            ):
                self.assertEqual(run.main([str(model), "--list"]), 0)
            execute.assert_not_called()
            verify.assert_not_called()
            self.assertEqual(len(output.getvalue().splitlines()), 6)

    def test_timeout_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(java="unused", timeout=1, workers=1)
            with (
                mock.patch.object(
                    run.subprocess,
                    "run",
                    side_effect=subprocess.TimeoutExpired("tlc", 1),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertFalse(
                    run.run_case(args, Path("x.jar"), Path(directory), "M", None)
                )

    def test_selected_models_have_separate_cache_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            jar = root / "tools.jar"
            jar.write_bytes(b"verified in dependency tests")
            cache = root / "cache"
            for name, size in [("alpha", 2), ("beta", 5)]:
                model = root / name
                model.mkdir()
                source = f"model snapshot for {name}"
                (model / "Model.tla").write_text(source)
                spec = {**SPEC, "constants": {**SPEC["constants"], "N": size}}
                (model / "model.json").write_text(json.dumps(spec))
                with (
                    mock.patch.object(run, "CACHE_DIR", cache),
                    mock.patch.object(run, "verify_jar"),
                    mock.patch.object(run.subprocess, "run"),
                    mock.patch.object(run, "run_case", return_value=True) as check,
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(run.main([str(model), "--jar", str(jar)]), 0)
                self.assertEqual(check.call_count, 6)
                snapshots = list(cache.glob(f"test-output/tla/{name}/run-*/model.json"))
                self.assertEqual(len(snapshots), 1)
                self.assertEqual(json.loads(snapshots[0].read_text()), spec)
                for snapshot in snapshots[0].parent.glob("*/Model.tla"):
                    self.assertEqual(snapshot.read_text(), source)

    def test_no_download_for_session_help_listing_or_missing_model(self):
        for args in [[], ["--help"], ["some_model", "--list"]]:
            with self.subTest(args=args):
                session = SimpleNamespace(posargs=args, run=mock.Mock())
                with mock.patch.object(noxfile, "prepare_tlc") as prepare:
                    noxfile.tla(session)
                prepare.assert_not_called()
                self.assertEqual(session.run.call_count, 1)
                self.assertEqual(session.run.call_args.args[2:], tuple(args))

    def test_session_prepares_shared_tlc_and_forwards_model(self):
        session = SimpleNamespace(
            posargs=["other_model", "--case", "example"], run=mock.Mock()
        )
        jar = Path("verified.jar")
        with mock.patch.object(noxfile, "prepare_tlc", return_value=jar) as prepare:
            noxfile.tla(session)
        prepare.assert_called_once_with()
        self.assertEqual(
            session.run.call_args.args[2:],
            ("--jar", str(jar), "other_model", "--case", "example"),
        )


class DependencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.cache = Path(self.directory.name)
        self.payload = b"verified TLC fixture (not executable)"
        self.patch_cache = mock.patch.object(noxfile, "CACHE_DIR", self.cache)
        self.patch_hash = mock.patch.object(
            run, "TLC_SHA256", hashlib.sha256(self.payload).hexdigest()
        )
        self.patch_cache.start()
        self.patch_hash.start()
        self.addCleanup(self.patch_cache.stop)
        self.addCleanup(self.patch_hash.stop)

    def test_download_and_cached_reuse(self):
        with mock.patch.object(
            noxfile.urllib.request, "urlopen", return_value=io.BytesIO(self.payload)
        ) as download:
            jar = noxfile.prepare_tlc()
            self.assertEqual(jar, run.cached_jar(self.cache))
            self.assertEqual(jar.read_bytes(), self.payload)
            self.assertEqual(noxfile.prepare_tlc(), jar)
        download.assert_called_once()
        self.assertEqual(list(jar.parent.iterdir()), [jar])

    def test_wrong_download_not_published(self):
        with (
            mock.patch.object(
                noxfile.urllib.request,
                "urlopen",
                return_value=io.BytesIO(b"wrong artifact"),
            ) as download,
            self.assertRaisesRegex(ValueError, "SHA-256 mismatch"),
        ):
            noxfile.prepare_tlc()
        download.assert_called_once()
        jar = run.cached_jar(self.cache)
        self.assertFalse(jar.exists())
        self.assertEqual(list(jar.parent.iterdir()), [])

    def test_wrong_cache_rejected_without_network(self):
        jar = run.cached_jar(self.cache)
        jar.parent.mkdir(parents=True)
        jar.write_bytes(b"corrupt cache")
        with (
            mock.patch.object(noxfile.urllib.request, "urlopen") as download,
            self.assertRaisesRegex(ValueError, "SHA-256 mismatch"),
        ):
            noxfile.prepare_tlc()
        download.assert_not_called()
        self.assertEqual(jar.read_bytes(), b"corrupt cache")

    def test_failed_download_cleans_staging_directory(self):
        with (
            mock.patch.object(
                noxfile, "download_file", side_effect=OSError("interrupted download")
            ),
            self.assertRaisesRegex(OSError, "interrupted download"),
        ):
            noxfile.prepare_tlc()
        jar = run.cached_jar(self.cache)
        self.assertFalse(jar.exists())
        self.assertEqual(list(jar.parent.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
