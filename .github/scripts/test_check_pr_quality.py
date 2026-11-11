#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("check_pr_quality.py")
SPEC = importlib.util.spec_from_file_location("check_pr_quality", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

GOOD_BODY = """I hereby agree to the terms of the CLA available at: https://docs.databend.com/dev/policies/cla/

## Summary

`INSERT OVERWRITE` leaves orphan snapshots when the transaction aborts
midway, because the purge step runs before commit. Move the purge after
commit and add a regression test.

- fixes: #12345

## Tests

- [x] Logic Test

## Type of change

- [x] Bug Fix (non-breaking change which fixes an issue)
"""


def filled_summary(body):
    return body.replace("<<SUMMARY>>", "")


class ValidateBodyTests(unittest.TestCase):
    def test_good_body_passes(self):
        self.assertEqual(MODULE.validate_body(GOOD_BODY), [])

    def test_empty_body_fails(self):
        problems = MODULE.validate_body("<!-- template only -->\n\n")
        self.assertTrue(any("empty" in p for p in problems))

    def test_template_only_summary_fails(self):
        body = GOOD_BODY.replace(
            "`INSERT OVERWRITE` leaves orphan snapshots when the transaction aborts\n"
            "midway, because the purge step runs before commit. Move the purge after\n"
            "commit and add a regression test.\n\n- fixes: #12345",
            "<!-- Briefly describe what this PR aims to solve. -->",
        )
        problems = MODULE.validate_body(body)
        self.assertTrue(any("## Summary" in p for p in problems))
        self.assertEqual(len(problems), 1)

    def test_oversized_body_fails(self):
        body = GOOD_BODY + "\nfiller " * 2000
        problems = MODULE.validate_body(body)
        self.assertTrue(any("characters" in p for p in problems))

    def test_pasted_dump_fails(self):
        body = GOOD_BODY + "\n```\n" + "log line\n" * 200 + "```\n"
        problems = MODULE.validate_body(body)
        self.assertTrue(any("pasted blocks" in p for p in problems))

    def test_minified_line_fails(self):
        body = GOOD_BODY + "\n" + "x" * 2500 + "\n"
        problems = MODULE.validate_body(body)
        self.assertTrue(any("single line" in p for p in problems))

    def test_repetition_fails(self):
        body = GOOD_BODY + ("\nThis change holistically improves the system overall.")
        body = GOOD_BODY + ("\nThis change holistically improves the system overall." * 1)
        repeated = "\nThis change holistically improves the whole system overall."
        body = GOOD_BODY + repeated * 5
        problems = MODULE.validate_body(body)
        self.assertTrue(any("appears" in p for p in problems))

    def test_short_repetition_is_ignored(self):
        body = GOOD_BODY + "\n- [x] ok" * 10
        self.assertEqual(MODULE.validate_body(body), [])


class SummaryTextTests(unittest.TestCase):
    def test_extracts_summary_without_comments(self):
        text = MODULE.summary_text(GOOD_BODY)
        self.assertIn("INSERT OVERWRITE", text)
        self.assertNotIn("## Tests", text)

    def test_missing_summary_is_empty(self):
        self.assertEqual(MODULE.summary_text("no headings here"), "")


if __name__ == "__main__":
    unittest.main()
