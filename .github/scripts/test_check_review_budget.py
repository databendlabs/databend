#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("check_review_budget.py")
SPEC = importlib.util.spec_from_file_location("check_review_budget", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def comments(*bodies):
    return [{"body": body} for body in bodies]


class TriggerTests(unittest.TestCase):
    def test_codex_review(self):
        self.assertTrue(MODULE.is_review_trigger("@codex review"))
        self.assertTrue(MODULE.is_review_trigger("  @codex review\n"))

    def test_slash_review(self):
        self.assertTrue(MODULE.is_review_trigger("/review"))

    def test_case_insensitive(self):
        self.assertTrue(MODULE.is_review_trigger("@Codex REVIEW"))

    def test_regular_comment_is_not_a_trigger(self):
        self.assertFalse(MODULE.is_review_trigger(
            "Please run @codex review again after the fix"))
        self.assertFalse(MODULE.is_review_trigger("@codex review for this file"))

    def test_empty_body(self):
        self.assertFalse(MODULE.is_review_trigger(""))
        self.assertFalse(MODULE.is_review_trigger(None))


class CountTests(unittest.TestCase):
    def test_counts_only_triggers(self):
        items = comments(
            "@codex review", "looks good", "@codex review", "/review",
            "CI passed", "@codex review",
        )
        self.assertEqual(MODULE.count_rounds(items), 4)

    def test_bot_receipts_not_counted(self):
        items = comments(
            "@codex review",
            "Codex Review: Didn't find any major issues. Keep it up!",
            "@codex review",
        )
        self.assertEqual(MODULE.count_rounds(items), 2)

    def test_no_comments(self):
        self.assertEqual(MODULE.count_rounds([]), 0)


class LabelTests(unittest.TestCase):
    def test_override_label_detected(self):
        issue = {"labels": [{"name": "review-budget-override"}]}
        self.assertTrue(MODULE.has_label(issue, MODULE.OVERRIDE_LABEL))

    def test_missing_override(self):
        issue = {"labels": [{"name": "pr-build"}]}
        self.assertFalse(MODULE.has_label(issue, MODULE.OVERRIDE_LABEL))


class CommentTests(unittest.TestCase):
    def test_warning_is_short_and_marked(self):
        body = MODULE.warning_comment(5, 2)
        self.assertIn(MODULE.COMMENT_MARKER, body)
        self.assertLess(len(body), 800)


if __name__ == "__main__":
    unittest.main()
