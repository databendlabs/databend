#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("check_pr_ai_assistance.py")
SPEC = importlib.util.spec_from_file_location("check_pr_ai_assistance", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

ATTESTATION = MODULE.REQUIRED_ATTESTATION
ENFORCEMENT_START = "2026-07-29T14:33:06Z"


def rendered_declaration(
    usage="None",
    human="bohutang",
    checked=True,
    attestation=ATTESTATION,
):
    checked_attribute = ' checked=""' if checked else ""
    return f'''<h2 dir="auto">AI assistance</h2>
<ul class="contains-task-list">
<li>AI usage: {usage}</li>
<li>Responsible human: <a class="user-mention" href="https://github.com/{human}">@{human}</a></li>
<li class="task-list-item"><input type="checkbox" disabled=""{checked_attribute}> {attestation}</li>
</ul>'''


def event(
    body="raw PR body",
    created_at=ENFORCEMENT_START,
    login="bohutang",
    account_type="User",
):
    return {
        "repository": {"full_name": "databendlabs/databend"},
        "pull_request": {
            "body": body,
            "created_at": created_at,
            "user": {"login": login, "type": account_type},
        },
    }


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        self.accounts = {
            "bohutang": {"type": "User"},
            "octocat": {"type": "User"},
            "dependabot": {"type": "Bot"},
        }
        self.rendered_html = rendered_declaration()
        self.render_calls = []

    def render(self, text, context):
        self.render_calls.append((text, context))
        return self.rendered_html

    def get_user(self, username):
        return self.accounts.get(username.casefold())

    def validate(self, current_event=None):
        return MODULE.validate_pull_request(
            current_event or event(), self.render, self.get_user
        )

    def test_renders_exact_body_with_github_gfm_context(self):
        current = event(body="## AI assistance\nraw source")
        result = self.validate(current)
        self.assertTrue(result.valid, result.problems)
        self.assertEqual(
            self.render_calls,
            [(current["pull_request"]["body"], "databendlabs/databend")],
        )

    def test_accepts_complete_top_level_declaration(self):
        result = MODULE.validate_rendered_declaration(rendered_declaration())
        self.assertEqual(result.problems, [])
        self.assertEqual(result.username, "bohutang")

    def test_rejects_content_github_renders_as_literal_code(self):
        hidden_renderings = [
            # Fenced Markdown.
            f"<div class='highlight'><pre>## AI assistance\n- [x] {ATTESTATION}</pre></div>",
            # GFM raw HTML block types, including the reported textarea case.
            f"&lt;textarea&gt;\n## AI assistance\n- [x] {ATTESTATION}\n&lt;/textarea&gt;",
            f"<script type='text/plain'>## AI assistance\n- [x] {ATTESTATION}</script>",
            f"<style>## AI assistance\n- [x] {ATTESTATION}</style>",
            f"<xmp>## AI assistance\n- [x] {ATTESTATION}</xmp>",
            f"<pre>## AI assistance\n- [x] {ATTESTATION}</pre>",
        ]
        for rendered_html in hidden_renderings:
            with self.subTest(rendered_html=rendered_html[:30]):
                result = MODULE.validate_rendered_declaration(rendered_html)
                self.assertFalse(result.valid)

    def test_rejects_declaration_nested_in_other_rendered_elements(self):
        nested = [
            f"<details><summary>Example</summary>{rendered_declaration()}</details>",
            f"<blockquote>{rendered_declaration()}</blockquote>",
            f"<div>{rendered_declaration()}</div>",
        ]
        for rendered_html in nested:
            with self.subTest(rendered_html=rendered_html[:20]):
                result = MODULE.validate_rendered_declaration(rendered_html)
                self.assertFalse(result.valid)

    def test_requires_exactly_one_top_level_section(self):
        self.assertFalse(MODULE.validate_rendered_declaration("<p>none</p>").valid)
        self.assertFalse(
            MODULE.validate_rendered_declaration(
                rendered_declaration() + rendered_declaration()
            ).valid
        )

    def test_requires_immediate_three_item_list(self):
        self.assertFalse(
            MODULE.validate_rendered_declaration(
                "<h2>AI assistance</h2><p>intervening content</p>"
                + rendered_declaration().split("</h2>", 1)[1]
            ).valid
        )
        self.assertFalse(
            MODULE.validate_rendered_declaration(
                rendered_declaration().replace("</ul>", "<li>extra</li></ul>")
            ).valid
        )

    def test_requires_nonempty_usage(self):
        self.rendered_html = rendered_declaration(usage="")
        self.assertFalse(self.validate().valid)

    def test_requires_exact_checked_attestation(self):
        cases = [
            rendered_declaration(checked=False),
            rendered_declaration(attestation=ATTESTATION + " — not this diff"),
            rendered_declaration(attestation="The responsible human has read every line"),
            rendered_declaration().replace('type="checkbox"', 'type="text"'),
            rendered_declaration().replace('<input type="checkbox"', '<input type="checkbox"><input type="checkbox"'),
        ]
        for rendered_html in cases:
            with self.subTest(rendered_html=rendered_html[-80:]):
                result = MODULE.validate_rendered_declaration(rendered_html)
                self.assertFalse(result.valid)

    def test_binds_human_pr_to_author(self):
        self.rendered_html = rendered_declaration(human="octocat")
        self.assertFalse(self.validate().valid)

        self.rendered_html = rendered_declaration(human="BohuTang")
        self.assertTrue(self.validate().valid)

    def test_bot_pr_may_name_human_but_not_bot(self):
        bot_event = event(login="agent[bot]", account_type="Bot")
        self.assertTrue(self.validate(bot_event).valid)

        self.rendered_html = rendered_declaration(human="dependabot")
        self.assertFalse(self.validate(bot_event).valid)

    def test_rejects_missing_and_placeholder_accounts(self):
        self.rendered_html = rendered_declaration(human="nobody-here")
        self.assertFalse(self.validate().valid)

        self.rendered_html = rendered_declaration(human="your-github-id")
        self.assertFalse(self.validate().valid)

    def test_cutoff_does_not_retroactively_block_legacy_prs(self):
        legacy = event(body="missing", created_at="2026-07-29T14:33:05Z")
        result = self.validate(legacy)
        self.assertTrue(result.valid)
        self.assertEqual(result.migration, "legacy-pr")
        self.assertEqual(self.render_calls, [])

        self.rendered_html = "<p>missing</p>"
        current = event(body="missing", created_at=ENFORCEMENT_START)
        self.assertFalse(self.validate(current).valid)


if __name__ == "__main__":
    unittest.main()
