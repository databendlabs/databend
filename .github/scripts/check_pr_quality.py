#!/usr/bin/env python3
"""Check PR description writing quality (PR Assistant `quality` job).

Enforces the description bar from AI_POLICY.md on the PR body: the author
must describe the concrete problem in their own words, concisely. Bloated
descriptions, pasted log/stack dumps, repeated boilerplate, and a Summary
section left at its template placeholder are rejected.

Scope note: the `## AI assistance` section is validated by the separate
`ai_assistance` job (check_pr_ai_assistance.py); this script intentionally
does not duplicate it.

Reads the pull_request_target payload from GITHUB_EVENT_PATH and reports
through GITHUB_OUTPUT (`quality` = valid/invalid, `problems` = bullet list).
Exits non-zero when the description is below the bar. Stdlib only.
"""

import json
import os
import re
import sys
import uuid

MAX_BODY_CHARS = 8000
MIN_SUMMARY_CHARS = 30
MAX_CODE_BLOCK_LINES = 60
MAX_TOTAL_CODE_LINES = 120
MAX_LINE_CHARS = 2000
MIN_REPEAT_LINE_CHARS = 40
MAX_LINE_REPEATS = 4


def strip_html_comments(text):
    return re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)


def code_block_stats(text):
    """Return (longest fenced block in lines, total fenced lines)."""
    longest = 0
    total = 0
    current = 0
    in_block = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            if in_block:
                longest = max(longest, current)
                total += current
                current = 0
                in_block = False
            else:
                in_block = True
            continue
        if in_block:
            current += 1
    if in_block:  # unterminated fence still counts as a dump
        longest = max(longest, current)
        total += current
    return longest, total


def repeated_lines(text):
    counts = {}
    for raw in text.splitlines():
        line = raw.strip()
        if len(line) < MIN_REPEAT_LINE_CHARS:
            continue
        counts[line] = counts.get(line, 0) + 1
    return [line for line, n in counts.items() if n >= MAX_LINE_REPEATS]


def summary_text(body):
    """Content of the '## Summary' section with template comments removed."""
    match = re.search(r"(?ms)^##\s+Summary\s*\n(.*?)(?=^##\s|\Z)", body)
    if not match:
        return ""
    return strip_html_comments(match.group(1)).strip()


def validate_body(body):
    """Return a list of human-readable problems (empty when the body is fine)."""
    problems = []
    plain = strip_html_comments(body).strip()
    if not plain:
        problems.append(
            "the description is empty or contains only template comments - "
            "state the problem in your own words"
        )
        return problems

    if len(summary_text(body)) < MIN_SUMMARY_CHARS:
        problems.append(
            "the `## Summary` section has no real content - state the root "
            "cause or user-visible problem and why this fix is correct, in "
            "at least a sentence or two"
        )
    if len(body) > MAX_BODY_CHARS:
        problems.append(
            "the description is %d characters (limit %d) - long generated "
            "essays are not reviewable; keep only what a human needs to "
            "understand the change" % (len(body), MAX_BODY_CHARS)
        )
    longest, total = code_block_stats(body)
    if longest > MAX_CODE_BLOCK_LINES or total > MAX_TOTAL_CODE_LINES:
        problems.append(
            "large pasted blocks detected (longest %d lines, total %d lines; "
            "limits %d/%d) - move logs and stack traces to a gist or a "
            "<details> block and keep the description itself short"
            % (longest, total, MAX_CODE_BLOCK_LINES, MAX_TOTAL_CODE_LINES)
        )
    if any(len(line) > MAX_LINE_CHARS for line in body.splitlines()):
        problems.append(
            "a single line longer than %d characters was found - this looks "
            "like a pasted dump; attach it as a file instead" % MAX_LINE_CHARS
        )
    dups = repeated_lines(body)
    if dups:
        problems.append(
            "the same sentence appears %d+ times (e.g. \"%s...\") - repeated "
            "boilerplate is a sign of unfiltered generated output"
            % (MAX_LINE_REPEATS, dups[0][:60])
        )
    return problems


def write_output(name, value):
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    delimiter = "ghadelimiter_%s" % uuid.uuid4().hex
    with open(output_path, "a", encoding="utf-8") as output:
        output.write("%s<<%s\n%s\n%s\n" % (name, delimiter, value, delimiter))


def workflow_error(message):
    escaped = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print("::error::%s" % escaped)


def main():
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RuntimeError("GITHUB_EVENT_PATH is not set")
    with open(event_path, encoding="utf-8") as event_file:
        event = json.load(event_file)

    pull_request = event["pull_request"]
    if pull_request.get("draft"):
        print("PR is in draft state, skipping description quality check")
        write_output("quality", "valid")
        return 0

    problems = validate_body(pull_request.get("body") or "")
    write_output("quality", "valid" if not problems else "invalid")
    if problems:
        write_output("problems", "\n".join("- " + p for p in problems))
        workflow_error("PR description below the writing bar: " + "; ".join(problems))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
