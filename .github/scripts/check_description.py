#!/usr/bin/env python3
"""Description gate for issues and pull requests.

Validates a submission body against the repository's writing bar
(AI_POLICY.md): the author must describe the concrete problem in their own
words, concisely. Bloated bodies, pasted log/stack dumps, repeated filler,
and unfilled template placeholders are rejected.

Input:  the webhook payload at GITHUB_EVENT_PATH (issues / pull_request_target).
Output: for PRs    -> a `description-gate` commit status on the head SHA plus
                      an upserted comment when failing.
        for issues -> a label + comment, and the issue is closed while failing;
                      editing the body to meet the bar reopens it automatically.

Stdlib only. Env:
  GITHUB_TOKEN        (required)
  GITHUB_EVENT_PATH   (set by Actions)
  GITHUB_REPOSITORY   (owner/repo, set by Actions)
  GATE_CLOSE_ISSUES   ("true"/"false", default "true")
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

MAX_PR_BODY_CHARS = 8000
MAX_ISSUE_BODY_CHARS = 10000
MIN_SUMMARY_CHARS = 30
MAX_CODE_BLOCK_LINES = 60
MAX_TOTAL_CODE_LINES = 120
MAX_LINE_CHARS = 2000
MIN_REPEAT_LINE_CHARS = 40
MAX_LINE_REPEATS = 4

LABEL = "needs-better-description"
COMMENT_MARKER = "<!-- description-gate -->"
STATUS_CONTEXT = "description-gate"

TRUSTED_ISSUE_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

WRITING_RULE = (
    "Describe the change the way you would explain it to a reviewer: "
    "the concrete problem, why it matters, and how you validated it - "
    "in your own words. Do not paste model transcripts, long logs, or "
    "stack traces; link a gist or use a <details> block instead."
)


class Violation:
    def __init__(self, rule, message):
        self.rule = rule
        self.message = message

    def __str__(self):
        return "[%s] %s" % (self.rule, self.message)


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
    if in_block:  # unterminated fence: still counts as a dump
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


def check_common(body, max_chars):
    violations = []
    plain = strip_html_comments(body).strip()
    if not plain:
        violations.append(Violation(
            "empty-body",
            "The description is empty or contains only template comments. "
            "State the problem in your own words."))
        return violations
    if len(body) > max_chars:
        violations.append(Violation(
            "too-long",
            "The description is %d characters (limit %d). Long AI-generated "
            "essays are not reviewable; keep only what a human needs to "
            "understand the problem." % (len(body), max_chars)))
    longest, total = code_block_stats(body)
    if longest > MAX_CODE_BLOCK_LINES or total > MAX_TOTAL_CODE_LINES:
        violations.append(Violation(
            "pasted-dump",
            "Large pasted blocks detected (longest %d lines, total %d lines; "
            "limits %d/%d). Move logs and stack traces to a gist or a "
            "<details> block and keep the description itself short."
            % (longest, total, MAX_CODE_BLOCK_LINES, MAX_TOTAL_CODE_LINES)))
    for line in body.splitlines():
        if len(line) > MAX_LINE_CHARS:
            violations.append(Violation(
                "minified-dump",
                "A single line longer than %d characters was found. This "
                "looks like a pasted dump; attach it as a file instead."
                % MAX_LINE_CHARS))
            break
    dups = repeated_lines(body)
    if dups:
        violations.append(Violation(
            "repetition",
            "The same sentence appears %d+ times (e.g. \"%s...\"). Repetitive "
            "boilerplate is a sign of unfiltered generated output."
            % (MAX_LINE_REPEATS, dups[0][:60])))
    return violations


def section_map(body):
    """Map '## Heading' -> raw section text."""
    parts = re.split(r"(?m)^(##\s+.+)$", body)
    sections = {}
    for i in range(1, len(parts) - 1, 2):
        heading = parts[i].lstrip("#").strip().lower()
        sections[heading] = parts[i + 1]
    return sections


def check_pr(body):
    violations = check_common(body, MAX_PR_BODY_CHARS)
    sections = section_map(body)

    summary = strip_html_comments(sections.get("summary", "")).strip()
    if len(summary) < MIN_SUMMARY_CHARS:
        violations.append(Violation(
            "thin-summary",
            "The '## Summary' section has no real content. State the root "
            "cause or user-visible problem and why this fix is correct, in "
            "at least a sentence or two."))

    ai = sections.get("ai assistance")
    if ai is None:
        violations.append(Violation(
            "missing-ai-section",
            "The '## AI assistance' section was removed. It is required by "
            "AI_POLICY.md even when the answer is 'None'."))
    else:
        usage = re.search(r"(?m)^-\s*AI usage:\s*(.*)$", ai)
        usage_text = strip_html_comments(usage.group(1)).strip() if usage else ""
        if not usage_text:
            violations.append(Violation(
                "unfilled-ai-usage",
                "'AI usage' is not filled in. Describe the assistance and "
                "scope, or write 'None'."))
        human = re.search(r"(?m)^-\s*Responsible human:\s*(.*)$", ai)
        human_text = strip_html_comments(human.group(1)).strip() if human else ""
        if not re.search(r"@[A-Za-z0-9][A-Za-z0-9-]*", human_text):
            violations.append(Violation(
                "missing-responsible-human",
                "No responsible human (@github-id) named. Every PR needs an "
                "author-side owner who has read the diff and answers review "
                "questions."))
        read_line = re.search(
            r"(?m)^-\s*\[(?P<mark>[ xX])\]\s*The responsible human has read", ai)
        if not read_line or read_line.group("mark") == " ":
            violations.append(Violation(
                "read-checkbox-unticked",
                "The 'has read every line of this diff' checkbox is not "
                "ticked. Unread code does not enter review."))
    return violations


def check_issue(body):
    violations = check_common(body, MAX_ISSUE_BODY_CHARS)
    plain = strip_html_comments(body).strip()
    if plain and len(plain) < MIN_SUMMARY_CHARS:
        violations.append(Violation(
            "too-thin",
            "The report is only %d characters. Say what you did, what you "
            "expected, and what happened instead." % len(plain)))
    return violations


# ---------------------------------------------------------------- GitHub API

def api(method, path, token, payload=None):
    url = "https://api.github.com" + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        body = e.read().decode(errors="replace")[:500]
        print("API %s %s -> %s: %s" % (method, path, e.code, body),
              file=sys.stderr)
        raise


def find_gate_comment(repo, number, token):
    comments = api("GET",
                   "/repos/%s/issues/%s/comments?per_page=100" % (repo, number),
                   token) or []
    for c in comments:
        if COMMENT_MARKER in (c.get("body") or ""):
            return c
    return None


def upsert_comment(repo, number, token, body):
    existing = find_gate_comment(repo, number, token)
    if existing:
        api("PATCH", "/repos/%s/issues/comments/%s" % (repo, existing["id"]),
            token, {"body": body})
    else:
        api("POST", "/repos/%s/issues/%s/comments" % (repo, number),
            token, {"body": body})


def delete_gate_comment(repo, number, token):
    existing = find_gate_comment(repo, number, token)
    if existing:
        req = urllib.request.Request(
            "https://api.github.com/repos/%s/issues/comments/%s"
            % (repo, existing["id"]), method="DELETE")
        req.add_header("Authorization", "Bearer " + token)
        req.add_header("Accept", "application/vnd.github+json")
        urllib.request.urlopen(req).read()


def failing_comment(author, kind, violations, consequence):
    lines = [
        COMMENT_MARKER,
        "Hi @%s - this %s does not meet the repository's description bar yet."
        % (author, kind),
        "",
        WRITING_RULE,
        "",
        "Problems found:",
    ]
    lines += ["%d. %s" % (i + 1, v.message) for i, v in enumerate(violations)]
    lines += [
        "",
        "Edit the description to resolve these. " + consequence,
        "If the gate misfires on a genuinely good-faith submission, ping a maintainer.",
    ]
    return "\n".join(lines)


def ensure_label(repo, token):
    existing = api("GET", "/repos/%s/labels/%s" % (repo, LABEL), token)
    if existing is None:
        api("POST", "/repos/%s/labels" % repo, token, {
            "name": LABEL,
            "color": "e11d48",
            "description": "Description does not meet the repository bar; see description-gate comment",
        })


def handle_pr(event, repo, token):
    pr = event["pull_request"]
    if (pr.get("user") or {}).get("type") == "Bot":
        print("bot PR, skipping")
        return 0
    body = pr.get("body") or ""
    number = pr["number"]
    sha = pr["head"]["sha"]
    author = (pr.get("user") or {}).get("login", "contributor")

    violations = check_pr(body)
    if violations:
        for v in violations:
            print("FAIL %s" % v)
        api("POST", "/repos/%s/statuses/%s" % (repo, sha), token, {
            "state": "failure",
            "context": STATUS_CONTEXT,
            "description": violations[0].message[:140],
        })
        upsert_comment(repo, number, token, failing_comment(
            author, "pull request", violations,
            "The `%s` status must pass before this PR can be merged." % STATUS_CONTEXT))
        return 1

    print("description gate passed")
    api("POST", "/repos/%s/statuses/%s" % (repo, sha), token, {
        "state": "success",
        "context": STATUS_CONTEXT,
        "description": "Description meets the repository bar",
    })
    delete_gate_comment(repo, number, token)
    return 0


def handle_issue(event, repo, token, close_issues):
    issue = event["issue"]
    if "pull_request" in issue:
        return 0
    if (issue.get("user") or {}).get("type") == "Bot":
        return 0
    if issue.get("author_association") in TRUSTED_ISSUE_ASSOCIATIONS:
        print("trusted author (%s), skipping"
              % issue.get("author_association"))
        return 0

    body = issue.get("body") or ""
    number = issue["number"]
    author = (issue.get("user") or {}).get("login", "reporter")
    violations = check_issue(body)

    if violations:
        for v in violations:
            print("FAIL %s" % v)
        ensure_label(repo, token)
        api("POST", "/repos/%s/issues/%s/labels" % (repo, number),
            token, {"labels": [LABEL]})
        consequence = (
            "This issue has been closed while the description is below the "
            "bar; it reopens automatically once the description is fixed."
            if close_issues else
            "Maintainers may close the issue if it stays below the bar.")
        upsert_comment(repo, number, token,
                       failing_comment(author, "issue", violations, consequence))
        if close_issues and issue.get("state") == "open":
            api("PATCH", "/repos/%s/issues/%s" % (repo, number), token,
                {"state": "closed", "state_reason": "not_planned"})
        return 1

    print("description gate passed")
    was_flagged = find_gate_comment(repo, number, token) is not None
    if was_flagged:
        delete_gate_comment(repo, number, token)
        api("DELETE", "/repos/%s/issues/%s/labels/%s" % (repo, number, LABEL),
            token)
        if issue.get("state") == "closed":
            api("PATCH", "/repos/%s/issues/%s" % (repo, number), token,
                {"state": "open"})
    return 0


def main():
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not (token and repo and event_path):
        print("missing GITHUB_TOKEN / GITHUB_REPOSITORY / GITHUB_EVENT_PATH",
              file=sys.stderr)
        return 2
    close_issues = os.environ.get("GATE_CLOSE_ISSUES", "true").lower() == "true"
    with open(event_path) as f:
        event = json.load(f)

    if "pull_request" in event:
        return handle_pr(event, repo, token)
    if "issue" in event:
        return handle_issue(event, repo, token, close_issues)
    print("unsupported event, skipping")
    return 0


if __name__ == "__main__":
    sys.exit(main())
