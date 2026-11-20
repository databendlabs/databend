#!/usr/bin/env python3
"""Quality gate for new/edited issues (issue-gate workflow).

Applies the description bar from AI_POLICY.md to issues opened by
non-members: bloated reports, pasted log/stack dumps, repeated boilerplate,
and near-empty bodies are rejected. A failing issue is labelled and closed;
editing the body to meet the bar removes the label and reopens the issue
automatically. Maintainers (OWNER/MEMBER/COLLABORATOR) are exempt.

Reads the `issues` webhook payload from GITHUB_EVENT_PATH. Stdlib only. Env:
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

MAX_BODY_CHARS = 10000
MIN_BODY_CHARS = 30
MAX_CODE_BLOCK_LINES = 60
MAX_TOTAL_CODE_LINES = 120
MAX_LINE_CHARS = 2000
MIN_REPEAT_LINE_CHARS = 40
MAX_LINE_REPEATS = 4

LABEL = "needs-better-description"
COMMENT_MARKER = "<!-- issue-quality-gate -->"
TRUSTED_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

WRITING_RULE = (
    "Describe the problem the way you would explain it to a maintainer: "
    "what you did, what you expected, and what happened instead - in your "
    "own words. Do not paste model transcripts, long logs, or stack traces; "
    "attach files, link a gist, or use a <details> block instead."
)


def strip_html_comments(text):
    return re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)


def code_block_stats(text):
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
    if in_block:
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


def validate_body(body):
    """Return a list of human-readable problems (empty when the body is fine)."""
    problems = []
    plain = strip_html_comments(body).strip()
    if not plain:
        problems.append(
            "the report is empty or contains only template comments - "
            "state the problem in your own words"
        )
        return problems
    if len(plain) < MIN_BODY_CHARS:
        problems.append(
            "the report is only %d characters - say what you did, what you "
            "expected, and what happened instead" % len(plain)
        )
    if len(body) > MAX_BODY_CHARS:
        problems.append(
            "the report is %d characters (limit %d) - long generated essays "
            "are not actionable; keep only what a human needs to understand "
            "the problem" % (len(body), MAX_BODY_CHARS)
        )
    longest, total = code_block_stats(body)
    if longest > MAX_CODE_BLOCK_LINES or total > MAX_TOTAL_CODE_LINES:
        problems.append(
            "large pasted blocks detected (longest %d lines, total %d lines; "
            "limits %d/%d) - move logs and stack traces to an attachment, a "
            "gist, or a <details> block"
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


# ---------------------------------------------------------------- GitHub API

def api(method, path, token, payload=None, tolerate=(404, 410, 422)):
    url = "https://api.github.com" + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if e.code in tolerate:
            return None
        body = e.read().decode(errors="replace")[:500]
        print("API %s %s -> %s: %s" % (method, path, e.code, body), file=sys.stderr)
        raise


def find_gate_comment(repo, number, token):
    comments = api(
        "GET", "/repos/%s/issues/%s/comments?per_page=100" % (repo, number), token
    ) or []
    for comment in comments:
        if COMMENT_MARKER in (comment.get("body") or ""):
            return comment
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
        api("DELETE", "/repos/%s/issues/comments/%s" % (repo, existing["id"]), token)


def ensure_label(repo, token):
    if api("GET", "/repos/%s/labels/%s" % (repo, LABEL), token) is None:
        api("POST", "/repos/%s/labels" % repo, token, {
            "name": LABEL,
            "color": "e11d48",
            "description": "Description below the repository bar; see issue-quality-gate comment",
        })


def failing_comment(author, problems, close_issues):
    consequence = (
        "This issue has been closed while the report is below the bar; it "
        "reopens automatically once the description is fixed."
        if close_issues else
        "Maintainers may close the issue if it stays below the bar."
    )
    lines = [
        COMMENT_MARKER,
        "Hi @%s - this report does not meet the repository's description bar "
        "yet." % author,
        "",
        WRITING_RULE,
        "",
        "Problems found:",
    ]
    lines += ["%d. %s" % (i + 1, p) for i, p in enumerate(problems)]
    lines += [
        "",
        "Edit the description to resolve these. " + consequence,
        "If the gate misfires on a good-faith report, ping a maintainer.",
    ]
    return "\n".join(lines)


def main():
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not (token and repo and event_path):
        print("missing GITHUB_TOKEN / GITHUB_REPOSITORY / GITHUB_EVENT_PATH",
              file=sys.stderr)
        return 2
    close_issues = os.environ.get("GATE_CLOSE_ISSUES", "true").lower() == "true"
    with open(event_path, encoding="utf-8") as event_file:
        event = json.load(event_file)

    issue = event["issue"]
    if "pull_request" in issue:
        return 0
    if (issue.get("user") or {}).get("type") == "Bot":
        print("bot author, skipping")
        return 0
    if issue.get("author_association") in TRUSTED_ASSOCIATIONS:
        print("trusted author (%s), skipping" % issue.get("author_association"))
        return 0

    number = issue["number"]
    author = (issue.get("user") or {}).get("login", "reporter")
    problems = validate_body(issue.get("body") or "")

    if problems:
        for problem in problems:
            print("FAIL %s" % problem)
        ensure_label(repo, token)
        api("POST", "/repos/%s/issues/%s/labels" % (repo, number),
            token, {"labels": [LABEL]})
        upsert_comment(repo, number, token,
                       failing_comment(author, problems, close_issues))
        if close_issues and issue.get("state") == "open":
            api("PATCH", "/repos/%s/issues/%s" % (repo, number), token,
                {"state": "closed", "state_reason": "not_planned"})
        return 1

    print("issue quality gate passed")
    if find_gate_comment(repo, number, token):
        delete_gate_comment(repo, number, token)
        api("DELETE", "/repos/%s/issues/%s/labels/%s" % (repo, number, LABEL),
            token)
        if issue.get("state") == "closed":
            api("PATCH", "/repos/%s/issues/%s" % (repo, number), token,
                {"state": "open"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
