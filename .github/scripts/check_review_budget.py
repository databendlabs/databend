#!/usr/bin/env python3
"""Warn when a PR exceeds the AI review round budget (warning only).

Triggered by `issue_comment` on pull requests. Counts review-trigger
comments such as `@codex review`; after MAX_REVIEW_ROUNDS it posts one
upserted comment and adds a label suggesting human escalation.

This check never fails a job: it puts a speed bump on the
review -> fix -> more tests -> bigger diff -> re-review loop. Maintainers
can silence it with the `review-budget-override` label.

Env:
  GITHUB_TOKEN / GITHUB_EVENT_PATH / GITHUB_REPOSITORY (set by Actions)
  GITHUB_EVENT_NAME   (issue_comment)
  MAX_REVIEW_ROUNDS   (default 2)
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

LABEL = "review-budget-exceeded"
OVERRIDE_LABEL = "review-budget-override"
COMMENT_MARKER = "<!-- review-budget -->"
TRIGGERS = (
    re.compile(r"^\s*@codex\s+review\s*$", re.IGNORECASE),
    re.compile(r"^\s*/review\s*$", re.IGNORECASE),
)


def is_review_trigger(body):
    if not body:
        return False
    return any(pattern.match(body) for pattern in TRIGGERS)


def count_rounds(comments):
    return sum(
        1 for comment in comments if is_review_trigger(comment.get("body") or "")
    )


def api(method, path, token, payload=None, tolerate=(404, 410, 422)):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        "https://api.github.com" + path, data=data, method=method
    )
    request.add_header("Authorization", "Bearer " + token)
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        if error.code in tolerate:
            return None
        print(
            "API %s %s -> %s" % (method, path, error.code), file=sys.stderr
        )
        raise


def has_label(issue, name):
    return any(
        (label.get("name") or "").casefold() == name.casefold()
        for label in issue.get("labels") or []
    )


def warning_comment(rounds, budget):
    return "\n".join([
        COMMENT_MARKER,
        "This PR has had %d automated review requests (budget: %d)." % (rounds, budget),
        "",
        "Repeated full re-reviews amplify generated churn: each round tends to add "
        "branches and tests, which produces more findings on the next round. "
        "Please escalate to a human reviewer instead of re-running the bot on the whole PR.",
        "",
        "- Re-review only the diff since the last round, at most twice per PR.",
        "- A finding without a reproducible query or config is a hypothesis, not a blocker.",
        "- Fixing a finding may mean deleting code or declining it with a reason - not "
        "necessarily a new test.",
        "",
        "Maintainers can add the `%s` label to silence this warning." % OVERRIDE_LABEL,
    ])


def upsert_comment(repo, number, token, body):
    comments = api(
        "GET", "/repos/%s/issues/%s/comments?per_page=100" % (repo, number), token
    ) or []
    existing = next(
        (c for c in comments if COMMENT_MARKER in (c.get("body") or "")), None
    )
    if existing:
        api("PATCH", "/repos/%s/issues/comments/%s" % (repo, existing["id"]),
            token, {"body": body})
    else:
        api("POST", "/repos/%s/issues/%s/comments" % (repo, number),
            token, {"body": body})


def main():
    if os.environ.get("GITHUB_EVENT_NAME") != "issue_comment":
        return 0
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not (token and repo and event_path):
        print("missing GITHUB_TOKEN / GITHUB_REPOSITORY / GITHUB_EVENT_PATH",
              file=sys.stderr)
        return 0
    try:
        budget = int(os.environ.get("MAX_REVIEW_ROUNDS", "2"))
    except ValueError:
        budget = 2

    with open(event_path, encoding="utf-8") as event_file:
        event = json.load(event_file)
    issue = event.get("issue") or {}
    if "pull_request" not in issue:
        return 0
    if not is_review_trigger((event.get("comment") or {}).get("body")):
        return 0
    if has_label(issue, OVERRIDE_LABEL):
        print("override label present, skipping")
        return 0

    number = issue["number"]
    comments = api(
        "GET", "/repos/%s/issues/%s/comments?per_page=100" % (repo, number), token
    ) or []
    rounds = count_rounds(comments)
    print("automated review requests on PR #%s: %d (budget %d)"
          % (number, rounds, budget))
    if rounds <= budget:
        return 0

    api("POST", "/repos/%s/labels" % repo, token, {
        "name": LABEL,
        "color": "e11d48",
        "description": "Too many automated review rounds; escalate to a human reviewer",
    })
    api("POST", "/repos/%s/issues/%s/labels" % (repo, number),
        token, {"labels": [LABEL]})
    upsert_comment(repo, number, token, warning_comment(rounds, budget))
    return 0


if __name__ == "__main__":
    sys.exit(main())
