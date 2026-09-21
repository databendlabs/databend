#!/usr/bin/env python3

import html
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Callable

AI_DECLARATION_ENFORCEMENT_START = datetime.fromisoformat("2026-07-29T14:33:06+00:00")
REQUIRED_ATTESTATION = (
    "The responsible human has read every line of this diff and can explain each change"
)
VOID_ELEMENTS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}


@dataclass
class Element:
    tag: str
    attrs: dict[str, str | None] = field(default_factory=dict)
    children: list["Element | str"] = field(default_factory=list)


class HtmlTreeParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Element("root")
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = Element(tag.lower(), {key.lower(): value for key, value in attrs})
        self.stack[-1].children.append(node)
        if node.tag not in VOID_ELEMENTS:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = Element(tag.lower(), {key.lower(): value for key, value in attrs})
        self.stack[-1].children.append(node)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                self.stack = self.stack[:index]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


@dataclass
class ValidationResult:
    problems: list[str] = field(default_factory=list)
    username: str | None = None
    migration: str | None = None

    @property
    def valid(self) -> bool:
        return not self.problems


def normalize_text(value: str) -> str:
    return " ".join(html.unescape(value).split())


def element_text(node: Element) -> str:
    parts: list[str] = []
    for child in node.children:
        parts.append(child if isinstance(child, str) else element_text(child))
    return normalize_text("".join(parts))


def direct_children(node: Element, tag: str) -> list[Element]:
    return [
        child
        for child in node.children
        if isinstance(child, Element) and child.tag == tag
    ]


def descendants(node: Element, tag: str) -> list[Element]:
    matches: list[Element] = []
    for child in node.children:
        if not isinstance(child, Element):
            continue
        if child.tag == tag:
            matches.append(child)
        matches.extend(descendants(child, tag))
    return matches


def next_top_level_element(root: Element, start_index: int) -> Element | None:
    for child in root.children[start_index:]:
        if isinstance(child, str):
            if child.strip():
                return None
            continue
        return child
    return None


def parse_rendered_html(rendered_html: str) -> Element:
    parser = HtmlTreeParser()
    parser.feed(rendered_html)
    parser.close()
    return parser.root


def validate_rendered_declaration(rendered_html: str) -> ValidationResult:
    root = parse_rendered_html(rendered_html)
    headings = [
        (index, child)
        for index, child in enumerate(root.children)
        if isinstance(child, Element)
        and child.tag == "h2"
        and element_text(child).casefold() == "ai assistance"
    ]

    if len(headings) != 1:
        message = (
            "the visible `## AI assistance` section is missing"
            if not headings
            else "the PR must contain exactly one visible `## AI assistance` section"
        )
        return ValidationResult(problems=[message])

    heading_index, _ = headings[0]
    declaration_list = next_top_level_element(root, heading_index + 1)
    if declaration_list is None or declaration_list.tag != "ul":
        return ValidationResult(
            problems=[
                "the `## AI assistance` heading must be followed by the required "
                "three-item list"
            ]
        )

    items = direct_children(declaration_list, "li")
    if len(items) != 3:
        return ValidationResult(
            problems=["the `## AI assistance` list must contain exactly three items"]
        )

    problems: list[str] = []
    usage = re.fullmatch(r"AI usage:\s*(.+)", element_text(items[0]), re.IGNORECASE)
    if not usage:
        problems.append("`AI usage:` is not filled in (write `None` if no AI was used)")

    human = re.fullmatch(
        r"Responsible human:\s*@([A-Za-z0-9-]+)",
        element_text(items[1]),
        re.IGNORECASE,
    )
    if not human:
        problems.append(
            "`Responsible human:` must name a real GitHub user, e.g. `@octocat`"
        )

    checkboxes = [
        checkbox
        for checkbox in descendants(items[2], "input")
        if (checkbox.attrs.get("type") or "").casefold() == "checkbox"
    ]
    checked = len(checkboxes) == 1 and "checked" in checkboxes[0].attrs
    if element_text(items[2]) != REQUIRED_ATTESTATION or not checked:
        problems.append(
            f'the checkbox "{REQUIRED_ATTESTATION}" is not checked exactly as written'
        )

    return ValidationResult(
        problems=problems,
        username=human.group(1) if human else None,
    )


def parse_github_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def valid_github_username(username: str) -> bool:
    return (
        len(username) <= 39
        and not username.startswith("-")
        and not username.endswith("-")
        and "--" not in username
        and username.casefold() != "your-github-id"
    )


def validate_pull_request(
    event: dict,
    render_markdown: Callable[[str, str | None], str],
    get_user: Callable[[str], dict | None],
) -> ValidationResult:
    pull_request = event["pull_request"]
    created_at = parse_github_time(pull_request.get("created_at"))
    if created_at and created_at < AI_DECLARATION_ENFORCEMENT_START:
        return ValidationResult(migration="legacy-pr")

    repository = event.get("repository", {})
    context = repository.get("full_name")
    rendered_html = render_markdown(pull_request.get("body") or "", context)
    result = validate_rendered_declaration(rendered_html)
    username = result.username
    if not username:
        return result

    if not valid_github_username(username):
        result.problems.append(
            "`Responsible human:` must name a real GitHub user, e.g. `@octocat`"
        )
        return result

    author = pull_request["user"]
    author_login = author["login"]
    author_is_bot = author.get("type") == "Bot" or author_login.casefold().endswith(
        "[bot]"
    )
    if not author_is_bot and username.casefold() != author_login.casefold():
        result.problems.append(
            f"Responsible human must match the PR author `@{author_login}`"
        )

    account = get_user(username)
    if account is None:
        result.problems.append(f"Responsible human `@{username}` does not exist")
    elif account.get("type") != "User":
        result.problems.append("`Responsible human:` must refer to a human account")

    return result


def github_request(method: str, path: str, payload: dict | None = None):
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "databend-pr-assistant",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        f"https://api.github.com{path}",
        data=data,
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        content = response.read().decode()
        content_type = response.headers.get_content_type()
        if content_type == "application/json":
            return json.loads(content) if content else None
        return content


def render_markdown(text: str, context: str | None) -> str:
    payload = {"text": text, "mode": "gfm"}
    if context:
        payload["context"] = context
    return github_request("POST", "/markdown", payload)


def get_user(username: str) -> dict | None:
    try:
        return github_request("GET", f"/users/{urllib.parse.quote(username, safe='')}")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def write_output(name: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    delimiter = f"ghadelimiter_{uuid.uuid4().hex}"
    with open(output_path, "a", encoding="utf-8") as output:
        output.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")


def workflow_error(message: str) -> None:
    escaped = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error::{escaped}")


def main() -> int:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RuntimeError("GITHUB_EVENT_PATH is not set")
    with open(event_path, encoding="utf-8") as event_file:
        event = json.load(event_file)

    result = validate_pull_request(event, render_markdown, get_user)
    write_output("ai", "valid" if result.valid else "invalid")
    if result.migration:
        write_output("migration", result.migration)
    if result.problems:
        problem_list = "\n".join(f"- {problem}" for problem in result.problems)
        write_output("problems", problem_list)
        message = f"AI assistance section incomplete: {'; '.join(result.problems)}"
        workflow_error(message)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
