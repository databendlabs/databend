#!/usr/bin/env python3
"""Extract MIN_METASRV_SEMVER and MIN_METACLI_SEMVER from each git tag."""

import os
import re
import subprocess
import sys

# Files where these variables have been defined across the repo history.
# Ordered by recency — we try each path until one works.
METASRV_PATHS = [
    "src/meta/client/src/lib.rs",
    "common/meta/grpc/src/lib.rs",
]

METACLI_PATHS = [
    "src/meta/service/src/version.rs",
]

# Patterns to match both old struct syntax and new Version::new() syntax:
#   Old: pub static VAR: Version = Version { major: 1, minor: 2, patch: 3, ... };
#   New: pub static VAR: Version = Version::new(1, 2, 3);
RE_NEW = {
    "MIN_METASRV_SEMVER": re.compile(
        r"MIN_METASRV_SEMVER.*Version::new\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)"
    ),
    "MIN_METACLI_SEMVER": re.compile(
        r"MIN_METACLI_SEMVER.*Version::new\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)"
    ),
}

RE_OLD = {
    "MIN_METASRV_SEMVER": re.compile(
        r"MIN_METASRV_SEMVER.*Version\s*\{[^}]*"
        r"major:\s*(\d+)[^}]*minor:\s*(\d+)[^}]*patch:\s*(\d+)",
        re.DOTALL,
    ),
    "MIN_METACLI_SEMVER": re.compile(
        r"MIN_METACLI_SEMVER.*Version\s*\{[^}]*"
        r"major:\s*(\d+)[^}]*minor:\s*(\d+)[^}]*patch:\s*(\d+)",
        re.DOTALL,
    ),
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "..")


def git_show(tag, path):
    """Return file content at a given tag:path, or None."""
    try:
        r = subprocess.run(
            ["git", "show", f"{tag}:{path}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode == 0:
            return r.stdout
    except subprocess.TimeoutExpired:
        pass
    return None


def extract_version(content, var_name):
    """Extract (major, minor, patch) for var_name from file content."""
    m = RE_NEW[var_name].search(content)
    if m:
        return f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    m = RE_OLD[var_name].search(content)
    if m:
        return f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    return None


def get_tags():
    """Return all version tags sorted by semver."""
    r = subprocess.run(
        ["git", "tag", "--sort=version:refname"],
        capture_output=True,
        text=True,
    )
    tags = [t for t in r.stdout.splitlines() if re.match(r"^v\d+\.\d+\.\d+", t)]
    return tags


def main():
    tags = get_tags()
    print(f"Found {len(tags)} tags", file=sys.stderr)

    out_path = os.path.join(BASE_DIR, "src/min_compatible_versions.txt")
    with open(out_path, "w") as out:
        out.write(f"{'tag':<30} {'MIN_METASRV_SEMVER':<22} {'MIN_METACLI_SEMVER':<22}\n")
        out.write("-" * 74 + "\n")

        for i, tag in enumerate(tags):
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(tags)}...", file=sys.stderr)

            metasrv = None
            for path in METASRV_PATHS:
                content = git_show(tag, path)
                if content:
                    metasrv = extract_version(content, "MIN_METASRV_SEMVER")
                    if metasrv:
                        break

            metacli = None
            for path in METACLI_PATHS:
                content = git_show(tag, path)
                if content:
                    metacli = extract_version(content, "MIN_METACLI_SEMVER")
                    if metacli:
                        break

            if metasrv or metacli:
                display_tag = tag.removeprefix("v").removesuffix("-nightly")
                out.write(
                    f"{display_tag:<30} {metasrv or '-':<22} {metacli or '-':<22}\n"
                )

    print(f"Written to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
