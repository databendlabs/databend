#!/usr/bin/env python3

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_FILE = REPO_ROOT / ".github/workflows/reuse.sqllogic.yml"
ACTION_FILE = REPO_ROOT / ".github/actions/test_sqllogic_stage/action.yml"


def main() -> int:
    workflow = WORKFLOW_FILE.read_text(encoding="utf-8")
    action = ACTION_FILE.read_text(encoding="utf-8")

    checks = {
        "stage workflow passes matrix.size to test_sqllogic_stage": (
            "size: ${{ matrix.size }}" in workflow
        ),
        "test_sqllogic_stage action declares a size input": ("\n  size:\n" in action),
        "test_sqllogic_stage action exports TEST_STAGE_SIZE": (
            "TEST_STAGE_SIZE: ${{ inputs.size }}" in action
        ),
    }

    failures = [name for name, ok in checks.items() if not ok]
    if failures:
        for failure in failures:
            print(f"missing check: {failure}", file=sys.stderr)
        return 1

    print("sqllogic stage size wiring checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
