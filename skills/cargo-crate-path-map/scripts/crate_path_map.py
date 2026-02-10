#!/usr/bin/env python3
"""Map file paths to Cargo package names for a Rust workspace."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import List, Optional, Tuple

try:
    import tomllib as toml  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - fallback when tomllib is missing
    try:
        import tomli as toml  # type: ignore
    except ModuleNotFoundError:
        toml = None

SKIP_DIRS = {".git", "target", ".idea", ".vscode", "node_modules", ".tox", ".mypy_cache"}


def _real(path: str) -> str:
    return os.path.realpath(path)


def _normalize_path(path: str, base: Optional[str] = None) -> str:
    if not os.path.isabs(path):
        base = base or os.getcwd()
        path = os.path.join(base, path)
    return _real(path)


def _run_cargo_metadata(cwd: str) -> Tuple[Optional[dict], Optional[str]]:
    cmd = ["cargo", "metadata", "--no-deps", "--format-version", "1"]
    try:
        output = subprocess.check_output(cmd, cwd=cwd, text=True)
    except FileNotFoundError as exc:
        return None, f"cargo not found: {exc}"
    except subprocess.CalledProcessError as exc:
        return None, f"cargo metadata failed: {exc}"
    try:
        return json.loads(output), None
    except json.JSONDecodeError as exc:
        return None, f"invalid cargo metadata JSON: {exc}"


def _packages_from_metadata(root: str) -> Tuple[List[dict], str]:
    data, err = _run_cargo_metadata(root)
    if data is None:
        raise RuntimeError(err or "cargo metadata failed")
    packages = []
    for pkg in data.get("packages", []):
        name = pkg.get("name")
        manifest_path = pkg.get("manifest_path")
        if not name or not manifest_path:
            continue
        manifest_path = _real(manifest_path)
        manifest_dir = os.path.dirname(manifest_path)
        packages.append(
            {
                "name": name,
                "manifest_path": manifest_path,
                "manifest_dir": manifest_dir,
            }
        )
    workspace_root = data.get("workspace_root") or root
    return packages, _real(workspace_root)


def _read_toml(path: str) -> Optional[dict]:
    if toml is None:
        return None
    try:
        with open(path, "rb") as f:
            return toml.load(f)
    except Exception:
        return None


def _packages_from_scan(root: str) -> Tuple[List[dict], str]:
    if toml is None:
        raise RuntimeError("tomllib/tomli not available; cannot scan Cargo.toml")
    packages = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        if "Cargo.toml" not in filenames:
            continue
        manifest_path = os.path.join(dirpath, "Cargo.toml")
        data = _read_toml(manifest_path)
        if not data:
            continue
        pkg = data.get("package") or {}
        name = pkg.get("name")
        if not name:
            continue
        manifest_path = _real(manifest_path)
        packages.append(
            {
                "name": name,
                "manifest_path": manifest_path,
                "manifest_dir": os.path.dirname(manifest_path),
            }
        )
    return packages, _real(root)


def _is_within(parent: str, child: str) -> bool:
    parent = _real(parent)
    child = _real(child)
    if child == parent:
        return True
    return child.startswith(parent + os.sep)


def _find_owner(file_path: str, packages: List[dict]) -> Optional[dict]:
    best = None
    best_len = -1
    for pkg in packages:
        root = pkg["manifest_dir"]
        if _is_within(root, file_path):
            length = len(root)
            if length > best_len:
                best = pkg
                best_len = length
    return best


def _print_list(packages: List[dict]) -> None:
    for pkg in sorted(packages, key=lambda p: p["name"]):
        print(f"{pkg['name']}	{pkg['manifest_dir']}	{pkg['manifest_path']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Map file paths to Cargo crates.")
    parser.add_argument("--workspace-root", default=".", help="Workspace root (default: .)")
    parser.add_argument(
        "--method",
        choices=["auto", "metadata", "scan"],
        default="auto",
        help="Mapping method (default: auto)",
    )
    parser.add_argument("--file", action="append", default=[], help="File path to map")
    parser.add_argument("--list", action="store_true", help="List all crates")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    args = parser.parse_args()

    root = _normalize_path(args.workspace_root)

    packages: List[dict]
    workspace_root = root
    method_used = "auto"

    if args.method in ("auto", "metadata"):
        try:
            packages, workspace_root = _packages_from_metadata(root)
            method_used = "metadata"
        except RuntimeError as exc:
            if args.method == "metadata":
                print(str(exc), file=sys.stderr)
                return 2
            packages = []

    if args.method == "scan" or (args.method == "auto" and not packages):
        try:
            packages, workspace_root = _packages_from_scan(root)
            method_used = "scan"
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    if not args.file and not args.list:
        args.list = True

    files_result = []
    if args.file:
        for raw_path in args.file:
            file_path = _normalize_path(raw_path, base=workspace_root)
            owner = _find_owner(file_path, packages)
            if owner:
                files_result.append(
                    {
                        "file": raw_path,
                        "resolved_path": file_path,
                        "package": owner["name"],
                        "manifest_dir": owner["manifest_dir"],
                        "manifest_path": owner["manifest_path"],
                    }
                )
            else:
                files_result.append(
                    {
                        "file": raw_path,
                        "resolved_path": file_path,
                        "package": None,
                        "manifest_dir": None,
                        "manifest_path": None,
                    }
                )

    if args.json:
        payload = {
            "method": method_used,
            "workspace_root": workspace_root,
            "packages": packages if args.list else [],
            "files": files_result,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    if args.list:
        _print_list(packages)

    for entry in files_result:
        if entry["package"]:
            print(
                f"{entry['file']} -> {entry['package']} ({entry['manifest_dir']})"
            )
        else:
            print(f"{entry['file']} -> (no crate)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
