#!/usr/bin/env python3
"""Check a TLA+ model directory locally with a verified TLC jar."""

import argparse
import hashlib
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# v1.8.0 and rolling are mutable master-build releases; use the stable release.
TLC_VERSION = "1.7.4"
TLC_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLC_URL = (
    f"https://github.com/tlaplus/tlaplus/releases/download/v{TLC_VERSION}/tla2tools.jar"
)
CACHE_DIR = Path(__file__).resolve().parents[1] / "cache"
CASE_NAME = re.compile(r"[\w-]+")
CASE_KEYS = {"constants", "invariants", "violation", "witness"}


def literal(value):
    if isinstance(value, bool):
        return str(value).upper()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and re.fullmatch(r'[^"\\\n]*', value):
        return f'"{value}"'
    raise ValueError(f"Unsupported TLA constant: {value!r}")


def cases(spec):
    """Expand the matrix and named cases into {name: (constants, invariants, expected)}."""
    for key in ("module", "constants", "invariants"):
        if key not in spec:
            raise ValueError(f"model.json is missing '{key}'")
    defaults = spec["constants"]
    if not spec["invariants"]:
        raise ValueError("model.json needs at least one invariant")
    matrix = spec.get("matrix", {})
    if matrix.keys() - defaults.keys() or any(not v for v in matrix.values()):
        raise ValueError("Matrix keys must be declared constants with nonempty domains")
    result = {}
    for values in itertools.product(*matrix.values()):
        overrides = dict(zip(matrix, values, strict=True))
        name = ",".join(f"{k}={label(v)}" for k, v in overrides.items()) or "default"
        result[name] = (defaults | overrides, list(spec["invariants"]), None)
    for name, case in spec.get("cases", {}).items():
        if not CASE_NAME.fullmatch(name) or name in result:
            raise ValueError(f"Invalid or duplicate case name: {name}")
        if case.keys() - CASE_KEYS:
            raise ValueError(
                f"Unknown options in case {name}: {sorted(case.keys() - CASE_KEYS)}"
            )
        overrides = case.get("constants", {})
        if overrides.keys() - defaults.keys():
            raise ValueError(f"Undeclared constants in case {name}")
        if "witness" in case and ("violation" in case or "invariants" in case):
            raise ValueError(f"A witness keeps the default invariants: {name}")
        invariants = list(case.get("invariants", spec["invariants"]))
        expected = case.get("violation", case.get("witness"))
        if "witness" in case:
            invariants.append(expected)
        if expected and expected not in invariants:
            raise ValueError(f"Expected invariant is not checked in case {name}")
        result[name] = (defaults | overrides, invariants, expected)
    return result


def label(value):
    if not isinstance(value, str):
        return literal(value)
    if not re.fullmatch(r"[\w.-]+", value):
        raise ValueError(f"Matrix string values must be simple names: {value!r}")
    return value


def config_text(constants, invariants):
    if not invariants:
        raise ValueError("At least one invariant is required")
    return "\n".join(
        [
            "CONSTANTS",
            *(f"    {name} = {literal(value)}" for name, value in constants.items()),
            "INIT Init",
            "NEXT Next",
            "CHECK_DEADLOCK FALSE",
            "INVARIANTS " + " ".join(invariants),
            "",
        ]
    )


def verdict(code, text, expected):
    """Normal cases need a complete search; controls need exactly the expected violation."""
    errors = [line for line in text.splitlines() if line.startswith("Error:")]
    if expected:
        return (
            code == 12
            and errors
            == [
                f"Error: Invariant {expected} is violated.",
                "Error: The behavior up to this point is:",
            ]
            and "State 1:" in text
        )
    return (
        code == 0
        and not errors
        and "Model checking completed. No error has been found." in text
        and re.search(
            r"(?m)^\d+ states generated, \d+ distinct states found, 0 states left on queue\.$",
            text,
        )
        is not None
    )


def cached_jar(cache_dir=CACHE_DIR):
    return cache_dir / "tlc" / TLC_SHA256 / f"tla2tools-{TLC_VERSION}.jar"


def verify_jar(path):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != TLC_SHA256:
        raise ValueError(
            f"SHA-256 mismatch for {path}: expected {TLC_SHA256}, got {digest}"
        )


def find_java(explicit):
    if explicit:
        return explicit
    home = os.environ.get("JAVA_HOME")
    return str(Path(home) / "bin" / "java") if home else "java"


def run_case(args, jar, directory, module, expected):
    command = [
        args.java,
        "-Xmx2g",
        "-XX:+UseParallelGC",
        f"-Djava.io.tmpdir={directory}",
        "-cp",
        str(jar),
        "tlc2.TLC",
        "-workers",
        str(args.workers),
        "-seed",
        "1",
        "-fp",
        "0",
        "-config",
        "model.cfg",
        f"{module}.tla",
    ]
    started = time.monotonic()
    log_path = directory / "tlc.log"
    with log_path.open("w") as log:
        try:
            result = subprocess.run(
                command,
                cwd=directory,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=args.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            print(
                f"FAIL {directory.name}: timeout after {args.timeout}s; {log_path}",
                flush=True,
            )
            return False
    text = log_path.read_text(errors="replace")
    passed = verdict(result.returncode, text, expected)
    kind = f"expected {expected} counterexample" if expected else "exhaustive safety"
    elapsed = time.monotonic() - started
    print(
        f"{'PASS' if passed else 'FAIL'} {directory.name}: {kind} ({elapsed:.2f}s)",
        flush=True,
    )
    for line in text.splitlines():
        if re.match(r"\d+ states generated,", line):
            print(f"  {line}", flush=True)
    if not passed:
        print(f"  TLC exit {result.returncode}; {log_path}", flush=True)
    return passed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "model_dir", type=Path, help="Directory containing model.json and <Module>.tla"
    )
    parser.add_argument("--list", action="store_true", help="List case names and exit")
    parser.add_argument(
        "--case", action="append", help="Run only this case; repeatable"
    )
    parser.add_argument(
        "--java", help="Java executable; default $JAVA_HOME/bin/java, then java"
    )
    parser.add_argument(
        "--jar", type=Path, default=cached_jar(), help="Verified TLC 1.7.4 jar"
    )
    parser.add_argument(
        "--output", type=Path, help="New artifact directory; default: nox cache"
    )
    parser.add_argument("--timeout", type=int, default=600, help="Seconds per case")
    parser.add_argument("--workers", type=int, default=2, help="TLC worker threads")
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.workers <= 0:
        parser.error("--timeout and --workers must be positive")
    model_dir = args.model_dir.expanduser().resolve()
    spec = json.loads((model_dir / "model.json").read_text())
    selected = cases(spec)
    if args.case:
        unknown = set(args.case) - selected.keys()
        if unknown:
            parser.error(f"Unknown cases: {sorted(unknown)}")
        selected = {k: v for k, v in selected.items() if k in args.case}
    if args.list:
        print("\n".join(selected))
        return 0
    module = spec["module"]
    if not re.fullmatch(r"\w+", module) or not (model_dir / f"{module}.tla").is_file():
        raise ValueError(
            f"model.json module must name an existing {model_dir}/<Module>.tla"
        )
    jar = args.jar.expanduser().resolve()
    if not jar.is_file():
        raise ValueError(
            "TLC jar missing; run `nox -s tla -- <model-dir>` from tests/nox to prepare it"
        )
    verify_jar(jar)
    args.java = find_java(args.java)
    subprocess.run([args.java, "-version"], check=True, timeout=30)
    if args.output:
        root = args.output.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=False)
    else:
        output_dir = CACHE_DIR / "test-output" / "tla" / model_dir.name
        output_dir.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="run-", dir=output_dir))
    shutil.copy2(model_dir / "model.json", root)
    print(f"Artifacts: {root}", flush=True)
    results = []
    for name, (constants, invariants, expected) in selected.items():
        directory = root / name
        directory.mkdir()
        for source in model_dir.glob("*.tla"):
            shutil.copy2(source, directory)
        (directory / "model.cfg").write_text(config_text(constants, invariants))
        results.append(run_case(args, jar, directory, module, expected))
    print(f"{sum(results)}/{len(results)} checks passed; artifacts: {root}", flush=True)
    return 0 if all(results) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        if isinstance(error, (FileNotFoundError, subprocess.CalledProcessError)):
            print(
                "Hint: Java 11+ is required; pass --java or set JAVA_HOME.",
                file=sys.stderr,
            )
        sys.exit(1)
