"""Read the last line of resolved_min_compatibles.txt and update ../src/lib.rs."""

import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
RESOLVED = SCRIPT_DIR / "../generated/resolved_min_compatibles.txt"
LIB_RS = SCRIPT_DIR / "../../src/lib.rs"


def last_data_line(path):
    """Return the last non-empty, non-header line."""
    for line in reversed(path.read_text().splitlines()):
        line = line.strip()
        if line and not line.startswith("-") and not line.startswith("tag"):
            return line
    raise ValueError(f"No data line found in {path}")


def parse_versions(line):
    """Parse 'tag  min_client  min_server' from a whitespace-separated line."""
    parts = line.split()
    assert len(parts) >= 3, f"Expected at least 3 columns, got: {line}"
    return parts[1], parts[2]  # min_client, min_server


def version_to_rust(ver_str):
    """Convert '1.2.869' to 'Version::new(1, 2, 869)'."""
    parts = ver_str.split(".")
    assert len(parts) == 3, f"Expected x.y.z, got: {ver_str}"
    return f"Version::new({parts[0]}, {parts[1]}, {parts[2]})"


def main():
    line = last_data_line(RESOLVED)
    min_client, min_server = parse_versions(line)

    text = LIB_RS.read_text()

    text = re.sub(
        r"(MIN_METASRV_VER_FOR_QUERY:\s*LazyLock<Version>\s*=\s*LazyLock::new\(\|\| )Version::new\([^)]+\)",
        rf"\g<1>{version_to_rust(min_server)}",
        text,
    )
    text = re.sub(
        r"(MIN_QUERY_VER_FOR_METASRV:\s*LazyLock<Version>\s*=\s*LazyLock::new\(\|\| )Version::new\([^)]+\)",
        rf"\g<1>{version_to_rust(min_client)}",
        text,
    )

    LIB_RS.write_text(text)
    print(f"Updated {LIB_RS}:")
    print(f"  MIN_METASRV_VER_FOR_QUERY = {min_server}")
    print(f"  MIN_QUERY_VER_FOR_METASRV = {min_client}")


if __name__ == "__main__":
    main()
