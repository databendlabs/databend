"""Shared parser for whitespace-separated version files."""


def parse_tsv(path: str, min_columns: int) -> list[list[str]]:
    """Parse a whitespace-separated file, skipping headers and separators.

    Header lines (starting with a letter) and separator lines (starting
    with "-") are excluded. Returns rows with at least min_columns fields.
    """
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line[0].isalpha() or line.startswith("-"):
                continue
            parts = line.split()
            if len(parts) >= min_columns:
                rows.append(parts)
    return rows
