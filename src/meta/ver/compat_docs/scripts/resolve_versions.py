#!/usr/bin/env python3
"""Merge external crate versions into min_compatible_versions.txt.

Resolves CalVer references so the output has only databend tag versions.
"""

import os
import sys

from tsv import parse_tsv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "..")
SRC_DIR = os.path.join(BASE_DIR, "src")

INPUT = os.path.join(SRC_DIR, "min_compatible_versions.txt")
EXTERNAL = os.path.join(SRC_DIR, "external-meta-min-compatibles.txt")
OUTPUT = os.path.join(BASE_DIR, "generated", "resolved_min_compatibles.txt")

EXT_PREFIX = "ext:"


class ExternalMinCompatibles:
    """Resolves ext: CalVer references to databend repo tag versions.

    Combines two data sources:
    - external-meta-min-compatibles.txt: CalVer → (min_server, min_client)
    - min_compatible_versions.txt: CalVer → first repo tag (per column)
    """

    def __init__(
        self,
        min_compatibles: dict[str, tuple[str, str]],
        client_calver_to_tag: dict[str, str],
        service_calver_to_tag: dict[str, str],
    ):
        """
        Args:
            min_compatibles: CalVer → (min_server, min_client) from external crate.
            client_calver_to_tag: maps a client CalVer to the first databend tag that uses it.
            service_calver_to_tag: maps a service CalVer to the first databend tag that uses it.
        """
        self._min_compatibles = min_compatibles
        self._client_calver_to_tag = client_calver_to_tag
        self._service_calver_to_tag = service_calver_to_tag

    @staticmethod
    def load(external_path: str, input_path: str) -> "ExternalMinCompatibles":
        """Build from external-meta-min-compatibles.txt and min_compatible_versions.txt."""
        min_compatibles = {
            r[0]: (r[1], r[2]) for r in parse_tsv(external_path, 3)
        }

        client_calver_to_tag: dict[str, str] = {}
        service_calver_to_tag: dict[str, str] = {}
        for r in parse_tsv(input_path, 3):
            ver = _strip_ext_prefix(r[1])
            if ver is not None:
                client_calver_to_tag.setdefault(ver, r[0])
            ver = _strip_ext_prefix(r[2])
            if ver is not None:
                service_calver_to_tag.setdefault(ver, r[0])

        return ExternalMinCompatibles(
            min_compatibles, client_calver_to_tag, service_calver_to_tag
        )

    def resolve_min_server(self, v: str) -> str | None:
        """Resolve a MetaClient column value to a min-server repo tag.

        If v is "ext:X", looks up X → (min_server, _) and translates to a tag.
        Non-ext values pass through unchanged.
        """
        return self._resolve(v, 0, self._service_calver_to_tag)

    def resolve_min_client(self, v: str) -> str | None:
        """Resolve a MetaService column value to a min-client repo tag.

        If v is "ext:X", looks up X → (_, min_client) and translates to a tag.
        Non-ext values pass through unchanged.
        """
        return self._resolve(v, 1, self._client_calver_to_tag)

    def resolve_to_min_compatibles(
        self, tag: str, meta_client: str, meta_service: str
    ) -> tuple[str, str, str] | None:
        """Resolve ext: CalVer in meta_client/meta_service to the first databend tag that references each CalVer.

        Non-ext values pass through unchanged.
        Returns (tag, min_client, min_server), or None if a CalVer cannot be resolved.
        """
        server = self.resolve_min_server(meta_client)
        if server is None:
            return None

        client = self.resolve_min_client(meta_service)
        if client is None:
            return None

        return (tag, client, server)

    def _resolve(
        self, v: str, ext_col: int, calver_to_tag: dict[str, str]
    ) -> str | None:
        ver = _strip_ext_prefix(v)
        if ver is not None:
            if ver not in self._min_compatibles:
                return None
            resolved = self._min_compatibles[ver][ext_col]
            return calver_to_tag.get(resolved, resolved)
        return v


def _strip_ext_prefix(v: str) -> str | None:
    """Return the version after "ext:" prefix, or None if not an ext ref."""
    if v.startswith(EXT_PREFIX):
        return v[len(EXT_PREFIX):]
    return None


def write_resolved_versions(rows: list[tuple[str, str, str]]) -> None:
    """Write resolved rows (tag, min_client, min_server) to OUTPUT file."""
    with open(OUTPUT, "w") as out:
        out.write(f"{'tag':<16}{'min_meta_client':<20}{'min_meta_server':<20}\n")
        out.write("-" * 56 + "\n")
        for tag, client, server in rows:
            out.write(f"{tag:<16}{client:<20}{server:<20}\n")


def main():
    ext_compat = ExternalMinCompatibles.load(EXTERNAL, INPUT)

    resolved = []
    for row in parse_tsv(INPUT, 3):
        resolved_row = ext_compat.resolve_to_min_compatibles(*row)
        if resolved_row:
            resolved.append(resolved_row)

    write_resolved_versions(resolved)
    print(f"Written to {OUTPUT}", file=sys.stderr)


if __name__ == "__main__":
    main()
