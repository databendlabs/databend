## Copyright 2021 Datafuse Labs
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

from __future__ import annotations

import uuid
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

from .databend import BoxSize
from .databend import DataBlock
from .databend import DataBlocks
from .databend import DataFrame as _RustDataFrame
from .databend import Schema
from .databend import SessionContext as _RustSessionContext
from .databend import init_embedded


def _normalize_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())


def _random_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class Relation:
    def __init__(self, relation: _RustDataFrame):
        self._relation = relation

    def __repr__(self) -> str:
        return repr(self._relation)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._relation, name)

    def df(self):
        return self._relation.to_pandas()

    def pl(self):
        return self._relation.to_polars()

    def arrow(self):
        return self._relation.to_arrow_table()

    def fetchall(self) -> list[tuple[Any, ...]]:
        table = self.arrow()
        columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
        return [
            tuple(column[row_index] for column in columns)
            for row_index in range(table.num_rows)
        ]

    def fetchone(self) -> tuple[Any, ...] | None:
        rows = self.fetchall()
        return rows[0] if rows else None


class SessionContext:
    def __init__(self, _tenant: str | None = None, data_path: str = ".databend"):
        self._tenant = _tenant
        self._data_path = Path(data_path).expanduser().resolve()
        self._ctx = _RustSessionContext(_tenant, str(self._data_path))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ctx, name)

    def sql(self, query: str) -> Relation:
        return Relation(self._ctx.sql(query))

    query = sql

    def execute(self, query: str) -> "SessionContext":
        self._ctx.sql(query).collect()
        return self

    def table(self, name: str) -> Relation:
        return self.sql(f"SELECT * FROM {name}")

    def register_parquet(
        self,
        name: str,
        path: str,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> None:
        self._ctx.register_parquet(name, path, pattern=pattern, connection=connection)

    def register_csv(
        self,
        name: str,
        path: str,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> None:
        self._ctx.register_csv(name, path, pattern=pattern, connection=connection)

    def register_ndjson(
        self,
        name: str,
        path: str,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> None:
        self._ctx.register_ndjson(name, path, pattern=pattern, connection=connection)

    def register_text(
        self,
        name: str,
        path: str,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> None:
        self._ctx.register_text(name, path, pattern=pattern, connection=connection)

    def register(
        self,
        name: str,
        source: Any,
        *,
        format: str | None = None,
        pattern: str | None = None,
        connection: str | None = None,
    ) -> "SessionContext":
        if isinstance(source, (str, Path)):
            source_path = str(source)
            source_format = (format or Path(source_path).suffix.lstrip(".")).lower()
            if source_format in {"parquet", "pq"}:
                self.register_parquet(name, source_path, pattern=pattern, connection=connection)
            elif source_format in {"csv"}:
                self.register_csv(name, source_path, pattern=pattern, connection=connection)
            elif source_format in {"json", "ndjson"}:
                self.register_ndjson(name, source_path, pattern=pattern, connection=connection)
            elif source_format in {"txt", "text", "tsv"}:
                self.register_text(name, source_path, pattern=pattern, connection=connection)
            else:
                raise ValueError(
                    f"Unsupported format for {source_path!r}. "
                    "Use format= explicitly or pass pandas/polars/pyarrow data."
                )
            return self

        parquet_path = self._materialize_relation_source(name, source)
        self.register_parquet(name, parquet_path)
        return self

    def from_df(self, source: Any, *, name: str | None = None) -> Relation:
        target = name or _random_name("df")
        self.register(target, source)
        return self.table(target)

    def read_parquet(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> Relation:
        target = name or _random_name("parquet")
        self.register_parquet(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def read_csv(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> Relation:
        target = name or _random_name("csv")
        self.register_csv(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def read_json(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> Relation:
        target = name or _random_name("json")
        self.register_ndjson(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def read_text(
        self,
        path: str | Path,
        *,
        pattern: str | None = None,
        connection: str | None = None,
        name: str | None = None,
    ) -> Relation:
        target = name or _random_name("text")
        self.register_text(target, str(path), pattern=pattern, connection=connection)
        return self.table(target)

    def _materialize_relation_source(self, name: str, source: Any) -> str:
        table = self._to_arrow_table(source)
        temp_dir = self._data_path / "python" / "registered"
        temp_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = temp_dir / f"{name}_{uuid.uuid4().hex}.parquet"

        import pyarrow.parquet as pq

        pq.write_table(table, parquet_path)
        return _normalize_path(parquet_path)

    @staticmethod
    def _to_arrow_table(source: Any):
        if hasattr(source, "schema") and hasattr(source, "to_pydict"):
            return source

        if hasattr(source, "to_arrow"):
            return source.to_arrow()

        if hasattr(source, "to_pandas"):
            source = source.to_pandas()

        try:
            import pyarrow as pa

            return pa.Table.from_pandas(source, preserve_index=False)
        except Exception as exc:
            raise TypeError(
                "Unsupported source type. Expected path, pandas.DataFrame, "
                "polars.DataFrame, or pyarrow.Table."
            ) from exc


Connection = SessionContext
DataFrame = Relation


def connect(database: str = ":memory:", *, data_path: str | None = None) -> Connection:
    if data_path is not None:
        return Connection(data_path=data_path)

    if database == ":memory:":
        conn = Connection(data_path=mkdtemp(prefix="databend-embedded-"))
        conn._ephemeral = True
        return conn

    return Connection(data_path=database)


__all__ = [
    "Connection",
    "BoxSize",
    "DataBlock",
    "DataBlocks",
    "Relation",
    "DataFrame",
    "Schema",
    "SessionContext",
    "connect",
    "init_embedded",
]
