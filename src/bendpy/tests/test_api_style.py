from pathlib import Path
from unittest.mock import patch

import databend
import pandas as pd
import pyarrow as pa


def test_connect_returns_session_context(tmp_path):
    conn = databend.connect(data_path=str(tmp_path / "embedded"))
    result = conn.sql("select 1").fetchone()
    assert result == (1,)


def test_execute_and_relation_fetch_helpers():
    conn = databend.connect()
    conn.execute("create or replace table t(a int)")
    conn.execute("insert into t values (1), (2)")

    relation = conn.table("t")
    assert relation.fetchall() == [(1,), (2,)]
    assert relation.df().values.tolist() == [[1], [2]]


def test_relation_fetch_helpers_preserve_duplicate_column_names():
    conn = databend.connect()
    relation = conn.sql("select 1 as a, 2 as a")

    assert relation.fetchall() == [(1, 2)]
    assert relation.fetchone() == (1, 2)


def test_register_pandas_dataframe_materializes_local_parquet(tmp_path):
    conn = databend.connect(data_path=str(tmp_path / "embedded"))
    frame = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    relation = conn.from_df(frame, name="memory_docs")

    assert relation.fetchall() == [(1, "a"), (2, "b")]


def test_register_arrow_table_uses_register_parquet(tmp_path):
    conn = databend.SessionContext(data_path=str(tmp_path / "embedded"))
    table = pa.table({"id": [1, 2]})

    with patch.object(conn, "register_parquet") as register_parquet:
        conn.register("arrow_docs", table)

    register_parquet.assert_called_once()
    _, parquet_path = register_parquet.call_args.args
    assert Path(parquet_path).suffix == ".parquet"


def test_register_csv_local_path_allows_select_star(tmp_path):
    csv_path = tmp_path / "foods.csv"
    csv_path.write_text("1,apple\n2,bread\n")
    conn = databend.SessionContext(data_path=str(tmp_path / "embedded"))

    conn.register_csv("foods", str(csv_path))

    rows = conn.table("foods").fetchall()
    assert len(rows) == 2
    assert len(rows[0]) == 2


def test_register_text_local_path_allows_select_star(tmp_path):
    text_path = tmp_path / "foods.tsv"
    text_path.write_text("1\tapple\n2\tbread\n")
    conn = databend.SessionContext(data_path=str(tmp_path / "embedded"))

    conn.register_text("foods_text", str(text_path))

    rows = conn.table("foods_text").fetchall()
    assert len(rows) == 2
    assert len(rows[0]) == 2
