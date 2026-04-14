import os
import uuid

import databend_driver
import lance
import pyarrow.compute as pc

from ..utils import DATABEND_DSL

debug = False

def _list_stage_files(conn, location):
    return [str(row.values()[0]) for row in conn.query_iter(f"list {location}")]


def _build_lance_storage_options():
    endpoint = os.environ.get("STORAGE_S3_ENDPOINT_URL", "http://127.0.0.1:9900")
    return {
        "aws_access_key_id": os.environ.get(
            "STORAGE_S3_ACCESS_KEY_ID", "minioadmin"
        ),
        "aws_secret_access_key": os.environ.get(
            "STORAGE_S3_SECRET_ACCESS_KEY", "minioadmin"
        ),
        "aws_endpoint": endpoint,
        "aws_allow_http": "true" if endpoint.startswith("http://") else "false",
        "region": os.environ.get("STORAGE_S3_REGION", "us-east-1"),
    }


def _lance_dataset_uri(stage, dataset):
    bucket = os.environ.get("STORAGE_S3_BUCKET", "testbucket")
    root = os.environ.get("STORAGE_S3_ROOT", "admin").strip("/")
    prefix = f"{root}/" if root else ""
    return f"s3://{bucket}/{prefix}stage/internal/{stage}/{dataset}"


def _infer_dataset_root(files):
    roots = set()
    for path in files:
        if "/data/" in path:
            roots.add(path.split("/data/", 1)[0])
        if "/_versions/" in path:
            roots.add(path.split("/_versions/", 1)[0])

    assert len(roots) == 1, f"expect one dataset root, got: {roots}"
    return next(iter(roots))


def test_copy_into_lance_parallel_manifest_complete():
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()

    suffix = uuid.uuid4().hex[:8]
    stage = f"test_lance_copy_{suffix}"
    dataset = f"ds2_{suffix}"
    location = f"@{stage}/{dataset}"

    conn.exec(f"create or replace stage {stage}")
    conn.exec("set max_threads=8")

    try:
        conn.exec(f"remove {location}")

        # Use a small max_file_size + enough rows to trigger parallel append/flush paths.
        copy_sql = (
            f"copy into {location} "
            "from (select number, number + 1 from numbers(200000)) "
            "file_format=(type=lance) "
            "max_file_size=8192 "
            "use_raw_path=true"
        )
        res = conn.query_row(copy_sql)
        assert res.values()[0] == 200000

        files = _list_stage_files(conn, location)
        assert len(list(path.endswith(".lance") and "data/" in path for path in files)) > 2
        assert any("_versions/" in path for path in files)
        assert any(".manifest" in path for path in files)

        # Read back from MinIO via Lance and validate data is complete.
        dataset_uri = _lance_dataset_uri(stage, dataset)
        ds = lance.dataset(dataset_uri, storage_options=_build_lance_storage_options())
        table = ds.to_table()

        assert table.num_rows == 200000
        assert pc.min(table["number"]).as_py() == 0
        assert pc.max(table["number"]).as_py() == 199999
        assert pc.sum(table["number"]).as_py() == 19999900000
        assert pc.sum(table["number + 1"]).as_py() == 20000100000
    finally:
        if debug:
            conn.exec(f"remove @{stage}")
            conn.exec(f"drop stage if exists {stage}")


def test_copy_into_lance_overwrite_raw_path_cleanup_prefix():
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()

    suffix = uuid.uuid4().hex[:8]
    stage = f"test_lance_overwrite_{suffix}"
    dataset = f"ds_overwrite_{suffix}"
    location = f"@{stage}/{dataset}"

    conn.exec(f"create or replace stage {stage}")

    try:
        conn.exec(f"remove {location}")

        first_copy = (
            f"copy into {location} "
            "from (select number from numbers(1000)) "
            "file_format=(type=lance) "
            "use_raw_path=true"
        )
        res = conn.query_row(first_copy)
        assert res.values()[0] == 1000

        junk_file = f"@{stage}/{dataset}/junk_{suffix}.csv"
        conn.exec(
            f"copy into {junk_file} "
            "from (select 1 as c) "
            "file_format=(type=csv) "
            "single=true "
            "use_raw_path=true "
            "overwrite=true"
        )

        files_before = _list_stage_files(conn, location)
        assert any(f"junk_{suffix}.csv" in path for path in files_before)

        second_copy = (
            f"copy into {location} "
            "from (select number from numbers(10)) "
            "file_format=(type=lance) "
            "use_raw_path=true "
            "overwrite=true"
        )
        res = conn.query_row(second_copy)
        assert res.values()[0] == 10

        files_after = _list_stage_files(conn, location)
        assert not any(f"junk_{suffix}.csv" in path for path in files_after)

        dataset_uri = _lance_dataset_uri(stage, dataset)
        ds = lance.dataset(dataset_uri, storage_options=_build_lance_storage_options())
        table = ds.to_table()
        assert table.num_rows == 10
    finally:
        if debug:
            conn.exec(f"remove @{stage}")
            conn.exec(f"drop stage if exists {stage}")


def test_copy_into_lance_default_path_with_query_id_directory():
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()

    suffix = uuid.uuid4().hex[:8]
    stage = f"test_lance_default_path_{suffix}"
    base_path = f"lance_default_{suffix}"
    location = f"@{stage}/{base_path}"

    conn.exec(f"create or replace stage {stage}")

    try:
        conn.exec(f"remove @{stage}/{base_path}")

        copy_sql = (
            f"copy into {location} "
            "from (select number from numbers(128)) "
            "file_format=(type=lance)"
        )
        res = conn.query_row(copy_sql)
        assert res.values()[0] == 128

        files = _list_stage_files(conn, f"@{stage}/{base_path}")
        assert any(path.endswith(".lance") and "data/" in path for path in files)
        assert any("_versions/" in path for path in files)

        dataset_root = _infer_dataset_root(files)
        assert dataset_root.startswith(base_path)
        assert dataset_root != base_path

        dataset_uri = _lance_dataset_uri(stage, dataset_root)
        ds = lance.dataset(dataset_uri, storage_options=_build_lance_storage_options())
        table = ds.to_table()
        assert table.num_rows == 128
        assert pc.min(table["number"]).as_py() == 0
        assert pc.max(table["number"]).as_py() == 127
    finally:
        if debug:
            conn.exec(f"remove @{stage}")
            conn.exec(f"drop stage if exists {stage}")


def test_copy_into_lance_detailed_output_is_aggregated_once():
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()

    suffix = uuid.uuid4().hex[:8]
    stage = f"test_lance_detail_{suffix}"
    dataset = f"ds_detail_{suffix}"
    location = f"@{stage}/{dataset}"

    conn.exec(f"create or replace stage {stage}")
    conn.exec("set max_threads=8")

    try:
        conn.exec(f"remove {location}")

        copy_sql = (
            f"copy into {location} "
            "from (select number, number + 1 from numbers(200000)) "
            "file_format=(type=lance) "
            "max_file_size=8192 "
            "use_raw_path=true "
            "detailed_output=true"
        )
        rows = list(conn.query_iter(copy_sql))
        assert len(rows) == 1

        values = rows[0].values()
        assert len(values) == 3
        assert dataset in str(values[0])
        assert values[1] > 0
        assert values[2] == 200000

        dataset_uri = _lance_dataset_uri(stage, dataset)
        ds = lance.dataset(dataset_uri, storage_options=_build_lance_storage_options())
        table = ds.to_table()
        assert table.num_rows == 200000
    finally:
        if debug:
            conn.exec(f"remove @{stage}")
            conn.exec(f"drop stage if exists {stage}")


def test_copy_into_lance_with_string_literal_projection():
    client = databend_driver.BlockingDatabendClient(DATABEND_DSL)
    conn = client.get_conn()

    suffix = uuid.uuid4().hex[:8]
    stage = f"test_lance_utf8view_{suffix}"
    dataset = f"ds_utf8view_{suffix}"
    location = f"@{stage}/{dataset}"

    conn.exec(f"create or replace stage {stage}")

    try:
        conn.exec(f"remove {location}")

        copy_sql = (
            f"copy into {location} "
            "from ("
            "select number, 'abc' as literal, number + 1 as label "
            "from numbers(10)"
            ") "
            "file_format=(type=lance) "
            "use_raw_path=true "
            "overwrite=true "
            "detailed_output=true"
        )
        rows = list(conn.query_iter(copy_sql))
        assert len(rows) == 1
        assert rows[0].values()[2] == 10

        dataset_uri = _lance_dataset_uri(stage, dataset)
        ds = lance.dataset(dataset_uri, storage_options=_build_lance_storage_options())
        table = ds.to_table()

        assert table.num_rows == 10
        assert table["literal"].to_pylist() == ["abc"] * 10
        assert pc.sum(table["label"]).as_py() == 55
    finally:
        if debug:
            conn.exec(f"remove @{stage}")
            conn.exec(f"drop stage if exists {stage}")
