import os
import uuid

import databend_driver
import lance
import pyarrow.compute as pc

from ..utils import DATABEND_DSL


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
        assert any(path.endswith(".lance") and "data/" in path for path in files)
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
        conn.exec(f"remove @{stage}")
        conn.exec(f"drop stage if exists {stage}")
