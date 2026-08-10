import pytest

from .copy_utils import run_all


# For row-based formats, queries must include at least one $N column reference.
# Parquet and ORC use named columns directly.
FORMATS = ["csv", "ndjson", "text", "parquet", "orc", "avro"]


def _col_ref(fmt):
    """Return a data column reference appropriate for the format."""
    if fmt in ("parquet", "orc"):
        return "a"
    return "$1"


def _storage_type(conn):
    """Return the configured storage backend type (e.g. 'fs', 's3')."""
    row = conn.query_row("select value from system.configs where name = 'type'")
    return row.values()[0]


def _content_key_present(conn):
    """content_key is backed by ETag/md5, present only on object stores.

    Object stores (s3/minio/azblob/...) expose an ETag or md5; the local fs
    backend has no such value, so content_key is NULL there.
    """
    return _storage_type(conn) != "fs"


def _unload(
    conn,
    name,
    path,
    fmt,
    query="select number::int64 as a, (number + 100)::int64 as b from numbers(2)",
):
    """Create a stage and unload `query` to a known raw path inside it.

    Using single=true + use_raw_path=true gives a deterministic file name so we
    can assert metadata$file_basename exactly.
    """
    conn.exec(f"create or replace stage {name}")
    conn.exec(
        f"copy into @{name}/{path} from ({query}) "
        f"file_format=(type={fmt}) single=true use_raw_path=true overwrite=true"
    )


@pytest.mark.parametrize("fmt", FORMATS)
def test_metadata_file_path_equals_filename(copy_env, fmt):
    """metadata$file_path and metadata$filename are equivalent."""
    name = copy_env.uniq_name
    conn = copy_env.conn
    col = _col_ref(fmt)
    _unload(conn, name, f"data/f1.{fmt}", fmt)

    run_all(conn, [
        (
            f"select metadata$file_path = metadata$filename "
            f"from (select {col}, metadata$file_path, metadata$filename "
            f"from @{name}/data/f1.{fmt} (file_format => '{fmt}')) limit 1;",
            [(True,)],
        ),
    ])


@pytest.mark.parametrize("fmt", FORMATS)
def test_metadata_file_basename(copy_env, fmt):
    """metadata$file_basename is exactly the file name (no directory prefix)."""
    name = copy_env.uniq_name
    conn = copy_env.conn
    col = _col_ref(fmt)
    _unload(conn, name, f"sub/dir/f1.{fmt}", fmt)

    run_all(conn, [
        (
            f"select metadata$file_basename, metadata$filename "
            f"from (select {col}, metadata$file_basename, metadata$filename "
            f"from @{name}/sub/dir/f1.{fmt} (file_format => '{fmt}')) limit 1;",
            [(f"f1.{fmt}", f"sub/dir/f1.{fmt}")],
        ),
    ])


@pytest.mark.parametrize("fmt", FORMATS)
def test_metadata_file_row_number(copy_env, fmt):
    """metadata$file_row_number is the 0-based row index within the file."""
    name = copy_env.uniq_name
    conn = copy_env.conn
    col = _col_ref(fmt)
    _unload(conn, name, f"rn/f1.{fmt}", fmt)

    run_all(conn, [
        (
            f"select metadata$file_row_number "
            f"from (select {col}, metadata$file_row_number "
            f"from @{name}/rn/f1.{fmt} (file_format => '{fmt}')) "
            f"order by metadata$file_row_number;",
            [(0,), (1,)],
        ),
    ])


@pytest.mark.parametrize("fmt", FORMATS)
def test_metadata_file_content_key(copy_env, fmt):
    """metadata$file_content_key is nullable string; non-null on object stores.

    Object stores (s3/minio/azblob/...) expose an ETag or md5 that backs the
    content key, so it must be present. The local fs backend has no such value,
    so NULL is expected there.
    """
    name = copy_env.uniq_name
    conn = copy_env.conn
    col = _col_ref(fmt)
    _unload(conn, name, f"ck/f1.{fmt}", fmt)

    # On fs: content_key is NULL. On object stores: it must be non-null.
    content_present_expected = _content_key_present(conn)

    run_all(conn, [
        (
            f"select typeof(metadata$file_content_key), "
            f"metadata$file_content_key is not null "
            f"from (select {col}, metadata$file_content_key "
            f"from @{name}/ck/f1.{fmt} (file_format => '{fmt}')) limit 1;",
            [("VARCHAR NULL", content_present_expected)],
        ),
    ])


@pytest.mark.parametrize("fmt", FORMATS)
def test_metadata_file_last_modified(copy_env, fmt):
    """metadata$file_last_modified is a timestamp set close to write time."""
    name = copy_env.uniq_name
    conn = copy_env.conn
    col = _col_ref(fmt)
    _unload(conn, name, f"lm/f1.{fmt}", fmt)

    run_all(conn, [
        (
            f"select typeof(metadata$file_last_modified), "
            f"metadata$file_last_modified > now() - interval 10 minute, "
            f"metadata$file_last_modified <= now() "
            f"from (select {col}, metadata$file_last_modified "
            f"from @{name}/lm/f1.{fmt} (file_format => '{fmt}')) limit 1;",
            [("TIMESTAMP NULL", True, True)],
        ),
    ])


@pytest.mark.parametrize("fmt", FORMATS)
def test_metadata_all_columns_together(copy_env, fmt):
    """All metadata columns can be selected together and agree with each other."""
    name = copy_env.uniq_name
    conn = copy_env.conn
    col = _col_ref(fmt)
    _unload(conn, name, f"all/f1.{fmt}", fmt)

    content_present_expected = _content_key_present(conn)

    run_all(conn, [
        (
            f"select "
            f"metadata$file_path = metadata$filename, "
            f"metadata$file_basename = 'f1.{fmt}', "
            f"metadata$file_row_number, "
            f"metadata$file_content_key is not null, "
            f"metadata$file_last_modified > now() - interval 10 minute "
            f"from (select {col}, metadata$file_path, metadata$filename, "
            f"metadata$file_basename, metadata$file_row_number, "
            f"metadata$file_content_key, metadata$file_last_modified "
            f"from @{name}/all/f1.{fmt} (file_format => '{fmt}')) "
            f"order by metadata$file_row_number;",
            [
                (True, True, 0, content_present_expected, True),
                (True, True, 1, content_present_expected, True),
            ],
        ),
    ])
