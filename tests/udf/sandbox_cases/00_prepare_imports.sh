echo "Preparing stage imports"
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9900

create_stage_sql=$(
	cat <<SQL
CREATE STAGE IF NOT EXISTS ${UDF_IMPORT_STAGE}
URL = 's3://testbucket/udf-imports/'
CONNECTION = (
  access_key_id = '${S3_ACCESS_KEY_ID}',
  secret_access_key = '${S3_SECRET_ACCESS_KEY}',
  endpoint_url = '${S3_ENDPOINT_URL}'
);
SQL
)
execute_query "$create_stage_sql"

tmp_dir="$(mktemp -d)"
helper_file="${tmp_dir}/helper.py"
data_file="${tmp_dir}/data.txt"
extra_file="${tmp_dir}/extra.txt"
dup_a_dir="${tmp_dir}/dup_a"
dup_b_dir="${tmp_dir}/dup_b"
dup_a_file="${dup_a_dir}/dup.txt"
dup_b_file="${dup_b_dir}/dup.txt"
archive_inner="${tmp_dir}/archive.txt"
archive_file="${tmp_dir}/archive.tar.gz"
zip_mod="${tmp_dir}/zip_mod.py"
whl_mod="${tmp_dir}/whl_mod.py"
egg_mod="${tmp_dir}/egg_mod.py"
zip_file="${tmp_dir}/lib.zip"
whl_file="${tmp_dir}/lib.whl"
egg_file="${tmp_dir}/lib.egg"
cat <<'PY' >"$helper_file"
def add_one(x: int) -> int:
    return x + 1
PY
cat <<'TXT' >"$data_file"
stage_data
TXT
cat <<'TXT' >"$extra_file"
extra_data
TXT
mkdir -p "$dup_a_dir" "$dup_b_dir"
cat <<'TXT' >"$dup_a_file"
dup_a
TXT
cat <<'TXT' >"$dup_b_file"
dup_b
TXT
cat <<'TXT' >"$archive_inner"
archive_data
TXT
cat <<'PY' >"$zip_mod"
VALUE = "zip"
PY
cat <<'PY' >"$whl_mod"
VALUE = "whl"
PY
cat <<'PY' >"$egg_mod"
VALUE = "egg"
PY
ARCHIVE_FILE="$archive_file" ARCHIVE_INNER="$archive_inner" python3 - <<'PY'
import os
import tarfile

archive_file = os.environ["ARCHIVE_FILE"]
archive_inner = os.environ["ARCHIVE_INNER"]
with tarfile.open(archive_file, "w:gz") as tf:
    tf.add(archive_inner, arcname="archive.txt")
PY
ZIP_FILE="$zip_file" WHL_FILE="$whl_file" EGG_FILE="$egg_file" \
	ZIP_MOD="$zip_mod" WHL_MOD="$whl_mod" EGG_MOD="$egg_mod" python3 - <<'PY'
import os
import zipfile

zip_path = os.environ["ZIP_FILE"]
whl_path = os.environ["WHL_FILE"]
egg_path = os.environ["EGG_FILE"]
zip_mod = os.environ["ZIP_MOD"]
whl_mod = os.environ["WHL_MOD"]
egg_mod = os.environ["EGG_MOD"]

with zipfile.ZipFile(zip_path, "w") as zf:
    zf.write(zip_mod, arcname="zip_mod.py")
with zipfile.ZipFile(whl_path, "w") as zf:
    zf.write(whl_mod, arcname="whl_mod.py")
with zipfile.ZipFile(egg_path, "w") as zf:
    zf.write(egg_mod, arcname="egg_mod.py")
PY

upload_stage_file "$UDF_IMPORT_STAGE" "imports" "$helper_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports" "$data_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports" "$archive_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports/subdir" "$extra_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports/dup_a" "$dup_a_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports/dup_b" "$dup_b_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports" "$zip_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports" "$whl_file"
upload_stage_file "$UDF_IMPORT_STAGE" "imports" "$egg_file"
