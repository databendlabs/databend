echo "Creating import reader UDF"
create_reader_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION read_stage()
RETURNS STRING
LANGUAGE PYTHON
IMPORTS = (
  '@__UDF_IMPORT_STAGE__/imports/helper.py',
  '@__UDF_IMPORT_STAGE__/imports/data.txt',
  '@__UDF_IMPORT_STAGE__/imports/subdir/extra.txt',
  '@__UDF_IMPORT_STAGE__/imports/dup_a/dup.txt',
  '@__UDF_IMPORT_STAGE__/imports/dup_b/dup.txt'
)
PACKAGES = ()
HANDLER = 'read_stage'
AS $$
import os
import sys
import helper

def read_stage() -> str:
    # Read staged files from sys._xoptions["databend_import_directory"].
    imports_dir = sys._xoptions["databend_import_directory"]
    with open(os.path.join(imports_dir, "data.txt"), "r") as f:
        content = f.read().strip()
    with open(os.path.join(imports_dir, "extra.txt"), "r") as f:
        extra = f.read().strip()
    # Same basename imports are flattened; last import wins for dup.txt.
    with open(os.path.join(imports_dir, "dup.txt"), "r") as f:
        dup = f.read().strip()
    return f"{content}:{extra}:{dup}:{helper.add_one(1)}"
$$
SQL
)
create_reader_udf_sql=${create_reader_udf_sql//__UDF_IMPORT_STAGE__/$UDF_IMPORT_STAGE}
execute_query "$create_reader_udf_sql"

echo "Executing read_stage UDF"
check_result "SELECT read_stage() AS result" '[["stage_data:extra_data:dup_b:2"]]' "$UDF_SETTINGS_JSON"
