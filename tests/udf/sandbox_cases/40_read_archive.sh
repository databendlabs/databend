echo "Creating archive reader UDF"
create_archive_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION read_archive()
RETURNS STRING
LANGUAGE PYTHON
IMPORTS = ('@__UDF_IMPORT_STAGE__/imports/archive.tar.gz')
PACKAGES = ()
HANDLER = 'read_archive'
AS $$
import os
import sys
import tarfile

def read_archive() -> str:
    # Access tar.gz imports via sys._xoptions["databend_import_directory"].
    imports_dir = sys._xoptions["databend_import_directory"]
    archive_path = os.path.join(imports_dir, "archive.tar.gz")
    with tarfile.open(archive_path, "r:gz") as tf:
        member = tf.extractfile("archive.txt")
        content = member.read().decode("utf-8").strip()
    return content
$$
SQL
)
create_archive_udf_sql=${create_archive_udf_sql//__UDF_IMPORT_STAGE__/$UDF_IMPORT_STAGE}
execute_query "$create_archive_udf_sql"

echo "Executing read_archive UDF"
check_result "SELECT read_archive() AS result" '[["archive_data"]]' "$UDF_SETTINGS_JSON"
