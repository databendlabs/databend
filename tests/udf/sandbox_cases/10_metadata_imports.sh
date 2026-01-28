echo "Creating python UDF"
create_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION add_one(INT)
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@__UDF_IMPORT_STAGE__/imports/helper.py')
PACKAGES = ()
HANDLER = 'add_one'
AS $$
# This metadata dependency is required for sandbox UDFs too; without parsing it
# the cloud path misses the package and raises ModuleNotFoundError. TOML format
# /// script
# dependencies = ["humanize==4.9.0"]
# ///
import helper
import humanize

def add_one(x: int) -> int:
    humanize.intcomma(x)
    return helper.add_one(x)
$$
SQL
)
create_udf_sql=${create_udf_sql//__UDF_IMPORT_STAGE__/$UDF_IMPORT_STAGE}
execute_query "$create_udf_sql"

echo "Executing metadata imports UDF"
check_result "SELECT add_one(1) AS result" '[["2"]]' "$UDF_SETTINGS_JSON"
