echo "Creating zip/whl/egg UDF"
create_pkg_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION read_pkg()
RETURNS STRING
LANGUAGE PYTHON
IMPORTS = ('@__UDF_IMPORT_STAGE__/imports/lib.zip', '@__UDF_IMPORT_STAGE__/imports/lib.whl', '@__UDF_IMPORT_STAGE__/imports/lib.egg')
PACKAGES = ()
HANDLER = 'read_pkg'
AS $$
import zip_mod
import whl_mod
import egg_mod

def read_pkg() -> str:
    return f"{zip_mod.VALUE}-{whl_mod.VALUE}-{egg_mod.VALUE}"
$$
SQL
)
create_pkg_udf_sql=${create_pkg_udf_sql//__UDF_IMPORT_STAGE__/$UDF_IMPORT_STAGE}
execute_query "$create_pkg_udf_sql"

echo "Executing zip/whl/egg UDF"
check_result "SELECT read_pkg() AS result" '[["zip-whl-egg"]]' "$UDF_SETTINGS_JSON"
