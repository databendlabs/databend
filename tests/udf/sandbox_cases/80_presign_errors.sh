echo "Checking presign error handling"

create_missing_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION missing_import()
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@__UDF_IMPORT_STAGE__/imports/missing.py')
PACKAGES = ()
HANDLER = 'missing_import'
AS $$
def missing_import() -> int:
    return 1
$$
SQL
)
create_missing_udf_sql=${create_missing_udf_sql//__UDF_IMPORT_STAGE__/$UDF_IMPORT_STAGE}
execute_query "$create_missing_udf_sql"
execute_query_expect_error "SELECT missing_import() AS result" "Failed to stat UDF import" "$UDF_SETTINGS_JSON"

bad_stage="udf_import_stage_bad"
create_bad_stage_sql=$(
	cat <<SQL
CREATE STAGE IF NOT EXISTS ${bad_stage}
URL = 's3://testbucket/udf-imports/'
CONNECTION = (
  access_key_id = '${S3_ACCESS_KEY_ID}',
  secret_access_key = 'bad-secret',
  endpoint_url = '${S3_ENDPOINT_URL}'
);
SQL
)
execute_query "$create_bad_stage_sql"

create_bad_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION bad_creds()
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@__UDF_BAD_STAGE__/imports/helper.py')
PACKAGES = ()
HANDLER = 'bad_creds'
AS $$
def bad_creds() -> int:
    return 1
$$
SQL
)
create_bad_udf_sql=${create_bad_udf_sql//__UDF_BAD_STAGE__/$bad_stage}
execute_query "$create_bad_udf_sql"
execute_query_expect_error "SELECT bad_creds() AS result" "Failed to stat UDF import" "$UDF_SETTINGS_JSON"
