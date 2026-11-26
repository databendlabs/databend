#!/usr/bin/env bash
# A query embedded in SET or in a SHOW filter must be checked like any query.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_user() {
	printf '%s\n' "$1" | bendsql_connect_user subquery_user password -A --output tsv --quote-style=never
}

# Prints the result, or the error code with the statement that failed.
check() {
	local output
	if output=$(run_user "$1" 2>&1); then
		printf '%s\n' "$output"
	else
		echo "error: $(sed -n 's/.*QueryFailed: //p' <<<"$output" | head -1)"
	fi
}

cleanup() {
	bendsql_connect_root_null --query="
DROP DATABASE IF EXISTS subquery_db;
DROP USER IF EXISTS subquery_user;
DROP STAGE IF EXISTS subquery_stage;
DROP FUNCTION IF EXISTS subquery_udf;
DROP TAG IF EXISTS subquery_tag;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE DATABASE subquery_db;
CREATE TABLE subquery_db.secret(v INT);
INSERT INTO subquery_db.secret VALUES (42);
CREATE VIEW subquery_db.secret_view AS SELECT v FROM subquery_db.secret;
CREATE TABLE subquery_db.visible(a INT);
CREATE STAGE subquery_stage;
COPY INTO @subquery_stage FROM subquery_db.secret FILE_FORMAT = (TYPE = CSV);
CREATE FUNCTION subquery_udf AS (p) -> (p > 0);
CREATE TAG subquery_tag;
CREATE USER subquery_user IDENTIFIED BY 'password';
GRANT SELECT ON subquery_db.visible TO subquery_user;
"

echo "== SET with a query"
check "SET VARIABLE v = (SELECT v FROM subquery_db.secret); SELECT \$v"
check "SET VARIABLE v = (SELECT v FROM subquery_db.secret_view); SELECT \$v"
check "SET VARIABLE v = (SELECT \$1 FROM @subquery_stage (FILE_FORMAT => 'CSV')); SELECT \$v"
check "SET VARIABLE v = (SELECT subquery_udf(1)); SELECT \$v"
check "SET max_threads = (SELECT v FROM subquery_db.secret); SELECT 'max_threads is set'"
check "SET VARIABLE v = (SELECT 1); SELECT \$v"

echo "== SHOW with a subquery in WHERE"
check "SHOW DATABASES WHERE name = 'subquery_db' AND (SELECT v FROM subquery_db.secret) = 42"
check "SHOW DATABASES WHERE name = 'subquery_db' AND (SELECT v FROM subquery_db.secret_view) = 42"
check "SHOW DATABASES WHERE name = 'subquery_db' AND (SELECT \$1 FROM @subquery_stage (FILE_FORMAT => 'CSV')) = '42'"
check "SHOW DATABASES WHERE name = 'subquery_db' AND subquery_udf(1)"
check "SHOW ENGINES WHERE \"Engine\" = 'MEMORY' AND (SELECT v FROM subquery_db.secret) = 42"
check "SHOW TABLE_FUNCTIONS WHERE name = 'numbers' AND (SELECT v FROM subquery_db.secret) = 42"
check "SHOW TABLES FROM subquery_db WHERE (SELECT v FROM subquery_db.secret) = 42"
check "SHOW COLUMNS FROM subquery_db.visible WHERE (SELECT v FROM subquery_db.secret) = 42"
echo "-- built-in metadata is readable without SELECT grants"
check "SHOW DATABASES WHERE name = 'subquery_db'"
check "SHOW ENGINES WHERE \"Engine\" = 'MEMORY'"
check "SELECT count(*) > 0 FROM system.engines"
check "SELECT count(*) > 0 FROM system.table_functions"

echo "== SHOW TAGS needs SUPER, and only its generated FROM is exempt"
check "SELECT count(*) FROM system.tags"
check "SHOW TAGS"
bendsql_connect_root_null --query="GRANT SUPER ON *.* TO subquery_user"
# Only the name: SHOW TAGS also prints when the tag was created.
check "SHOW TAGS WHERE name = 'subquery_tag'" | cut -f1
check "SHOW TAGS WHERE name IN (SELECT name FROM system.tags)"
check "SHOW TAGS WHERE (SELECT v FROM subquery_db.secret) = 42"
bendsql_connect_root_null --query="REVOKE SUPER ON *.* FROM subquery_user"

echo "== after granting"
bendsql_connect_root_null --query="
GRANT SELECT ON subquery_db.secret TO subquery_user;
GRANT USAGE ON UDF subquery_udf TO subquery_user;
"
check "SET VARIABLE v = (SELECT v FROM subquery_db.secret); SELECT \$v"
check "SHOW DATABASES WHERE name = 'subquery_db' AND (SELECT v FROM subquery_db.secret) = 42"
check "SHOW DATABASES WHERE name = 'subquery_db' AND subquery_udf(1)"
