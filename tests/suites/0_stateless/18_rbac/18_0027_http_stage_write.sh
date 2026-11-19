#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

url="http://${QUERY_MYSQL_HANDLER_HOST}:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage"
user=stage_write_user_0027
password=stage_write_test_password
role=stage_write_role_0027
stage=stage_write_0027
owned_stage=stage_owned_0027
file=stage_write_0027.csv
clean_user_stage=false

cleanup() {
	local status=0
	if "$clean_user_stage"; then
		bendsql_connect_user "$user" "$password" --output null --query="REMOVE @~/$file" || status=$?
	fi
	bendsql_connect_root_null --query="
DROP STAGE IF EXISTS $stage;
DROP STAGE IF EXISTS $owned_stage;
DROP USER IF EXISTS $user;
DROP ROLE IF EXISTS $role;
"
	return "$status"
}

expect_denied() {
	# No file part: check authorization without attempting to write data.
	printf -- '--stage-test--\r\n' |
		curl --silent --show-error --user "$user:$password" --request PUT \
			--header "x-databend-stage-name: $stage" \
			--header 'Content-Type: multipart/form-data; boundary=stage-test' \
			--data-binary @- --output /dev/null --write-out '%{http_code}\n' "$url"
}

expect_allowed() {
	printf '1\n' |
		curl --fail --silent --show-error --user "$user:$password" --request PUT \
			--header "$1: $2" --form "upload=@-;filename=$file" "$url" |
		jq -er --arg stage "$2" --arg file "$file" \
			'select(.stage_name == $stage and .state == "SUCCESS" and .files == [$file]) | .state'
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE USER $user IDENTIFIED BY '$password';
CREATE ROLE $role;
GRANT ROLE $role TO USER $user;
CREATE STAGE $stage;
CREATE STAGE $owned_stage;
GRANT OWNERSHIP ON STAGE $owned_stage TO ROLE $role;
"

printf 'Without a stage grant: '
expect_denied

bendsql_connect_root_null --query="GRANT READ ON STAGE $stage TO ROLE $role"
printf 'With READ only: '
expect_denied

printf 'Named stage file count before authorization: '
bendsql_connect_root --output tsv --query="SELECT count(*) FROM list_stage(location => '@$stage')"

bendsql_connect_root_null --query="GRANT WRITE ON STAGE $stage TO ROLE $role"
printf 'With WRITE: '
expect_allowed x-databend-stage-name "$stage"

printf 'With ownership (legacy header): '
expect_allowed stage_name "$owned_stage"

printf 'Named stage file sizes:\n'
bendsql_connect_root --output tsv --query="
SELECT size FROM list_stage(location => '@$stage/$file');
SELECT size FROM list_stage(location => '@$owned_stage/$file');
"

clean_user_stage=true
printf 'Own user stage: '
expect_allowed x-databend-stage-name '~'
printf 'Own user stage file size: '
bendsql_connect_user "$user" "$password" --output tsv \
	--query="SELECT size FROM list_stage(location => '@~/$file')"
