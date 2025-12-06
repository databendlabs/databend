#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

comment "prepare row access policy rbac environment"
stmt "SET GLOBAL enable_experimental_row_access_policy = 1"
stmt "DROP TABLE IF EXISTS rap_rbac"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_region"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_dept"
stmt "DROP USER IF EXISTS rap_create"
stmt "DROP USER IF EXISTS rap_apply"
stmt "DROP ROLE IF EXISTS role_rap_apply"
stmt "DROP ROLE IF EXISTS role_rap_create"

stmt "CREATE TABLE rap_rbac(id INT, region STRING, dept STRING)"
stmt "CREATE USER rap_create IDENTIFIED BY '123' with default_role='role_rap_create'"
stmt "CREATE USER rap_apply IDENTIFIED BY '123' with default_role='role_rap_apply'"
stmt "CREATE ROLE role_rap_apply"
stmt "GRANT ROLE role_rap_apply TO rap_apply"
stmt "CREATE ROLE role_rap_create"
stmt "GRANT ROLE role_rap_create TO rap_create"

export USER_RAP_CREATE="bendsql --user=rap_create --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT} --quote-style=never"
export USER_RAP_APPLY="bendsql --user=rap_apply --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT} --quote-style=never"

echo "SET enable_experimental_row_access_policy = 1;" | $USER_RAP_CREATE
echo "SET enable_experimental_row_access_policy = 1;" | $USER_RAP_APPLY

comment "create privilege requires CREATE ROW ACCESS POLICY"
echo "CREATE ROW ACCESS POLICY rap_region AS (region STRING) RETURNS boolean -> region = 'APAC';" | $USER_RAP_CREATE
stmt "GRANT CREATE ROW ACCESS POLICY ON *.* TO USER rap_create"
stmt "GRANT CREATE ROW ACCESS POLICY ON *.* TO ROLE role_rap_create"
echo "CREATE ROW ACCESS POLICY rap_region AS (region STRING) RETURNS boolean -> region = 'APAC';" | $USER_RAP_CREATE
stmt "CREATE ROW ACCESS POLICY rap_dept AS (dept STRING) RETURNS boolean -> dept = 'security';"

comment "apply privilege requires APPLY on row access policy"
stmt "GRANT ALTER ON default.rap_rbac TO ROLE role_rap_apply"
stmt "ALTER TABLE rap_rbac ADD ROW ACCESS POLICY rap_region ON (region)"
echo "ALTER TABLE rap_rbac DROP ROW ACCESS POLICY rap_region;" | $USER_RAP_APPLY
stmt "GRANT APPLY ON ROW ACCESS POLICY rap_region TO ROLE role_rap_apply"
echo "ALTER TABLE rap_rbac DROP ROW ACCESS POLICY rap_region;" | $USER_RAP_APPLY
echo "ALTER TABLE rap_rbac ADD ROW ACCESS POLICY rap_region ON (region);" | $USER_RAP_APPLY
echo "DESC ROW ACCESS POLICY rap_region;" | $USER_RAP_APPLY | grep rap_region | wc -l
stmt "GRANT OWNERSHIP ON ROW ACCESS POLICY rap_region TO ROLE role_rap_apply"
stmt "SHOW GRANTS ON ROW ACCESS POLICY rap_region" | grep role_rap_apply | wc -l

stmt "ALTER TABLE rap_rbac DROP ROW ACCESS POLICY rap_region"
echo "ALTER TABLE rap_rbac ADD ROW ACCESS POLICY rap_dept ON (dept);" | $USER_RAP_APPLY
stmt "GRANT APPLY ROW ACCESS POLICY ON *.* TO ROLE role_rap_apply"
echo "ALTER TABLE rap_rbac ADD ROW ACCESS POLICY rap_dept ON (dept);" | $USER_RAP_APPLY
stmt "SHOW GRANTS ON ROW ACCESS POLICY rap_dept" | grep rap_dept | wc -l
echo "DESC ROW ACCESS POLICY rap_dept;" | $USER_RAP_APPLY | grep rap_dept | wc -l

comment "cleanup row access policy rbac environment"
stmt "DROP TABLE IF EXISTS rap_rbac"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_region"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_dept"
stmt "DROP USER IF EXISTS rap_create"
stmt "DROP USER IF EXISTS rap_apply"
stmt "DROP ROLE IF EXISTS role_rap_apply"
stmt "DROP ROLE IF EXISTS role_rap_create"
