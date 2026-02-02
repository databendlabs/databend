#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

comment "prepare masking policy rbac environment"
stmt "DROP TABLE IF EXISTS mp_rbac"
stmt "DROP MASKING POLICY IF EXISTS mask_phone"
stmt "DROP MASKING POLICY IF EXISTS mask_email"
stmt "DROP USER IF EXISTS mask_create"
stmt "DROP USER IF EXISTS mask_apply"
stmt "DROP USER IF EXISTS mask_desc"
stmt "DROP ROLE IF EXISTS role_mask_apply"
stmt "DROP ROLE IF EXISTS role_mask_create"

stmt "CREATE TABLE mp_rbac(id INT, phone STRING, email STRING)"
stmt "CREATE USER mask_create IDENTIFIED BY '123' with default_role='role_mask_create'"
stmt "CREATE USER mask_apply IDENTIFIED BY '123' with default_role='role_mask_apply'"
stmt "CREATE ROLE role_mask_apply"
stmt "GRANT ROLE role_mask_apply TO mask_apply"
stmt "CREATE ROLE role_mask_create"
stmt "GRANT ROLE role_mask_create TO mask_create"
stmt "CREATE USER mask_desc IDENTIFIED BY '123' with default_role='mask_desc'"

export USER_MASK_DESC="bendsql -A --user=mask_desc --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT} --quote-style=never"

export USER_MASK_CREATE="bendsql -A --user=mask_create --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT} --quote-style=never"
export USER_MASK_APPLY="bendsql -A --user=mask_apply --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT} --quote-style=never"

comment "create privilege requires CREATE MASKING POLICY"
echo "CREATE MASKING POLICY mask_phone AS (val STRING) RETURNS STRING -> concat('***', right(val, 2));" | $USER_MASK_CREATE
stmt "GRANT CREATE MASKING POLICY ON *.* TO role role_mask_create"
echo "CREATE MASKING POLICY mask_phone AS (val STRING) RETURNS STRING -> concat('***', right(val, 2));" | $USER_MASK_CREATE
stmt "CREATE MASKING POLICY mask_email AS (val STRING) RETURNS STRING -> 'EMAIL'"
#expect failure for user without privileges
echo "DESC MASKING POLICY mask_phone; " | $USER_MASK_APPLY
echo "DESC MASKING POLICY mask_phone;" | $USER_MASK_CREATE | grep mask_phone | wc -l

comment "apply privilege requires APPLY on masking policy"
stmt "GRANT ALTER ON default.mp_rbac TO ROLE role_mask_apply"
stmt "ALTER TABLE mp_rbac MODIFY COLUMN phone SET MASKING POLICY mask_phone"
# expect failure
echo "ALTER TABLE mp_rbac MODIFY COLUMN phone UNSET MASKING POLICY; " | $USER_MASK_APPLY 2>&1 | grep 1063 |wc -l
stmt "GRANT APPLY ON MASKING POLICY mask_phone TO ROLE role_mask_apply"
echo "ALTER TABLE mp_rbac MODIFY COLUMN phone UNSET MASKING POLICY;" | $USER_MASK_APPLY
echo "ALTER TABLE mp_rbac MODIFY COLUMN phone SET MASKING POLICY mask_phone;" | $USER_MASK_APPLY
echo "DESC MASKING POLICY mask_phone;" | $USER_MASK_APPLY  | grep mask_phone | wc -l
stmt "GRANT OWNERSHIP ON MASKING POLICY mask_phone TO ROLE role_mask_apply"
stmt "SHOW GRANTS ON MASKING POLICY mask_phone" | grep role_mask_apply |wc -l

## expect failure
echo "ALTER TABLE mp_rbac MODIFY COLUMN email SET MASKING POLICY mask_email" | $USER_MASK_APPLY
stmt "GRANT APPLY MASKING POLICY ON *.* TO ROLE role_mask_apply"
echo "ALTER TABLE mp_rbac MODIFY COLUMN email SET MASKING POLICY mask_email;" | $USER_MASK_APPLY
stmt "SHOW GRANTS ON MASKING POLICY mask_email" | grep mask_email|wc -l
echo "DESC MASKING POLICY mask_email;" | $USER_MASK_APPLY | grep mask_email|wc -l

comment "cleanup masking policy rbac environment"
stmt "DROP TABLE IF EXISTS mp_rbac"
stmt "DROP MASKING POLICY IF EXISTS mask_phone"
stmt "DROP MASKING POLICY IF EXISTS mask_email"
stmt "DROP USER IF EXISTS mask_create"
stmt "DROP USER IF EXISTS mask_apply"
stmt "DROP USER IF EXISTS mask_desc"
stmt "DROP ROLE IF EXISTS role_mask_apply"
