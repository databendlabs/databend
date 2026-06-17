#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

comment "Test: Data masking WHERE security and constant folding"

comment "setup: create table, roles, users, masking policy"
stmt "DROP TABLE IF EXISTS default.mask_test"
stmt "DROP MASKING POLICY IF EXISTS mask_variant_sensitive"
stmt "DROP USER IF EXISTS mask_admin_user"
stmt "DROP USER IF EXISTS mask_reader_user"
stmt "DROP ROLE IF EXISTS mask_admin"
stmt "DROP ROLE IF EXISTS mask_reader"

stmt "CREATE ROLE mask_admin"
stmt "CREATE ROLE mask_reader"
stmt "CREATE USER mask_admin_user IDENTIFIED BY '123' WITH DEFAULT_ROLE='mask_admin'"
stmt "CREATE USER mask_reader_user IDENTIFIED BY '123' WITH DEFAULT_ROLE='mask_reader'"
stmt "GRANT ROLE mask_admin TO mask_admin_user"
stmt "GRANT ROLE mask_reader TO mask_reader_user"

stmt "CREATE TABLE default.mask_test(id INT, data VARIANT)"
stmt "INSERT INTO default.mask_test VALUES (1, '{\"name\":\"alice\",\"content\":\"secret\",\"age\":30}'), (2, '{\"name\":\"bob\",\"content\":\"private\",\"age\":25}')"

stmt "CREATE MASKING POLICY mask_variant_sensitive AS (val VARIANT) RETURNS VARIANT -> CASE WHEN current_role() IN ('mask_admin', 'account_admin') THEN val ELSE object_delete(val, 'content') END"
stmt "ALTER TABLE default.mask_test MODIFY COLUMN data SET MASKING POLICY mask_variant_sensitive"

stmt "GRANT SELECT ON default.mask_test TO ROLE mask_admin"
stmt "GRANT SELECT ON default.mask_test TO ROLE mask_reader"

ADMIN_CONNECT="bendsql_connect_user mask_admin_user 123 --quote-style=never"
READER_CONNECT="bendsql_connect_user mask_reader_user 123 --quote-style=never"

comment "=== Constant folding verification ==="

comment "test 1: admin EXPLAIN shows no IF, no object_delete (folded to column ref)"
echo "EXPLAIN SELECT data FROM default.mask_test WHERE id = 1;" | $ADMIN_CONNECT | grep -c "if("
echo "EXPLAIN SELECT data FROM default.mask_test WHERE id = 1;" | $ADMIN_CONNECT | grep -c "object_delete"

comment "test 2: reader EXPLAIN shows no IF, has object_delete (folded to else branch)"
echo "EXPLAIN SELECT data FROM default.mask_test WHERE id = 1;" | $READER_CONNECT | grep -c "if("
echo "EXPLAIN SELECT data FROM default.mask_test WHERE id = 1;" | $READER_CONNECT | grep -c "object_delete"

comment "=== Data correctness ==="

comment "test 3: admin sees full data"
echo "SELECT data FROM default.mask_test ORDER BY id;" | $ADMIN_CONNECT

comment "test 4: reader sees masked data (no content field)"
echo "SELECT data FROM default.mask_test ORDER BY id;" | $READER_CONNECT

comment "=== WHERE security: masked column cannot be exploited ==="

comment "test 5: WHERE IS NOT NULL on masked field returns 0 rows"
echo "SELECT id FROM default.mask_test WHERE data['content'] IS NOT NULL;" | $READER_CONNECT

comment "test 6: WHERE equality on masked field returns 0 rows"
echo "SELECT id FROM default.mask_test WHERE data['content'] = 'secret';" | $READER_CONNECT

comment "test 7: WHERE via CAST LIKE cannot detect hidden field"
echo "SELECT id FROM default.mask_test WHERE data::STRING LIKE '%content%';" | $READER_CONNECT

comment "=== Access bypass prevention ==="

comment "test 8: subfield access returns NULL"
echo "SELECT data['content'] AS c FROM default.mask_test ORDER BY id;" | $READER_CONNECT

comment "test 9: json_object_keys does not leak hidden field"
echo "SELECT json_object_keys(data) AS keys FROM default.mask_test WHERE id = 1;" | $READER_CONNECT

comment "test 10: CAST does not bypass masking"
echo "SELECT data::STRING AS s FROM default.mask_test ORDER BY id;" | $READER_CONNECT

comment "cleanup"
stmt "ALTER TABLE default.mask_test MODIFY COLUMN data UNSET MASKING POLICY"
stmt "DROP TABLE IF EXISTS default.mask_test"
stmt "DROP MASKING POLICY IF EXISTS mask_variant_sensitive"
stmt "DROP USER IF EXISTS mask_admin_user"
stmt "DROP USER IF EXISTS mask_reader_user"
stmt "DROP ROLE IF EXISTS mask_admin"
stmt "DROP ROLE IF EXISTS mask_reader"
