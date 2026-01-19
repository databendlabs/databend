#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

comment "Test: SecureFilter should not block filter/limit pushdown"

comment "setup: enable row access policy and create test table"
stmt "SET GLOBAL enable_experimental_row_access_policy = 1"
stmt "DROP TABLE IF EXISTS rap_pushdown_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pushdown_policy"

stmt "CREATE TABLE rap_pushdown_test(id INT, name STRING, region STRING)"
stmt "INSERT INTO rap_pushdown_test VALUES (1, 'alice', 'APAC'), (2, 'bob', 'EMEA'), (3, 'carol', 'APAC')"

comment "create row access policy"
stmt "CREATE ROW ACCESS POLICY rap_pushdown_policy AS (region STRING) RETURNS boolean -> region = 'APAC'"
stmt "ALTER TABLE rap_pushdown_test ADD ROW ACCESS POLICY rap_pushdown_policy ON (region)"

comment "test 1: filter pushdown through SecureFilter"
comment "EXPLAIN should show filters pushed down to TableScan (not empty)"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "filters: \[\]"

comment "test 2: limit pushdown through SecureFilter (with WHERE clause)"
comment "Note: limit is NOT pushed down because sort rule runs before filter pushdown, so push_down_predicates is still empty"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "limit: NONE" || true

comment "test 3: verify SecureFilter is preserved in the plan"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -c "SecureFilter"

comment "test 4: limit should NOT be pushed down when only SecureFilter exists (no user WHERE)"
comment "This is a safety check - limit without filter predicates could cause incorrect results"
echo "EXPLAIN SELECT * FROM rap_pushdown_test LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep "limit: NONE"

comment "test 5: sort (ORDER BY + LIMIT) should NOT push limit when only SecureFilter exists"
comment "This is a safety check - TopK pruning could cause incorrect results"
echo "EXPLAIN SELECT * FROM rap_pushdown_test ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep "limit: NONE"

comment "test 6: sort + filter pushdown through SecureFilter"
comment "Note: limit is NOT pushed down because sort rule runs before filter pushdown"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "filters: \\[\\]" | grep -v "limit: NONE" || true

comment "cleanup"
stmt "ALTER TABLE rap_pushdown_test DROP ROW ACCESS POLICY rap_pushdown_policy"
stmt "DROP TABLE IF EXISTS rap_pushdown_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pushdown_policy"
