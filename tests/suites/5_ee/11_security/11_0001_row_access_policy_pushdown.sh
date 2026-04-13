#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

comment "Test: Row access policy should not block filter/limit pushdown"

comment "setup: enable row access policy and create test table"
stmt "SET GLOBAL enable_experimental_row_access_policy = 1"
stmt "DROP TABLE IF EXISTS rap_pushdown_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pushdown_policy"

stmt "CREATE TABLE rap_pushdown_test(id INT, name STRING, region STRING)"
stmt "INSERT INTO rap_pushdown_test VALUES (1, 'alice', 'APAC'), (2, 'bob', 'EMEA'), (3, 'carol', 'APAC')"

comment "create row access policy"
stmt "CREATE ROW ACCESS POLICY rap_pushdown_policy AS (region STRING) RETURNS boolean -> region = 'APAC'"
stmt "ALTER TABLE rap_pushdown_test ADD ROW ACCESS POLICY rap_pushdown_policy ON (region)"

comment "test 1: filter pushdown with RAP - both user and secure predicates in push downs"
comment "EXPLAIN should show filters pushed down to TableScan (not empty)"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "filters: \[\]" | sed 's/^[[:space:]]*//g'
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep "row access policy: APPLIED" | sed 's/^[[:space:]]*//g'

comment "test 2: limit pushdown with RAP (with WHERE clause)"
comment "Note: limit is NOT pushed down because sort rule runs before filter pushdown, so push_down_predicates is still empty"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g' || true

comment "test 3: verify SecureFilter node is eliminated and RAP is on Scan"
comment "SecureFilter should NOT appear in the plan; secure predicates are in Scan's filters"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -c "SecureFilter" || true

comment "test 4: limit should NOT be pushed down when RAP exists (no user WHERE)"
comment "This is a safety check - limit without filter predicates could cause incorrect results"
echo "EXPLAIN SELECT * FROM rap_pushdown_test LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep "limit: NONE" | sed 's/^[[:space:]]*//g'
echo "EXPLAIN SELECT * FROM rap_pushdown_test LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "row access policy: APPLIED" | sed 's/^[[:space:]]*//g'

comment "test 5: sort (ORDER BY + LIMIT) should NOT push limit when RAP exists"
comment "This is a safety check - TopK pruning could cause incorrect results"
echo "EXPLAIN SELECT * FROM rap_pushdown_test ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep "limit: NONE" | sed 's/^[[:space:]]*//g'
echo "EXPLAIN SELECT * FROM rap_pushdown_test ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "row access policy: APPLIED" | sed 's/^[[:space:]]*//g'

comment "test 6: sort + filter pushdown with RAP"
comment "Note: limit is NOT pushed down because sort rule runs before filter pushdown"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "filters: \\[\\]" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g' || true

comment "test 7: EXPLAIN output should not leak the raw RAP expression"
if echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN"
    exit 1
else
    echo "EXPLAIN redacts RAP expression"
fi

comment "test 8: EXPLAIN ANALYZE output should not leak the raw RAP expression"
if echo "EXPLAIN ANALYZE SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN ANALYZE"
    exit 1
else
    echo "EXPLAIN ANALYZE redacts RAP expression"
fi

comment "test 9: EXPLAIN PERF output should not leak the raw RAP expression"
if echo "EXPLAIN PERF SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN PERF"
    exit 1
else
    echo "EXPLAIN PERF redacts RAP expression"
fi

comment "cleanup"
stmt "ALTER TABLE rap_pushdown_test DROP ROW ACCESS POLICY rap_pushdown_policy"
stmt "DROP TABLE IF EXISTS rap_pushdown_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pushdown_policy"
