#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

comment "Test: Row access policy should participate in pushdown and prewhere"

comment "setup: enable row access policy and create test table"
stmt "SET GLOBAL enable_experimental_row_access_policy = 1"
stmt "DROP TABLE IF EXISTS rap_pushdown_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pushdown_policy"

stmt "CREATE TABLE rap_pushdown_test(id INT, name STRING, region STRING)"
stmt "INSERT INTO rap_pushdown_test VALUES (1, 'bob', 'EMEA'), (2, 'alice', 'APAC'), (3, 'carol', 'APAC')"

comment "create row access policy"
stmt "CREATE ROW ACCESS POLICY rap_pushdown_policy AS (region STRING) RETURNS boolean -> region = 'APAC'"
stmt "ALTER TABLE rap_pushdown_test ADD ROW ACCESS POLICY rap_pushdown_policy ON (region)"

comment "test 1: filter pushdown with RAP - user predicate is displayed, secure predicate is hidden"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "filters: \[\]" | sed 's/^[[:space:]]*//g'

comment "test 2: limit pushdown with RAP and user WHERE after RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 3: verify Filter [SECURE] node exists for RAP enforcement"
comment "Filter [SECURE] should appear in the plan; secure predicates are enforced by pipeline Filter"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -c "Filter \[SECURE\]"

comment "test 4: limit pushdown with RAP and no user WHERE after RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 5: sort (ORDER BY + LIMIT) pushdown with RAP after RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 6: sort + filter pushdown with RAP"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 ORDER BY id LIMIT 1;" | $BENDSQL_CLIENT_CONNECT | grep "push downs:" | grep -v "filters: \\[\\]" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 7: ORDER BY + LIMIT semantics should apply RAP before TopK result selection"
query "SELECT id, region FROM rap_pushdown_test ORDER BY id LIMIT 1"

comment "test 8: EXPLAIN output should not leak the raw RAP expression"
if echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN"
    exit 1
else
    echo "EXPLAIN redacts RAP expression"
fi

comment "test 9: EXPLAIN ANALYZE output should not leak the raw RAP expression"
if echo "EXPLAIN ANALYZE SELECT * FROM rap_pushdown_test WHERE id > 0;" | $BENDSQL_CLIENT_CONNECT | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN ANALYZE"
    exit 1
else
    echo "EXPLAIN ANALYZE redacts RAP expression"
fi

comment "test 10: EXPLAIN PERF output should not leak the raw RAP expression"
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
