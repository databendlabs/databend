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
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | bendsql_connect_root | grep "push downs:" | grep -v "filters: \[\]" | sed 's/^[[:space:]]*//g'

comment "test 2: limit pushdown with RAP and user WHERE after RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 LIMIT 1;" | bendsql_connect_root | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 3: verify Filter [SECURE] is skipped when prewhere covers all secure predicates"
comment "Filter [SECURE] should NOT appear because secure predicates are fully applied by prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | bendsql_connect_root | grep -c "Filter \[SECURE\]" || true

comment "test 4: limit pushdown with RAP and no user WHERE after RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test LIMIT 1;" | bendsql_connect_root | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 5: sort (ORDER BY + LIMIT) pushdown with RAP after RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_pushdown_test ORDER BY id LIMIT 1;" | bendsql_connect_root | grep "push downs:" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 6: sort + filter pushdown with RAP"
echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0 ORDER BY id LIMIT 1;" | bendsql_connect_root | grep "push downs:" | grep -v "filters: \\[\\]" | grep -v "limit: NONE" | sed 's/^[[:space:]]*//g'

comment "test 7: ORDER BY + LIMIT semantics should apply RAP before TopK result selection"
query "SELECT id, region FROM rap_pushdown_test ORDER BY id LIMIT 1"

comment "test 8: EXPLAIN output should not leak the raw RAP expression"
if echo "EXPLAIN SELECT * FROM rap_pushdown_test WHERE id > 0;" | bendsql_connect_root | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN"
    exit 1
else
    echo "EXPLAIN redacts RAP expression"
fi

comment "test 9: EXPLAIN ANALYZE output should not leak the raw RAP expression"
if echo "EXPLAIN ANALYZE SELECT * FROM rap_pushdown_test WHERE id > 0;" | bendsql_connect_root | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN ANALYZE"
    exit 1
else
    echo "EXPLAIN ANALYZE redacts RAP expression"
fi

comment "test 10: EXPLAIN PERF output should not leak the raw RAP expression"
if echo "EXPLAIN PERF SELECT * FROM rap_pushdown_test WHERE id > 0;" | bendsql_connect_root | grep -F "region = 'APAC'"; then
    echo "raw RAP expression leaked in EXPLAIN PERF"
    exit 1
else
    echo "EXPLAIN PERF redacts RAP expression"
fi

comment "test 11: RAP on VARIANT column enters prewhere, limit is pushed"
stmt "DROP TABLE IF EXISTS rap_vcol_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_vcol_policy"
stmt "CREATE TABLE rap_vcol_test(id INT, val VARIANT)"
stmt "INSERT INTO rap_vcol_test VALUES (1, '{\"region\":\"EMEA\"}'), (2, '{\"region\":\"APAC\"}'), (3, '{\"region\":\"APAC\"}')"
stmt "CREATE ROW ACCESS POLICY rap_vcol_policy AS (val VARIANT) RETURNS boolean -> val['region'] = 'APAC'"
stmt "ALTER TABLE rap_vcol_test ADD ROW ACCESS POLICY rap_vcol_policy ON (val)"

comment "verify limit IS pushed because RAP enters prewhere"
echo "EXPLAIN SELECT * FROM rap_vcol_test LIMIT 1;" | bendsql_connect_root | grep "push downs:" | sed 's/^[[:space:]]*//g'

comment "verify query still returns correct results with RAP enforced"
query "SELECT id FROM rap_vcol_test ORDER BY id"

stmt "ALTER TABLE rap_vcol_test DROP ROW ACCESS POLICY rap_vcol_policy"
stmt "DROP TABLE IF EXISTS rap_vcol_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_vcol_policy"

comment "test 12: multi-block ORDER BY LIMIT correctness with RAP"
stmt "DROP TABLE IF EXISTS rap_multiblock_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_multiblock_policy"
stmt "CREATE TABLE rap_multiblock_test(id INT, name STRING, region STRING)"
stmt "INSERT INTO rap_multiblock_test VALUES (1, 'x1', 'EMEA'), (2, 'x2', 'EMEA'), (3, 'x3', 'EMEA')"
stmt "INSERT INTO rap_multiblock_test VALUES (10, 'a1', 'APAC'), (11, 'a2', 'APAC'), (12, 'a3', 'APAC')"
stmt "INSERT INTO rap_multiblock_test VALUES (20, 'b1', 'APAC'), (21, 'b2', 'APAC'), (22, 'b3', 'APAC')"
stmt "CREATE ROW ACCESS POLICY rap_multiblock_policy AS (region STRING) RETURNS boolean -> region = 'APAC'"
stmt "ALTER TABLE rap_multiblock_test ADD ROW ACCESS POLICY rap_multiblock_policy ON (region)"

comment "ORDER BY id LIMIT 2: should return 10,11 not 1,2"
query "SELECT id, region FROM rap_multiblock_test ORDER BY id LIMIT 2"

comment "ORDER BY id DESC LIMIT 2: should return 22,21"
query "SELECT id, region FROM rap_multiblock_test ORDER BY id DESC LIMIT 2"

comment "ORDER BY id LIMIT 4: spans two allowed blocks"
query "SELECT id, region FROM rap_multiblock_test ORDER BY id LIMIT 4"

stmt "ALTER TABLE rap_multiblock_test DROP ROW ACCESS POLICY rap_multiblock_policy"
stmt "DROP TABLE IF EXISTS rap_multiblock_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_multiblock_policy"

comment "test 13: prove RAP expression participates in range index pruning"
stmt "DROP TABLE IF EXISTS rap_pruning_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pruning_policy"
stmt "CREATE TABLE rap_pruning_test(id INT, region STRING) row_per_block=1"
stmt "INSERT INTO rap_pruning_test VALUES (1, 'EMEA'), (2, 'EMEA'), (3, 'APAC'), (4, 'APAC')"

comment "4 rows with row_per_block=1 means 4 blocks"
comment "RAP: region = 'APAC' should prune blocks where region min/max = 'EMEA'"
stmt "CREATE ROW ACCESS POLICY rap_pruning_policy AS (region STRING) RETURNS boolean -> region = 'APAC'"
stmt "ALTER TABLE rap_pruning_test ADD ROW ACCESS POLICY rap_pruning_policy ON (region)"

comment "EXPLAIN ANALYZE should show blocks range pruning: 4 to 2 (2 EMEA blocks pruned by RAP)"
echo "EXPLAIN ANALYZE SELECT * FROM rap_pruning_test;" | bendsql_connect_root | grep "pruning stats:" | sed 's/^[[:space:]]*//g' | sed 's/ cost: [0-9]* ms//g'

stmt "ALTER TABLE rap_pruning_test DROP ROW ACCESS POLICY rap_pruning_policy"
stmt "DROP TABLE IF EXISTS rap_pruning_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pruning_policy"

comment "cleanup"
stmt "ALTER TABLE rap_pushdown_test DROP ROW ACCESS POLICY rap_pushdown_policy"
stmt "DROP TABLE IF EXISTS rap_pushdown_test"
stmt "DROP ROW ACCESS POLICY IF EXISTS rap_pushdown_policy"
