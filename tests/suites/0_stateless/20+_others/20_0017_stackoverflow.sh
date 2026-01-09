#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

SQL="SELECT COUNT(1), 'numbers(10)' FROM numbers(10)"

for i in $(seq 1 1000); do
	SQL="$SQL UNION ALL SELECT COUNT(1), 'numbers(10)' FROM numbers(10)"
done

echo "SELECT * FROM ($SQL) LIMIT 1000" | $BENDSQL_CLIENT_CONNECT | wc -l

# New test case for subquery stack overflow
echo "Testing subquery stack overflow with large IN clause and EXISTS subqueries..."

# Create a simpler version of the complex query for testing
cat <<'EOF' | $BENDSQL_CLIENT_CONNECT
-- Test query with large IN clause and EXISTS subqueries
-- This is designed to test stack overflow in subquery rewriting
SELECT * FROM (
  SELECT DISTINCT 
    t1.id AS main_id,
    t1.partner_id AS related_id,
    'partner' AS relation_type
  FROM test_table_1 t1
  WHERE t1.id IN (
    'test_id_001', null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null
  )
  AND EXISTS (
    SELECT 1 
    FROM test_table_2 t2 
    WHERE t2.identification = t1.partner_id
  )
  
  UNION ALL
  
  SELECT DISTINCT
    t1.id AS main_id,
    t1.parent_id AS related_id,
    'parent' AS relation_type
  FROM test_table_1 t1
  WHERE t1.id IN (
    'test_id_001', null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null, null, null, null
  )
  AND EXISTS (
    SELECT 1 
    FROM test_table_2 t2 
    WHERE t2.identification = t1.parent_id
  )
) t
LIMIT 0, 100
EOF

echo "Subquery stack overflow test completed."
