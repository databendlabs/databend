WITH filtered_orders AS (
    SELECT o_orderkey, o_custkey
    FROM orders
    WHERE o_orderdate >= CAST('1993-10-01' AS date)
      AND o_orderdate < CAST('1994-01-01' AS date)
),
returned AS (
    SELECT l_orderkey, SUM(l_quantity) AS return_qty
    FROM lineitem
    WHERE l_returnflag = 'R'
    GROUP BY l_orderkey
)
SELECT c.c_nationkey, COUNT(*) AS order_count, SUM(r.return_qty) AS total_qty
FROM customer AS c
     INNER JOIN filtered_orders AS o ON c.c_custkey = o.o_custkey
     INNER JOIN returned AS r ON r.l_orderkey = o.o_orderkey
WHERE c.c_name LIKE 'Customer%'
GROUP BY c.c_nationkey
HAVING COUNT(*) > 0
ORDER BY c.c_nationkey
LIMIT 5
