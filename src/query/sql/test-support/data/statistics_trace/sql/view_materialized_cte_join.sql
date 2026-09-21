settings (enable_auto_materialize_cte = 1)
WITH high_value AS (
    SELECT o_custkey, SUM(o_total) AS total
    FROM statistics_trace_recent_orders
    GROUP BY o_custkey
)
SELECT trace_region_norm(c.c_region) AS region, h1.total + h2.total AS combined_total
FROM statistics_trace_customers AS c
     INNER JOIN high_value AS h1 ON c.c_custkey = h1.o_custkey
     INNER JOIN high_value AS h2 ON c.c_custkey = h2.o_custkey
WHERE c.c_region IN ('ASIA', 'EUROPE')
