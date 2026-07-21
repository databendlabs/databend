SELECT c1.c_nationkey, COUNT(*) AS pair_count
FROM customer AS c1
     INNER JOIN customer AS c2
         ON c1.c_nationkey = c2.c_nationkey
        AND c1.c_custkey < c2.c_custkey
WHERE c2.c_name LIKE 'Customer%'
GROUP BY c1.c_nationkey
HAVING COUNT(*) > 1
