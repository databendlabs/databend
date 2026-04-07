-- =============================================================
-- Nested View Performance Benchmark
-- Issue: https://github.com/databendlabs/databend/issues/17514
-- Usage: cat tests/nested_view_benchmark.sql | bendsql
-- =============================================================

DROP DATABASE IF EXISTS view_bench;
CREATE DATABASE view_bench;
USE view_bench;

-- =============================================================
-- Base tables (small dataset, focus on planning overhead)
-- =============================================================

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    product_id INT,
    amount DECIMAL(10,2),
    order_date DATE
);

CREATE TABLE customers (
    customer_id INT,
    name VARCHAR(100),
    region VARCHAR(50)
);

CREATE TABLE products (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);

INSERT INTO customers
SELECT
    number,
    CONCAT('Customer_', number::VARCHAR),
    CASE number % 4 WHEN 0 THEN 'East' WHEN 1 THEN 'West' WHEN 2 THEN 'North' ELSE 'South' END
FROM numbers(200);

INSERT INTO products
SELECT
    number,
    CONCAT('Product_', number::VARCHAR),
    CASE number % 5 WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Books' WHEN 2 THEN 'Clothing' WHEN 3 THEN 'Food' ELSE 'Tools' END,
    (number % 100 + 1) * 9.99
FROM numbers(100);

INSERT INTO orders
SELECT
    number,
    number % 200,
    number % 100,
    (number % 500 + 1) * 1.5,
    TO_DATE('2024-01-01') + (number % 365)
FROM numbers(5000);

-- =============================================================
-- Group A: JOIN chain (each layer adds a JOIN)
-- =============================================================

-- View chain
CREATE VIEW va1 AS
SELECT o.order_id, o.amount, o.order_date, c.name, c.region
FROM orders o JOIN customers c ON o.customer_id = c.customer_id;

CREATE VIEW va2 AS
SELECT va1.*, p.product_name, p.category, p.price
FROM va1 JOIN products p ON va1.order_id % 100 = p.product_id;

CREATE VIEW va3 AS
SELECT va2.*, c2.name AS ref_customer, c2.region AS ref_region
FROM va2 JOIN customers c2 ON (va2.order_id + 1) % 200 = c2.customer_id;

CREATE VIEW va4 AS
SELECT va3.*, p2.product_name AS alt_product, p2.category AS alt_category
FROM va3 JOIN products p2 ON (va3.order_id + 10) % 100 = p2.product_id;

CREATE VIEW va5 AS
SELECT va4.*, c3.name AS third_customer
FROM va4 JOIN customers c3 ON (va4.order_id + 50) % 200 = c3.customer_id
WHERE va4.amount > 10 AND va4.region = 'East';

-- Equivalent FUSE table chain
CREATE TABLE ta1 AS
SELECT o.order_id, o.amount, o.order_date, c.name, c.region
FROM orders o JOIN customers c ON o.customer_id = c.customer_id;

CREATE TABLE ta2 AS
SELECT ta1.*, p.product_name, p.category, p.price
FROM ta1 JOIN products p ON ta1.order_id % 100 = p.product_id;

CREATE TABLE ta3 AS
SELECT ta2.*, c2.name AS ref_customer, c2.region AS ref_region
FROM ta2 JOIN customers c2 ON (ta2.order_id + 1) % 200 = c2.customer_id;

CREATE TABLE ta4 AS
SELECT ta3.*, p2.product_name AS alt_product, p2.category AS alt_category
FROM ta3 JOIN products p2 ON (ta3.order_id + 10) % 100 = p2.product_id;

CREATE TABLE ta5 AS
SELECT ta4.*, c3.name AS third_customer
FROM ta4 JOIN customers c3 ON (ta4.order_id + 50) % 200 = c3.customer_id
WHERE ta4.amount > 10 AND ta4.region = 'East';

-- =============================================================
-- Group B: Aggregation + JOIN (each layer aggregates then joins back)
-- =============================================================

-- View chain
CREATE VIEW vb1 AS
SELECT c.region, COUNT(*) AS order_cnt, SUM(o.amount) AS total_amount, AVG(o.amount) AS avg_amount
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region;

CREATE VIEW vb2 AS
SELECT o.order_id, o.amount, c.region, vb1.order_cnt, vb1.total_amount,
       o.amount / vb1.avg_amount AS amount_ratio
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN vb1 ON c.region = vb1.region;

CREATE VIEW vb3 AS
SELECT vb2.region, vb2.order_cnt,
       COUNT(*) AS high_ratio_cnt,
       AVG(vb2.amount_ratio) AS avg_ratio,
       MAX(vb2.amount) AS max_amount
FROM vb2
WHERE vb2.amount_ratio > 0.5
GROUP BY vb2.region, vb2.order_cnt;

CREATE VIEW vb4 AS
SELECT o.order_id, o.amount, c.region,
       vb3.high_ratio_cnt, vb3.avg_ratio, vb3.max_amount,
       o.amount / vb3.max_amount AS pct_of_max
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN vb3 ON c.region = vb3.region;

CREATE VIEW vb5 AS
SELECT vb4.region,
       COUNT(*) AS final_cnt,
       SUM(vb4.amount) AS final_total,
       AVG(vb4.pct_of_max) AS avg_pct_of_max,
       MAX(vb4.avg_ratio) AS max_avg_ratio
FROM vb4
WHERE vb4.pct_of_max < 0.8
GROUP BY vb4.region;

-- Equivalent FUSE table chain
CREATE TABLE tb1 AS
SELECT c.region, COUNT(*) AS order_cnt, SUM(o.amount) AS total_amount, AVG(o.amount) AS avg_amount
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region;

CREATE TABLE tb2 AS
SELECT o.order_id, o.amount, c.region, tb1.order_cnt, tb1.total_amount,
       o.amount / tb1.avg_amount AS amount_ratio
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN tb1 ON c.region = tb1.region;

CREATE TABLE tb3 AS
SELECT tb2.region, tb2.order_cnt,
       COUNT(*) AS high_ratio_cnt,
       AVG(tb2.amount_ratio) AS avg_ratio,
       MAX(tb2.amount) AS max_amount
FROM tb2
WHERE tb2.amount_ratio > 0.5
GROUP BY tb2.region, tb2.order_cnt;

CREATE TABLE tb4 AS
SELECT o.order_id, o.amount, c.region,
       tb3.high_ratio_cnt, tb3.avg_ratio, tb3.max_amount,
       o.amount / tb3.max_amount AS pct_of_max
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN tb3 ON c.region = tb3.region;

CREATE TABLE tb5 AS
SELECT tb4.region,
       COUNT(*) AS final_cnt,
       SUM(tb4.amount) AS final_total,
       AVG(tb4.pct_of_max) AS avg_pct_of_max,
       MAX(tb4.avg_ratio) AS max_avg_ratio
FROM tb4
WHERE tb4.pct_of_max < 0.8
GROUP BY tb4.region;

-- =============================================================
-- Group C: Window functions + nesting
-- =============================================================

-- View chain
CREATE VIEW vc1 AS
SELECT o.order_id, o.customer_id, o.amount, o.order_date, c.region,
       ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY o.amount DESC) AS rank_in_region,
       SUM(o.amount) OVER (PARTITION BY c.region) AS region_total
FROM orders o JOIN customers c ON o.customer_id = c.customer_id;

CREATE VIEW vc2 AS
SELECT vc1.*,
       LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
       LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount,
       AVG(amount) OVER (PARTITION BY customer_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM vc1
WHERE rank_in_region <= 1000;

CREATE VIEW vc3 AS
SELECT vc2.*,
       NTILE(10) OVER (PARTITION BY region ORDER BY amount) AS decile,
       amount - COALESCE(prev_amount, amount) AS amount_change,
       PERCENT_RANK() OVER (PARTITION BY region ORDER BY moving_avg) AS pct_rank
FROM vc2;

CREATE VIEW vc4 AS
SELECT vc3.region, vc3.decile,
       COUNT(*) AS cnt,
       AVG(vc3.amount) AS avg_amount,
       AVG(vc3.amount_change) AS avg_change,
       AVG(vc3.pct_rank) AS avg_pct_rank,
       SUM(vc3.region_total) AS sum_region_total
FROM vc3
GROUP BY vc3.region, vc3.decile;

CREATE VIEW vc5 AS
SELECT vc4.*,
       RANK() OVER (PARTITION BY region ORDER BY avg_amount DESC) AS amount_rank,
       SUM(cnt) OVER (PARTITION BY region ORDER BY decile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cnt
FROM vc4;

-- Equivalent FUSE table chain
CREATE TABLE tc1 AS
SELECT o.order_id, o.customer_id, o.amount, o.order_date, c.region,
       ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY o.amount DESC) AS rank_in_region,
       SUM(o.amount) OVER (PARTITION BY c.region) AS region_total
FROM orders o JOIN customers c ON o.customer_id = c.customer_id;

CREATE TABLE tc2 AS
SELECT tc1.*,
       LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
       LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount,
       AVG(amount) OVER (PARTITION BY customer_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM tc1
WHERE rank_in_region <= 1000;

CREATE TABLE tc3 AS
SELECT tc2.*,
       NTILE(10) OVER (PARTITION BY region ORDER BY amount) AS decile,
       amount - COALESCE(prev_amount, amount) AS amount_change,
       PERCENT_RANK() OVER (PARTITION BY region ORDER BY moving_avg) AS pct_rank
FROM tc2;

CREATE TABLE tc4 AS
SELECT tc3.region, tc3.decile,
       COUNT(*) AS cnt,
       AVG(tc3.amount) AS avg_amount,
       AVG(tc3.amount_change) AS avg_change,
       AVG(tc3.pct_rank) AS avg_pct_rank,
       SUM(tc3.region_total) AS sum_region_total
FROM tc3
GROUP BY tc3.region, tc3.decile;

CREATE TABLE tc5 AS
SELECT tc4.*,
       RANK() OVER (PARTITION BY region ORDER BY avg_amount DESC) AS amount_rank,
       SUM(cnt) OVER (PARTITION BY region ORDER BY decile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cnt
FROM tc4;

-- =============================================================
-- Benchmark queries
-- =============================================================

-- Group A: JOIN chain
SELECT '--- Group A: JOIN chain ---';
SELECT '  View va5:';
SELECT COUNT(*), SUM(amount) FROM va5;
SELECT '  Inline SQL (equivalent to va5, no view mechanism):';
SELECT COUNT(*), SUM(amount) FROM (
    SELECT t4.*, c3.name AS third_customer
    FROM (
        SELECT t3.*, p2.product_name AS alt_product, p2.category AS alt_category
        FROM (
            SELECT t2.*, c2.name AS ref_customer, c2.region AS ref_region
            FROM (
                SELECT t1.*, p.product_name, p.category, p.price
                FROM (
                    SELECT o.order_id, o.amount, o.order_date, c.name, c.region
                    FROM orders o JOIN customers c ON o.customer_id = c.customer_id
                ) t1 JOIN products p ON t1.order_id % 100 = p.product_id
            ) t2 JOIN customers c2 ON (t2.order_id + 1) % 200 = c2.customer_id
        ) t3 JOIN products p2 ON (t3.order_id + 10) % 100 = p2.product_id
    ) t4 JOIN customers c3 ON (t4.order_id + 50) % 200 = c3.customer_id
    WHERE t4.amount > 10 AND t4.region = 'East'
);
SELECT '  Table ta5:';
SELECT COUNT(*), SUM(amount) FROM ta5;

-- Group B: Aggregation + JOIN
SELECT '--- Group B: Aggregation + JOIN ---';
SELECT '  View vb5:';
SELECT * FROM vb5 ORDER BY region;
SELECT '  Inline SQL (equivalent to vb5, no view mechanism):';
SELECT region,
       COUNT(*) AS final_cnt,
       SUM(amount) AS final_total,
       AVG(pct_of_max) AS avg_pct_of_max,
       MAX(avg_ratio) AS max_avg_ratio
FROM (
    SELECT o.order_id, o.amount, c.region,
           sub3.high_ratio_cnt, sub3.avg_ratio, sub3.max_amount,
           o.amount / sub3.max_amount AS pct_of_max
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN (
        SELECT sub2.region, sub2.order_cnt,
               COUNT(*) AS high_ratio_cnt,
               AVG(sub2.amount_ratio) AS avg_ratio,
               MAX(sub2.amount) AS max_amount
        FROM (
            SELECT o2.order_id, o2.amount, c2.region, sub1.order_cnt, sub1.total_amount,
                   o2.amount / sub1.avg_amount AS amount_ratio
            FROM orders o2
            JOIN customers c2 ON o2.customer_id = c2.customer_id
            JOIN (
                SELECT c1.region, COUNT(*) AS order_cnt, SUM(o1.amount) AS total_amount, AVG(o1.amount) AS avg_amount
                FROM orders o1 JOIN customers c1 ON o1.customer_id = c1.customer_id
                GROUP BY c1.region
            ) sub1 ON c2.region = sub1.region
        ) sub2
        WHERE sub2.amount_ratio > 0.5
        GROUP BY sub2.region, sub2.order_cnt
    ) sub3 ON c.region = sub3.region
) sub4
WHERE pct_of_max < 0.8
GROUP BY region
ORDER BY region;
SELECT '  Table tb5:';
SELECT * FROM tb5 ORDER BY region;

-- Group C: Window functions
SELECT '--- Group C: Window functions ---';
SELECT '  View vc5:';
SELECT * FROM vc5 ORDER BY region, decile;
SELECT '  Inline SQL (equivalent to vc5, no view mechanism):';
SELECT sub4.*,
       RANK() OVER (PARTITION BY region ORDER BY avg_amount DESC) AS amount_rank,
       SUM(cnt) OVER (PARTITION BY region ORDER BY decile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cnt
FROM (
    SELECT sub3.region, sub3.decile,
           COUNT(*) AS cnt,
           AVG(sub3.amount) AS avg_amount,
           AVG(sub3.amount_change) AS avg_change,
           AVG(sub3.pct_rank) AS avg_pct_rank,
           SUM(sub3.region_total) AS sum_region_total
    FROM (
        SELECT sub2.*,
               NTILE(10) OVER (PARTITION BY region ORDER BY amount) AS decile,
               amount - COALESCE(prev_amount, amount) AS amount_change,
               PERCENT_RANK() OVER (PARTITION BY region ORDER BY moving_avg) AS pct_rank
        FROM (
            SELECT sub1.*,
                   LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
                   LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount,
                   AVG(amount) OVER (PARTITION BY customer_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
            FROM (
                SELECT o.order_id, o.customer_id, o.amount, o.order_date, c.region,
                       ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY o.amount DESC) AS rank_in_region,
                       SUM(o.amount) OVER (PARTITION BY c.region) AS region_total
                FROM orders o JOIN customers c ON o.customer_id = c.customer_id
            ) sub1
            WHERE rank_in_region <= 1000
        ) sub2
    ) sub3
    GROUP BY sub3.region, sub3.decile
) sub4
ORDER BY region, decile;
SELECT '  Table tc5:';
SELECT * FROM tc5 ORDER BY region, decile;

-- Bonus: EXPLAIN to compare plans
SELECT '--- EXPLAIN: View va5 ---';
EXPLAIN SELECT COUNT(*), SUM(amount) FROM va5;
SELECT '--- EXPLAIN: View vb5 ---';
EXPLAIN SELECT * FROM vb5 ORDER BY region;
SELECT '--- EXPLAIN: View vc5 ---';
EXPLAIN SELECT * FROM vc5 ORDER BY region, decile;

-- =============================================================
-- Cleanup
-- =============================================================
-- DROP DATABASE view_bench;
