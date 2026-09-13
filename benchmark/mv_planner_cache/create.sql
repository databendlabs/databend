DROP DATABASE IF EXISTS mv_planner_cache;
CREATE DATABASE mv_planner_cache;
USE mv_planner_cache;

CREATE TABLE source (
    a INT,
    b INT,
    c STRING,
    t INT
);

INSERT INTO source
SELECT
    number % 100,
    number % 10,
    IF(number % 2 = 0, 'xxxxxxx', 'other'),
    number
FROM numbers(10000);

CREATE MATERIALIZED VIEW mv_by_abc (
    a,
    b,
    c,
    min_t,
    max_t,
    sum_t,
    count_t,
    avg_t,
    approx_count_distinct_t
) AS
SELECT a, b, c, min(t), max(t), sum(t), count(t), avg(t), approx_count_distinct(t)
FROM source
GROUP BY a, b, c;

REFRESH MATERIALIZED VIEW mv_by_abc;
