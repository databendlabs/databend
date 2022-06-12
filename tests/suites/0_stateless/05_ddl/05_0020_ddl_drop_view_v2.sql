set enable_planner_v2 = 1;

DROP VIEW IF EXISTS tmp_view;
CREATE VIEW tmp_view AS SELECT number % 3 AS a, avg(number) FROM numbers(1000) GROUP BY a ORDER BY a;
DROP TABLE tmp_view; -- {ErrorCode 1054}
DROP VIEW tmp_view;
SELECT * FROM tmp_view; -- {ErrorCode 1025}
