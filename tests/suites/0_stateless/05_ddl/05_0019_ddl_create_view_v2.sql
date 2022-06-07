set enable_planner_v2 = 1;

DROP VIEW IF EXISTS tmp_view;
DROP VIEW IF EXISTS tmp_view2;
CREATE VIEW tmp_view AS SELECT number % 3 AS a, avg(number) FROM numbers(1000) GROUP BY a ORDER BY a;
CREATE VIEW tmp_view AS SELECT 1; -- {ErrorCode 2306}
CREATE VIEW tmp_view2 AS SELECT * FROM numbers(100);
DROP VIEW IF EXISTS tmp_view;
DROP VIEW IF EXISTS tmp_view2;
