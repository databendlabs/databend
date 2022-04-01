DROP VIEW IF EXISTS tmp_view;
CREATE VIEW tmp_view AS SELECT number % 3 AS a, avg(number) FROM numbers(1000) GROUP BY a ORDER BY a;
SELECT * FROM tmp_view;
ALTER VIEW tmp_view AS SELECT number from numbers(3) ORDER BY number;
SELECT * FROM tmp_view;
DROP VIEW tmp_view;
