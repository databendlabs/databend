DROP TABLE IF EXISTS retention_test;

CREATE TABLE retention_test(date DATE, uid INT);

INSERT INTO retention_test SELECT '2018-08-06', number FROM numbers(80);
INSERT INTO retention_test SELECT '2018-08-07', number FROM numbers(50);
INSERT INTO retention_test SELECT '2018-08-08', number FROM numbers(60);

SELECT sum(r[0]::TINYINT) as r1, sum(r[1]::TINYINT) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date = '2018-08-06' or date = '2018-08-07' GROUP BY uid);
SELECT sum(r[0]::TINYINT) as r1, sum(r[1]::TINYINT) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-08') AS r FROM retention_test WHERE date = '2018-08-06' or date = '2018-08-08' GROUP BY uid);
SELECT sum(r[0]::TINYINT) as r1, sum(r[1]::TINYINT) as r2, sum(r[2]::TINYINT) as r3 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07', date = '2018-08-08') AS r FROM retention_test GROUP BY uid);

DROP TABLE retention_test;
