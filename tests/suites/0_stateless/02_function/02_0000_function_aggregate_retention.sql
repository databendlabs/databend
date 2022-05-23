DROP TABLE IF EXISTS retention_test;

CREATE TABLE retention_test(date DATE, uid INT);

INSERT INTO retention_test SELECT '2018-08-06', number FROM numbers(80);
INSERT INTO retention_test SELECT '2018-08-07', number FROM numbers(50);
INSERT INTO retention_test SELECT '2018-08-08', number FROM numbers(60);

INSERT INTO retention_test VALUES ('2018-08-07', 999);

SELECT sum(r[0]) as r1, sum(r[1]) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date = '2018-08-06' or date = '2018-08-07' GROUP BY uid);
SELECT sum(r[0]) as r1, sum(r[1]) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-08') AS r FROM retention_test WHERE date = '2018-08-06' or date = '2018-08-08' GROUP BY uid);
SELECT sum(r[0]) as r1, sum(r[1]) as r2, sum(r[2]) as r3 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07', date = '2018-08-08') AS r FROM retention_test GROUP BY uid);

SELECT uid, retention(date = '2018-08-06', date = '2018-08-07', date = '2018-08-08') AS r FROM retention_test WHERE uid = 999 GROUP BY uid;

SELECT uid FROM (SELECT uid,retention(date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date = '2018-08-06' OR date = '2018-08-07' GROUP BY uid) WHERE r[0] = 1 ORDER BY uid desc limit 1;

DROP TABLE retention_test;
