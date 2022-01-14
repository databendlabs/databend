DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a UInt8, b UInt64, c Int8, d Int64, e Date16, f Date32, g DateTime32, h String) Engine = Memory;
CREATE TABLE IF NOT EXISTS t2(a String, b String, c String, d String, e String, f String, g String, h String) Engine = Memory;
CREATE TABLE IF NOT EXISTS t3(a String, b String, c String, d String) Engine = Memory;


INSERT INTO t1 (a,b,c,d,e) select * from t3; -- {ErrorCode 1006}
INSERT INTO t1 (a,b,c,d,e) select a,b,c from t3; -- {ErrorCode 1006}

INSERT INTO t2 (a,b,c,d,e,f,g,h) VALUES('1','2','3','4','2021-08-15', '2021-09-15', '2021-08-15 10:00:00', 'string1234'),
                                       ('5','6','7','8','2021-10-15', '2021-11-15', '2021-11-15 10:00:00', 'string5678');
INSERT INTO t1 (a,b,c,d,e,f,g,h) select * from t2;
SELECT * FROM t1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

-- Aggregation test
CREATE TABLE base_table(a Int32);
CREATE TABLE aggregate_table(b Int32);

INSERT INTO base_table VALUES(1),(2),(3),(4),(5),(6);
INSERT INTO aggregate_table SELECT SUM(a) FROM base_table GROUP BY a%3;
SELECT * FROM aggregate_table ORDER BY b;

DROP TABLE base_table;
DROP TABLE aggregate_table;

DROP DATABASE db1;
