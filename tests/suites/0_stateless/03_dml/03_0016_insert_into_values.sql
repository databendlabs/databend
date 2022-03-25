DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8 null, b UInt32 null, c DateTime32 null, d String null) Engine = Memory;


INSERT INTO t1 (a,b,c,d) VALUES(-1, 33, '2021-08-15 10:00:00', 'string1234'),
                                       (101, 67, '2021-11-15 10:00:00', 'string5678');

select * from t1;
select sum(a),sum(b) from t1;


CREATE TABLE IF NOT EXISTS t2(id Int null, var Variant null) Engine = Memory;

INSERT INTO t2 (id, var) VALUES(1, null),
                               (2, true),
                               (3, false),
                               (4, 1),
                               (5, -1),
                               (6, 1000),
                               (7, -1000),
                               (8, 9223372036854775807),
                               (9, -9223372036854775808),
                               (10, 18446744073709551615),
                               (11, 0.12345679),
                               (12, 0.12345678912121212);

select * from t2;

DROP DATABASE db1;
