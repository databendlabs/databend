DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

select '==Variant==';

CREATE TABLE IF NOT EXISTS t1(id Int null, var Variant null) Engine = Memory;

INSERT INTO t1 (id, var) VALUES(1, null),
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

select * from t1;

CREATE TABLE IF NOT EXISTS t2(id Int null, var Variant null) Engine = Memory;

INSERT INTO t2 SELECT 13, parse_json('"abcd"');

select * from t2;

select '==Array==';

CREATE TABLE IF NOT EXISTS t3(id Int null, arr Array null) Engine = Memory;

INSERT INTO t3 SELECT 1, parse_json('[1,2,3,4]');

select * from t3;

select '==Object==';

CREATE TABLE IF NOT EXISTS t4(id Int null, obj Object null) Engine = Memory;

INSERT INTO t4 SELECT 1, parse_json('["a","b","c"]');  -- {ErrorCode 1010}
INSERT INTO t4 SELECT 1, parse_json('{"a":1,"b":2}');

select * from t4;

DROP DATABASE db1;
