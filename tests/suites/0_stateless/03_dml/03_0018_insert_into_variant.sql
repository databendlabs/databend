DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

select '==Variant==';

CREATE TABLE IF NOT EXISTS t1(id Int null, var Variant null) Engine = Fuse;

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

INSERT INTO t1 SELECT 13, parse_json('"abcd"');
INSERT INTO t1 SELECT 14, parse_json('[1,2,3]');
INSERT INTO t1 SELECT 15, parse_json('{"k":"v"}');

select * from t1 order by id asc;

select '==Array==';

CREATE TABLE IF NOT EXISTS t2(id Int null, arr Array null) Engine = Fuse;

INSERT INTO t2 SELECT 1, parse_json('[1,2,3,["a","b","c"],{"k":"v"}]');

select * from t2;
select arr[0] from t2;
select arr[5] from t2;
select arr[3][0] from t2;
select arr[4]["k"] from t2;
select arr[4][0] from t2;

select '==Object==';

CREATE TABLE IF NOT EXISTS t3(id Int null, obj Object null) Engine = Fuse;

INSERT INTO t3 SELECT 1, parse_json('["a","b","c"]');  -- {ErrorCode 1010}
INSERT INTO t3 SELECT 1, parse_json('{"a":1,"b":{"k":2},"c":[10,11,12]}');

select * from t3;
select obj:a from t3;
select obj["a"] from t3;
select obj[0] from t3;
select obj:x from t3;
select obj:b from t3;
select obj:b:k from t3;
select obj:b.k from t3;
select obj:c from t3;
select obj:c[0] from t3;
select obj["c"][0] from t3;
select obj["c"][3] from t3;

select '==Json==';

CREATE TABLE IF NOT EXISTS t4(id Int null, j Json null) Engine = Fuse;

INSERT INTO t4 (id, j) VALUES(1, null),
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

INSERT INTO t4 SELECT 13, parse_json('"abcd"');
INSERT INTO t4 SELECT 14, parse_json('[1,2,3]');
INSERT INTO t4 SELECT 15, parse_json('{"k":"v"}');

select * from t4 order by id asc;

select '==Map==';

CREATE TABLE IF NOT EXISTS t5(id Int null, m Map null) Engine = Fuse;

INSERT INTO t5 SELECT 1, parse_json('["a","b","c"]');  -- {ErrorCode 1010}
INSERT INTO t5 SELECT 1, parse_json('{"a":1,"b":{"k":2},"c":[10,11,12]}');

select * from t5;

DROP DATABASE db1;
