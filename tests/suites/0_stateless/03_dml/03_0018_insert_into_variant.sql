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

INSERT INTO t3 SELECT 1, parse_json('[1,2,3,["a","b","c"],{"k":"v"}]');

select * from t3;
select arr[0] from t3;
select arr[5] from t3;
select arr[3][0] from t3;
select arr[4]["k"] from t3;
select arr[4][0] from t3;

select '==Object==';

CREATE TABLE IF NOT EXISTS t4(id Int null, obj Object null) Engine = Memory;

INSERT INTO t4 SELECT 1, parse_json('["a","b","c"]');  -- {ErrorCode 1010}
INSERT INTO t4 SELECT 1, parse_json('{"a":1,"b":{"k":2},"c":[10,11,12]}');

select * from t4;
select obj:a from t4;
select obj["a"] from t4;
select obj[0] from t4;
select obj:x from t4;
select obj:b from t4;
select obj:b:k from t4;
select obj:b.k from t4;
select obj:c from t4;
select obj:c[0] from t4;
select obj["c"][0] from t4;
select obj["c"][3] from t4;

DROP DATABASE db1;
