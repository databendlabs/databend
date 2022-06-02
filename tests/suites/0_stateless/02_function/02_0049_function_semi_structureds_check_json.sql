select '==check_json==';
select check_json(null);
select check_json(true);
select check_json(123);
select check_json(12.34);
select check_json('null');
select check_json('true');
select check_json('-17');
select check_json('123.12');
select check_json('1.912e2');
select check_json('"Om ara pa ca na dhih"  ');
select check_json('[-1, 12, 289, 2188, false]');
select check_json('{ "x" : "abc", "y" : false, "z": 10} ');
select check_json('[1,');
select check_json('"ab');
select check_json(to_date('2022-01-01'));
select check_json(to_datetime('2022-01-01 20:20:20'));

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(v String null) Engine = Memory;

insert into t1 values (null),('null'),('true'),('123'),('"abc"'),('[1,2,3]'),('{"a":"b"}');

select '==check_json from table==';
select check_json(v), v from t1;

CREATE TABLE IF NOT EXISTS t2(v String null) Engine = Memory;

insert into t2 values ('abc'),('[1,');

select '==check_json from table invalid json==';
select check_json(v), v from t2;

DROP DATABASE db1;
