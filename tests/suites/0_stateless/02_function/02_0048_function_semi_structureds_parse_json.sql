select '==parse_json==';
select parse_json(null);
select parse_json(true);
select parse_json(123);
select parse_json(12.34);
select parse_json('null');
select parse_json('true');
select parse_json('-17');
select parse_json('123.12');
select parse_json('1.912e2');
select parse_json('"Om ara pa ca na dhih"  ');
select parse_json('[-1, 12, 289, 2188, false]');
select parse_json('{ "x" : "abc", "y" : false, "z": 10} ');
select parse_json('[1,'); -- {ErrorCode 1010}
select parse_json('"ab'); -- {ErrorCode 1010}
select parse_json(parse_json('123'));
select parse_json(parse_json('"\\\"abc\\\""'));
select parse_json(parse_json('"abc"')); -- {ErrorCode 1010}
select parse_json(todate32('2022-01-01')); -- {ErrorCode 1010}

select '==try_parse_json==';
select try_parse_json(null);
select try_parse_json(true);
select try_parse_json(123);
select try_parse_json(12.34);
select try_parse_json('null');
select try_parse_json('true');
select try_parse_json('-17');
select try_parse_json('123.12');
select try_parse_json('1.912e2');
select try_parse_json('"Om ara pa ca na dhih"  ');
select try_parse_json('[-1, 12, 289, 2188, false]');
select try_parse_json('{ "x" : "abc", "y" : false, "z": 10} ');
select try_parse_json('[1,');
select try_parse_json('"ab');
select try_parse_json(parse_json('123'));
select try_parse_json(parse_json('"\\\"abc\\\""'));
select try_parse_json(parse_json('"abc"'));
select try_parse_json(todate32('2022-01-01'));

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(v String) Engine = Memory;

insert into t1 values (null),('null'),('true'),('123'),('"abc"'),('[1,2,3]'),('{"a":"b"}');

select '==parse_json from table==';
select parse_json(v), v from t1;
select '==try_parse_json from table==';
select try_parse_json(v), v from t1;

CREATE TABLE IF NOT EXISTS t2(v String) Engine = Memory;

insert into t2 values ('abc'),('[1,');

select '==parse_json from table invalid json==';
select parse_json(v), v from t2; -- {ErrorCode 1010}
select '==try_parse_json from table invalid json==';
select try_parse_json(v), v from t2;

DROP DATABASE db1;
