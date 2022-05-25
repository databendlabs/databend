select '==as_boolean==';
select as_boolean(parse_json('true'));
select as_boolean(parse_json('false'));

select '==as_integer==';
select as_integer(parse_json('123'));
select as_integer(parse_json('456'));

select '==as_float==';
select as_float(parse_json('12.34'));
select as_float(parse_json('56.78'));

select '==as_string==';
select as_string(parse_json('"abc"'));
select as_string(parse_json('"xyz"'));

select '==as_array==';
select as_array(parse_json('[1,2,3]'));
select as_array(parse_json('["a","b","c"]'));

select '==as_object==';
select as_object(parse_json('{"a":"b"}'));
select as_object(parse_json('{"k":123}'));

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(id Int null, v Variant null) Engine = Fuse;

insert into t1 select 1, parse_json('true');
insert into t1 select 2, parse_json('123');
insert into t1 select 3, parse_json('45.67');
insert into t1 select 4, parse_json('"abc"');
insert into t1 select 5, parse_json('[1,2,3]');
insert into t1 select 6, parse_json('{"a":"b"}');

select '==as from table==';
select id, v, as_boolean(v), as_integer(v), as_float(v), as_string(v), as_array(v), as_object(v) from t1 order by id asc;

DROP DATABASE db1;
