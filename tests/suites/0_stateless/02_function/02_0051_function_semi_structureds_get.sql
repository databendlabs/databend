select '==get==';
select get(parse_json('[2.71, 3.14]'), 0);
select get(parse_json('[2.71, 3.14]'), 2);
select get(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'aA');
select get(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'AA');

select '==get_ignore_case==';
select get_ignore_case(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'aA');
select get_ignore_case(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'AA');

select '==get_path==';
select get_path(parse_json('{"attr":[{"name":1}, {"name":2}]}'), 'attr[0].name');
select get_path(parse_json('{"attr":[{"name":1}, {"name":2}]}'), 'attr[1]:name');
select get_path(parse_json('{"customer":{"id":1, "name":"databend", "extras":["ext", "test"]}}'), 'customer:id');
select get_path(parse_json('{"customer":{"id":1, "name":"databend", "extras":["ext", "test"]}}'), 'customer.name');
select get_path(parse_json('{"customer":{"id":1, "name":"databend", "extras":["ext", "test"]}}'), 'customer["extras"][0]');
select get_path(parse_json('{"customer":{"id":1, "name":"databend", "extras":["ext", "test"]}}'), 'customer["extras"][2]');
select get_path(parse_json('{"customer":{"id":1, "name":"databend", "extras":["ext", "test"]}}'), ''); -- {ErrorCode 1005}

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(id Int null, arr Array null) Engine = Memory;

insert into t1 select 1, parse_json('[1,2,3,["a","b","c"]]');

CREATE TABLE IF NOT EXISTS t2(id Int null, obj Object null) Engine = Memory;

insert into t2 select 1, parse_json('{"a":1,"b":{"c":2}}');

select '==get from table==';
select get(arr, 0) from t1;
select get(arr, 'a') from t1;
select get(obj, 0) from t2;
select get(obj, 'a') from t2;

select '==get_ignore_case from table==';
select get_ignore_case(obj, 'a') from t2;
select get_ignore_case(obj, 'A') from t2;

select '==get_path from table==';
select get_path(arr, '[0]') from t1;
select get_path(arr, '[3][0]') from t1;
select get_path(obj, 'a') from t2;
select get_path(obj, '["a"]') from t2;
select get_path(obj, 'b.c') from t2;
select get_path(obj, '["b"]["c"]') from t2;

DROP DATABASE db1;
