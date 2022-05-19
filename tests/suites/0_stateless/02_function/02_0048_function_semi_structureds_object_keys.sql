CREATE TABLE IF NOT EXISTS objects_test1(id TINYINT, obj OBJECT, var VARIANT) Engine=Memory;

insert into objects_test1 values (1, parse_json('{"a": 1, "b": [1,2,3]}'), parse_json('{"1": 2}'));
insert into objects_test1 values (2, parse_json('{"b": [2,3,4]}'), parse_json('{"c": "d"}'));

select id, object_keys(obj), object_keys(var) from objects_test1;

drop table objects_test1;

select object_keys(parse_json('[1,2,3]')); -- {ErrorCode 1010}
select object_keys(1); -- {ErrorCode 1010}