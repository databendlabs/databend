---
title: OBJECT_KEYS
---

Returns an array containing the list of keys in the input object.


## Syntax

```sql
OBJECT_KEYS(<object>)
OBJECT_KEYS(<variant>)
```

## Arguments

| Arguments         | Description                               |
| ----------------- | ----------------------------------------- |
| `<object>`  | The OBJECT value                          |
| `<variant>` | The VARIANT value that contains an OBJECT |

## Return Type

Array`<String>`

## Examples

```sql
CREATE TABLE IF NOT EXISTS objects_test1(id TINYINT, obj OBJECT, var VARIANT);

insert into objects_test1 values (1, parse_json('{"a": 1, "b": [1,2,3]}'), parse_json('{"1": 2}'));
insert into objects_test1 values (2, parse_json('{"b": [2,3,4]}'), parse_json('{"c": "d"}'));

select id, object_keys(obj), object_keys(var) from objects_test1;

+------+------------------+------------------+
| id   | object_keys(obj) | object_keys(var) |
+------+------------------+------------------+
|    1 | ['a', 'b']       | ['1']            |
|    2 | ['b']            | ['c']            |
+------+------------------+------------------+

drop table objects_test1;
```