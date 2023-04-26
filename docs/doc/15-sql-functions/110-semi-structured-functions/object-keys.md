---
title: OBJECT_KEYS
---

Returns an Array containing the list of keys in the input Variant OBJECT.


## Syntax

```sql
OBJECT_KEYS(<variant>)
```

## Arguments

| Arguments   | Description                               |
|-------------|-------------------------------------------|
| `<variant>` | The VARIANT value that contains an OBJECT |

## Return Type

Array`<String>`

## Examples

```sql
CREATE TABLE IF NOT EXISTS objects_test1(id TINYINT, var VARIANT);

insert into objects_test1 values (1, parse_json('{"a": 1, "b": [1,2,3]}'));
insert into objects_test1 values (2, parse_json('{"b": [2,3,4]}'));

select id, object_keys(obj), object_keys(var) from objects_test1;

+------+------------------+
| id   | object_keys(var) |
+------+------------------+
|    1 | ['a', 'b']       |
|    2 | ['b']            |
+------+------------------+

drop table objects_test1;
```