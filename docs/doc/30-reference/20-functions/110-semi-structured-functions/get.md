---
title: GET
---

Extracts value from an `ARRAY` by `index`, an `OBJECT` by `field_name`, or a `VARIANT` that contains either `ARRAY` or `OBJECT`.
The value is returned as a `Variant` or `NULL` if either of the arguments is `NULL`.

`GET` applies case-sensitive matching to `field_name`. For case-insensitive matching, use `GET_IGNORE_CASE`.

## Syntax

```sql
GET(<array>, <index>)
get(<variant>, <index>)

get(<object>, <field_name>)
get(<variant>, <field_name>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<array>`       | The ARRAY value
| `<object>`      | The OBJECT value
| `<variant>`     | The VARIANT value that contains either an ARRAY or an OBJECT
| `<index>`       | The Uint32 value specifies the position of the value in ARRAY  
| `<field_name>`  | The String value specifies the key in a key-value pair of OBJECT

## Return Type

VARIANT

## Examples

```sql
SELECT get(parse_json('[2.71, 3.14]'), 0);
+------------------------------------+
| get(parse_json('[2.71, 3.14]'), 0) |
+------------------------------------+
| 2.71                               |
+------------------------------------+

SELECT get(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'aa');
+---------------------------------------------------+
| get(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'aa') |
+---------------------------------------------------+
| 1                                                 |
+---------------------------------------------------+

SELECT get(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'AA');
+---------------------------------------------------+
| get(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'AA') |
+---------------------------------------------------+
| NULL                                              |
+---------------------------------------------------+
```
