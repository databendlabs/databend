---
title: GET_IGNORE_CASE
---

Extracts value from an `OBJECT` by `field_name`, or a `VARIANT` that contains `OBJECT`. The value is returned as a `Variant` or `NULL` if either of the arguments is `NULL`.

`GET_IGNORE_CASE` is similar to `GET` but applies case-insensitive matching to field names.

## Syntax

```sql
GET_IGNORE_CASE( <object>, <field_name> )
GET_IGNORE_CASE( <variant>, <field_name> )
```

## Arguments

| Arguments            | Description                                                      |
| -------------------- | ---------------------------------------------------------------- |
| `<object>`     | The OBJECT value                                                 |
| `<variant>`    | The VARIANT value that contains either an ARRAY or an OBJECT     |
| `<field_name>` | The String value specifies the key in a key-value pair of OBJECT |

## Return Type

VARIANT

## Examples

```sql
SELECT get_ignore_case(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'AA');
+---------------------------------------------------------------+
| get_ignore_case(parse_json('{"aa":1, "aA":2, "Aa":3}'), 'AA') |
+---------------------------------------------------------------+
| 1                                                             |
+---------------------------------------------------------------+
```
