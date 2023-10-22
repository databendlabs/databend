---
title: GET_IGNORE_CASE
---

Extracts value from a `VARIANT` that contains `OBJECT` by the field_name.
The value is returned as a `Variant` or `NULL` if either of the arguments is `NULL`.

`GET_IGNORE_CASE` is similar to `GET` but applies case-insensitive matching to field names.
First match the exact same field name, if not found, match the case-insensitive field name alphabetically.

## Syntax

```sql
GET_IGNORE_CASE( <variant>, <field_name> )
```

## Arguments

| Arguments      | Description                                                      |
|----------------|------------------------------------------------------------------|
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
| 3                                                             |
+---------------------------------------------------------------+
```
