---
title: JSON PATH
---

Databend supports [Semi-structured data type](/doc/reference/data-types/data-type-semi-structured-types) and allow retrieving the inner elements by JSON path operators:

## Syntax

### Colon Notation

Colon notation `:` is used to retrieving element of object by name: `<column>:<level1_name>:<level2_name>`.

### Dot Notation

Dot notation `.` is used to retrieving element of object by name: `<column>:<level1_name>.<level2_name>`.

:::note
Please note that dot notation cannot be used as first-level name notation to avoid confusion with dot notation between table and column.
:::

### Bracket Notation

Bracket notation `[]` is used to retrieving element of array by index: `<column>[<level1_index>][<level2_index>]` or element of object by single quoted name: `<column>['<level1_name>']['<level2_name>']`.

:::tip
These notations can be mixed in use.
:::

## Examples

```sql
CREATE TABLE test(var Variant, arr Variant);
INSERT INTO test VALUES (parse_json('{"a":{"b":1,"c":[1,2]}}'), parse_json('[["a","b"],{"k":"a"}]')),
                        (parse_json('{"a":{"b":2,"c":[3,4]}}'), parse_json('[["c","d"],{"k":"b"}]'));

-- Colon Notation
SELECT var:a:b FROM test;
+---------+
| var:a:b |
+---------+
| 1       |
| 2       |
+---------+

-- Dot Notation
SELECT var:a.c FROM test;
+---------+
| var:a.c |
+---------+
| [1,2]   |
| [3,4]   |
+---------+

-- Bracket Notation
SELECT var['a']['c'], arr[0][1] FROM test;
+---------------+-----------+
| var['a']['c'] | arr[0][1] |
+---------------+-----------+
| [1,2]         | "b"       |
| [3,4]         | "d"       |
+---------------+-----------+

-- Mixed Notations
SELECT var['a']:b, var:a['c'][0], arr[1].k FROM test;
+------------+---------------+----------+
| var['a']:b | var:a['c'][0] | arr[1].k |
+------------+---------------+----------+
| 1          | 1             | "a"      |
| 2          | 3             | "b"      |
+------------+---------------+----------+
```
