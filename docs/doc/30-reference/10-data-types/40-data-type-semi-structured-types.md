---
title: Semi-structured
description: Semi-structured Types can hold any other data types.
---

| Data Type | Syntax   |
| ----------| -------- |
| Array     | ARRAY
| Object    | OBJECT
| Variant   | VARIANT


## Array Types

Array in Databend is similar to an array in many other programming languages, but the value in an Array types can be different, each value in an Array is Variant type.

### Example

```text title='mysql>'
CREATE TABLE array_table(arr ARRAY NULL);
```

```text title='mysql>'
DESC array_table;
```

Insert a value into the table, `[1,2,3,["a","b","c"]]`:
```text title='mysql>'
INSERT INTO array_table VALUES(parse_json('[1,2,3,["a","b","c"]]'));
```

Get the element at index 0 of the array:
```text title='mysql>'
SELECT arr[0] FROM array_table;
```
```
+--------+
| arr[0] |
+--------+
| 1      |
+--------+
```

Get the element at index 3 of the array:
```text title='mysql>'
SELECT arr[3] FROM array_table;
```

```text
+---------------+
| arr[3]        |
+---------------+
| ["a","b","c"] |
+---------------+
```

`arr[3]` value is a Array type too, we can get the sub elements like:
```text title='mysql>'
SELECT arr[3][0] FROM array_table;
```

```text
+-----------+
| arr[3][0] |
+-----------+
| "a"       |
+-----------+
```

## Object Types

Databend Object is a data type likes a "dictionary”, “hash”, or “map” in other programming languages.
An Object contains key-value pairs.

### Example

Create a table with Object type:
```text title='mysql>'
CREATE TABLE object_table(obj OBJECT NULL);
```

Desc the `object_table`:
```text
DESC object_table;
```
```text
+-------+--------+------+---------+
| Field | Type   | Null | Default |
+-------+--------+------+---------+
| obj   | Object | YES  | NULL    |
+-------+--------+------+---------+
```

Insert a value into the table, `{"a":1,"b":{"c":2}}`:
```text title='mysql>'
INSERT INTO object_table VALUES(parse_json('{"a":1,"b":{"c":2}}'));
```

Get the value by key `a`:
```text title='mysql>'
SELECT obj:a FROM object_table;
```

```text
+-------+
| obj:a |
+-------+
| 1     |
+-------+
```

Get the value by key `b`:

```text title='mysql>'
SELECT obj:b FROM object_table;
```

```text
+---------+
| obj:b   |
+---------+
| {"c":2} |
+---------+
```

Get the sub value by the key `b:c`:
```text title='mysql>'
SELECT obj:b:c FROM object_table;
```

```text
+---------+
| obj:b:c |
+---------+
| 2       |
+---------+
```
## Variant Types

A Variant can store a value of any other type, including Array and Object.

### Example

Create a table:
```text title='mysql>'
CREATE TABLE variant_table(var VARIANT NULL);
```

Insert a value with different type into the table:
```text title='mysql>'
INSERT INTO variant_table VALUES(1),(1.34),(true),(parse_json('[1,2,3,["a","b","c"]]')),(parse_json('{"a":1,"b":{"c":2}}'));
```

Query the result:
```text title='mysql>'
SELECT * FROM variant_table;
 ```
```text
+-----------------------+
| var                   |
+-----------------------+
| 1                     |
| 1.34                  |
| true                  |
| [1,2,3,["a","b","c"]] |
| {"a":1,"b":{"c":2}}   |
+-----------------------+
```

## Data Type Conversion

By default, elements retrieved from a Variant column are returned. To convert a returned element to a specific type, add the `::` operator and the target data type (e.g. expression::type).

```text
SELECT Arr[0]::INT FROM array_table
```

```text
+---------------+
| arr[0]::Int32 |
+---------------+
|             1 |
+---------------+
```

Let's do a more complex query:
```text
SELECT sum(arr[0]::INT) FROM array_table GROUP BY arr[0]::INT;
```

```text
+--------------------+
| sum(arr[0]::Int32) |
+--------------------+
|                  1 |
+--------------------+
```

## Functions

See [Semi-structured Functions](/doc/reference/functions/semi-structured-functions).