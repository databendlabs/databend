---
title: Semi-structured Data Types
description: Semi-structured Types can hold any other data types.
---

| Data Type | Syntax   |
| ----------| -------- |
| Array     | Array
| Object    | Object
| Variant   | Variant


## Array

Array in Databend is similar to an array in many other programming languages, but the value in an Array types can be different, each value in an Array is Variant type.

### Example

```text title='mysql>'
create table array_table(arr array null);
```

```text title='mysql>'
desc array_table;
```

Insert a value into the table, `[1,2,3,["a","b","c"]]`:
```text title='mysql>'
insert into array_table values(parse_json('[1,2,3,["a","b","c"]]'));
```

Get the element at index 0 of the array:
```text title='mysql>'
select arr[0] from array_table;
```
```
+----------------------+
| get_path(arr, '[0]') |
+----------------------+
| 1                    |
+----------------------+
```

Get the element at index 3 of the array:
```text title='mysql>'
select arr[3] from array_table;
```

```text
+----------------------+
| get_path(arr, '[3]') |
+----------------------+
| ["a","b","c"]        |
+----------------------+
```

`arr[3]` value is a Array type too, we can get the sub elements like:
```text title='mysql>'
select arr[3][0] from array_table;
```

```text
+-------------------------+
| get_path(arr, '[3][0]') |
+-------------------------+
| "a"                     |
+-------------------------+
```

## Object

Databend Object is a data type likes a "dictionary”, “hash”, or “map” in other programming languages.
An Object contains key-value pairs.

### Example

Create a table with Object type:
```text title='mysql>'
create table object_table(obj object null);
```

Desc the `object_table`:
```text
desc object_table;
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
insert into object_table values(parse_json('{"a":1,"b":{"c":2}}'));
```

Get the value by key `a`:
```text title='mysql>'
select obj:a from object_table;
```

```text
+--------------------+
| get_path(obj, 'a') |
+--------------------+
| 1                  |
+--------------------+
```

Get the value by key `b`:

```text title='mysql>'
select obj:b from object_table;
```

```text
+--------------------+
| get_path(obj, 'b') |
+--------------------+
| {"c":2}            |
+--------------------+
```

Get the sub value by the key `b:c`:
```text title='mysql>'
select obj:b:c from object_table;
```

```text
+----------------------+
| get_path(obj, 'b:c') |
+----------------------+
| 2                    |
+----------------------+
```
## Variant

A Variant can store a value of any other type, including Array and Object.

### Example

Create a table:
```text title='mysql>'
create table variant_table(var variant null);
```

Insert a value with different type into the table:
```text title='mysql>'
insert into variant_table values(1),(1.34),(true),(parse_json('[1,2,3,["a","b","c"]]')),(parse_json('{"a":1,"b":{"c":2}}'));
```

Query the result:
```text title='mysql>'
 select * from variant_table;
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
select arr[0]::Int32 from array_table;
```

```text
+-------------------------------------+
| cast(get_path(arr, '[0]') as Int32) |
+-------------------------------------+
|                                   1 |
+-------------------------------------+
```

Let's do a more complex query:
```text
select sum(arr[0]::Int32) from array_table group by arr[0]::Int32;
```

```text
+------------------------------------------+
| sum(cast(get_path(arr, '[0]') as Int32)) |
+------------------------------------------+
|                                        1 |
+------------------------------------------+
```
