---
title: Variant
---

A VARIANT can store a value of any other type, including NULL, BOOLEAN, NUMBER, STRING, ARRAY, and OBJECT, and the internal value can be any level of nested structure, which is very flexible to store various data. VARIANT can also be called JSON, for more information, please refer to [JSON website](https://www.json.org/json-en.html)

Here's an example of inserting and querying Variant data in Databend:

Create a table:
```sql
CREATE TABLE variant_table(var VARIANT NULL);
```

Insert a value with different type into the table:
```sql
INSERT INTO variant_table VALUES(1),(1.34),(true),(parse_json('[1,2,3,["a","b","c"]]')),(parse_json('{"a":1,"b":{"c":2}}'));
```

Query the result:
```sql
SELECT * FROM variant_table;
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

## Get by index

Variant contains ARRAY is a zero based array like many other programming languages, each element is also a Variant type.
Element can be accessed by its index.

### Example

```sql
CREATE TABLE array_table(arr VARIANT NULL);
```

Desc the `array_table`:
```sql
DESC array_table;
+-------+---------+------+---------+
| Field | Type    | Null | Default |
+-------+---------+------+---------+
| arr   | Variant | YES  | NULL    |
+-------+---------+------+---------+
```

Insert a value into the table, `[1,2,3,["a","b","c"]]`:
```sql
INSERT INTO array_table VALUES(parse_json('[1,2,3,["a","b","c"]]'));
```

Get the element at index 0 of the array:
```sql
SELECT arr[0] FROM array_table;
+--------+
| arr[0] |
+--------+
| 1      |
+--------+
```

Get the element at index 3 of the array:
```sql
SELECT arr[3] FROM array_table;
+---------------+
| arr[3]        |
+---------------+
| ["a","b","c"] |
+---------------+
```

`arr[3]` value is a ARRAY type too, we can get the sub elements like:
```sql
SELECT arr[3][0] FROM array_table;
+-----------+
| arr[3][0] |
+-----------+
| "a"       |
+-----------+
```

## Get by field name

Variant contains OBJECT is key-value pairs, each key is a VARCHAR, and each value is a Variant. It act like a "dictionary”, “hash”, or “map” in other programming languages.
Value can be accessed by the field name.

### Example 1

This example shows how to access the values at each hierarchical level of a Variant:

Create a table with VARIANT type:
```sql
CREATE TABLE object_table(obj VARIANT NULL);
```

Desc the `object_table`:
```sql
DESC object_table;
+-------+---------+------+---------+
| Field | Type    | Null | Default |
+-------+---------+------+---------+
| obj   | Variant | YES  | NULL    |
+-------+---------+------+---------+
```

Insert a value into the table, `{"a":1,"b":{"c":2}}`:
```sql
INSERT INTO object_table VALUES(parse_json('{"a":1,"b":{"c":2}}'));
```

Get the value by key `a`:
```sql
SELECT obj:a FROM object_table;
+-------+
| obj:a |
+-------+
| 1     |
+-------+
```

Get the value by key `b`:

```sql
SELECT obj:b FROM object_table;
+---------+
| obj:b   |
+---------+
| {"c":2} |
+---------+
```

Get the sub value by the key `b:c`:
```sql
SELECT obj:b:c FROM object_table;
+---------+
| obj:b:c |
+---------+
| 2       |
+---------+
```

### Example 2

This example shows how to query with data of the VARIANT type:

Create a table with an VARIANT column to hold the employee's contact information including name and Email address:

```sql
CREATE TABLE employees (id INT, info VARIANT);
```

Insert two rows to the table:

```sql
INSERT INTO employees VALUES (1, parse_json('{"Email": "amy@databend.com", "Name":"Amy"}'));
INSERT INTO employees VALUES (2, parse_json('{"Email": "bob@databend.com", "Name":"Bob"}'));
```

The following statement lists all the Email addresses of the the employees with an ID smaller than 3:

```sql
SELECT info:Email FROM employees WHERE id < 3;

+------------------+
| info:Email       |
+------------------+
| amy@databend.com |
| bob@databend.com |
+------------------+
```

The following statement returns Bob's Email address by his name:

```sql
SELECT info:Email FROM employees WHERE info:Name = 'Bob';

+------------------+
| info:Email       |
+------------------+
| bob@databend.com |
+------------------+
```
The following statement returns Bob's Email address by his ID and name:

```sql
SELECT info:Email FROM employees WHERE id = 2 and info:Name = 'Bob';

+------------------+
| info:Email       |
+------------------+
| bob@databend.com |
+------------------+
```

## Data Type Conversion

By default, elements retrieved from a VARIANT column are returned. To convert a returned element to a specific type, add the `::` operator and the target data type (e.g. expression::type).

```sql
SELECT Arr[0]::INT FROM array_table
+---------------+
| arr[0]::Int32 |
+---------------+
|             1 |
+---------------+
```

Let's do a more complex query:
```sql
SELECT sum(arr[0]::INT) FROM array_table GROUP BY arr[0]::INT;
+--------------------+
| sum(arr[0]::Int32) |
+--------------------+
|                  1 |
+--------------------+
```

## Functions

See [Variant Functions](/doc/reference/functions/variant-functions).