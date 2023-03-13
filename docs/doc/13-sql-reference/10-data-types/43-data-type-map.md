---
title: Map
---

The MAP data structure is utilized for holding a set of `Key:Value` pairs, and stores data using a nested data structure of Array(Tuple(key, value)). It is appropriate in situations where the data type is constant, but the `Key`'s value cannot be entirely ascertained.

## Understanding Key:Value

The `Key` is of a specified basic data type, including Boolean, Number, Decimal, String, Date, or Timestamp. A `Key`'s value cannot be Null, and duplicates are not allowed. The `Value` can be any data type, including nested arrays, tuples, and so on.

Map data can be generated through `Key:Value` pairs enclosed in curly braces or by using the Map function to convert two arrays into a Map. The Map function takes two arrays as input, where the elements in the first array serve as the keys and the elements in the second array serve as the values. See an example below:

```sql
-- Input arrays: [1, 2] and ['v1', 'v2']
-- Resulting Map: {1: 'v1', 2: 'v2'}

SELECT {'k1': 1, 'k2': 2}, map([1, 2], ['v1', 'v2']);
+-----------------+---------------------------+
| {'k1':1,'k2':2} | map([1, 2], ['v1', 'v2']) |
+-----------------+---------------------------+
| {'k1':1,'k2':2} | {1:'v1',2:'v2'}           |
+-----------------+---------------------------+
```

## Examples

The following example creates a table that includes a Map column, then querys Map data from the table.

```sql
-- Create a table
CREATE TABLE map_table(m MAP(INT64, STRING));

DESC map_table;
+-------+--------------------+------+---------+-------+
| Field | Type               | Null | Default | Extra |
+-------+--------------------+------+---------+-------+
| m     | MAP(INT64, STRING) | NO   | {}      |       |
+-------+--------------------+------+---------+-------+

-- Insert Map data
INSERT INTO map_table VALUES({1:'a',2:'b'}), ({1:'c',3:'d',4:'e'});

SELECT * FROM map_table;
+---------------------+
| m                   |
+---------------------+
| {1:'a',2:'b'}       |
| {1:'c',3:'d',4:'e'} |
+---------------------+

-- Query Values in Map by Keys
-- NULL will be retured if Key is not found in a row.

SELECT m[1], m[3] FROM map_table;
+------+------+
| m[1] | m[3] |
+------+------+
| a    | NULL |
| c    | d    |
+------+------+
```