---
title: Variant
---

A VARIANT can store a value of any other type, including NULL, BOOLEAN, NUMBER, STRING, ARRAY, and OBJECT, and the internal value can be any level of nested structure, which is very flexible to store various data. VARIANT can also be called JSON, for more information, please refer to [JSON website](https://www.json.org/json-en.html)

Here's an example of inserting and querying Variant data in Databend:

Create a table:
```sql
-- Create a table for storing customer orders
CREATE TABLE customer_orders(id INT64, order_data VARIANT);
```

Insert a value with different type into the table:
```sql
-- Insert sample data containing customer ID, order ID, and the list of purchased items
INSERT INTO customer_orders VALUES(1, parse_json('{"customer_id": 123, "order_id": 1001, "items": [{"name": "Shoes", "price": 59.99}, {"name": "T-shirt", "price": 19.99}]}')),
                                  (2, parse_json('{"customer_id": 456, "order_id": 1002, "items": [{"name": "Backpack", "price": 79.99}, {"name": "Socks", "price": 4.99}]}')),
                                  (3, parse_json('{"customer_id": 123, "order_id": 1003, "items": [{"name": "Shoes", "price": 59.99}, {"name": "Socks", "price": 4.99}]}'));
```

Query the result:
```sql
SELECT * FROM custom_orders;

+------+---------------------------------------------------------------------------------------------------------------+
| id   | order_data                                                                                                    |
+------+---------------------------------------------------------------------------------------------------------------+
|    1 | {"customer_id":123,"items":[{"name":"Shoes","price":59.99},{"name":"T-shirt","price":19.99}],"order_id":1001} |
|    2 | {"customer_id":456,"items":[{"name":"Backpack","price":79.99},{"name":"Socks","price":4.99}],"order_id":1002} |
|    3 | {"customer_id":123,"items":[{"name":"Shoes","price":59.99},{"name":"Socks","price":4.99}],"order_id":1003}    |
+------+---------------------------------------------------------------------------------------------------------------+
```

## Get by index

Variant contains ARRAY is a zero based array like many other programming languages, each element is also a Variant type.
Element can be accessed by its index.

### Example

In this example, we demonstrate how to access elements within a VARIANT column that contains an ARRAY.

Create the table:
```sql
-- Create a table to store user hobbies
CREATE TABLE user_hobbies(user_id INT64, hobbies VARIANT NULL);
```

Insert sample data into the table:
```sql
INSERT INTO user_hobbies VALUES(1, parse_json('["Cooking", "Reading", "Cycling"]')),
                                (2, parse_json('["Photography", "Travel", "Swimming"]'));
```

Retrieve the first hobby for each user:
```sql
SELECT user_id, hobbies[0] as first_hobby FROM user_hobbies;

+---------+-------------+
| user_id | first_hobby |
+---------+-------------+
|       1 | Cooking     |
|       2 | Photography |
+---------+-------------+
```

Retrieve the third hobby for each user:
```sql
SELECT user_id, hobbies[2] as third_hobby FROM user_hobbies;

+---------+------------+
| user_id | third_hobby|
+---------+------------+
|       1 | Cycling    |
|       2 | Swimming   |
+---------+------------+
```

Retrieve hobbies with group by:
```sql
mysql> SELECT hobbies[2], count() as third_hobby FROM user_hobbies GROUP BY hobbies[2];

+------------+-------------+
| hobbies[2] | third_hobby |
+------------+-------------+
| "Cycling"  |           1 |
| "Swimming" |           1 |
+------------+-------------+
```

## Get by field name

Variant contains OBJECT is key-value pairs, each key is a VARCHAR, and each value is a Variant. It act like a "dictionary”, “hash”, or “map” in other programming languages.
Value can be accessed by the field name.

### Example 1

Create a table to store user preferences with VARIANT type:
```sql
CREATE TABLE user_preferences(user_id INT64, preferences VARIANT NULL);
```

Insert sample data into the table:
```sql
INSERT INTO user_preferences VALUES(1, parse_json('{"color":"red", "fontSize":16, "theme":"dark"}')),
                                    (2, parse_json('{"color":"blue", "fontSize":14, "theme":"light"}'));
```

Retrieve the preferred color for each user:
```sql
SELECT user_id, preferences:color as color FROM user_preferences;
+---------+-------+
| user_id | color |
+---------+-------+
|       1 | red   |
|       2 | blue  |
+---------+-------+
```

## Data Type Conversion

By default, elements retrieved from a VARIANT column are returned. To convert a returned element to a specific type, add the `::` operator and the target data type (e.g. expression::type).

Create a table to store user preferences with a VARIANT column:
```sql
CREATE TABLE user_pref(user_id INT64, pref VARIANT NULL);
```

Insert sample data into the table:
```sql
INSERT INTO user_pref VALUES(1, parse_json('{"age": 25, "isPremium": "true", "lastActive": "2023-04-10"}')),
                             (2, parse_json('{"age": 30, "isPremium": "false", "lastActive": "2023-03-15"}'));
```

Convert the age to an INT64:
```sql
SELECT user_id, pref:age::INT64 as age FROM user_pref;

+---------+-----+
| user_id | age |
+---------+-----+
|       1 |  25 |
|       2 |  30 |
+---------+-----+
```

## Functions

See [Variant Functions](/doc/reference/functions/variant-functions).