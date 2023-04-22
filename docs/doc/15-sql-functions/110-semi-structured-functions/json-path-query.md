---
title: JSON_PATH_QUERY
---

Get all JSON items returned by JSON path for the specified JSON value.

## Syntax

```sql
JSON_PATH_QUERY(<variant>, '<path_name>')
```


## Return Type

VARIANT

## Example

**Create a Table and Insert Sample Data**

```sql
CREATE TABLE products (
    name VARCHAR,
    details VARIANT
);

INSERT INTO products (name, details)
VALUES ('Laptop', '{"brand": "Dell", "colors": ["Black", "Silver"], "price": 1200, "features": {"ram": "16GB", "storage": "512GB"}}'),
       ('Smartphone', '{"brand": "Apple", "colors": ["White", "Black"], "price": 999, "features": {"ram": "4GB", "storage": "128GB"}}'),
       ('Headphones', '{"brand": "Sony", "colors": ["Black", "Blue", "Red"], "price": 150, "features": {"battery": "20h", "bluetooth": "5.0"}}');
```

**Query Demo: Extracting All Features from Product Details**

```sql
SELECT
    name,
    JSON_PATH_QUERY(details, '$.features.*') AS all_features
FROM
    products;
```

**Result**

```sql
+------------+--------------+
| name       | all_features |
+------------+--------------+
| Laptop     | "16GB"       |
| Laptop     | "512GB"      |
| Smartphone | "4GB"        |
| Smartphone | "128GB"      |
| Headphones | "20h"        |
| Headphones | "5.0"        |
+------------+--------------+
```