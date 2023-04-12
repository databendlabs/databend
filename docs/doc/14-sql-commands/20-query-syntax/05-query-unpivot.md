---
title: UNPIVOT
---

The `UNPIVOT` operation rotates a table by transforming columns into rows. 

It is a relational operator that accepts two columns (from a table or subquery), along with a list of columns, and generates a row for each column specified in the list. In a query, it is specified in the FROM clause after the table name or subquery.

**See also:**
[PIVOT](./05-query-pivot.md)


## Syntax

```sql
SELECT ...
FROM ...
    UNPIVOT ( <value_column>
    FOR <name_column> IN ( <column_list> ) )

[ ... ]
```

Where:
* `<value_column>`: The column that will store the values extracted from the columns listed in `<column_list>`.
* `<name_column>`: The column that will store the names of the columns from which the values were extracted.
* `<column_list>`: The list of columns to be unpivoted, separated by commas.


## Examples

Let's unpivot the individual month columns to return a single sales value by month for each employee:

### Creating and Inserting Data


```sql
-- Create the unpivoted_monthly_sales table
CREATE TABLE unpivoted_monthly_sales(
  empid INT, 
  jan INT,
  feb INT,
  mar INT,
  apr INT
);

-- Insert sales data
INSERT INTO unpivoted_monthly_sales VALUES
  (1, 10400,  8000, 11000, 18000),
  (2, 39500, 90700, 12000,  5300);
```

### Using UNPIVOT


```sql
SELECT *
FROM unpivoted_monthly_sales
    UNPIVOT (amount
    FOR month IN (jan, feb, mar, apr));
```

Output:
```sql
+-------+-------+--------+
| empid | month | amount |
+-------+-------+--------+
|     1 | jan   |  10400 |
|     1 | feb   |   8000 |
|     1 | mar   |  11000 |
|     1 | apr   |  18000 |
|     2 | jan   |  39500 |
|     2 | feb   |  90700 |
|     2 | mar   |  12000 |
|     2 | apr   |   5300 |
+-------+-------+--------+
```