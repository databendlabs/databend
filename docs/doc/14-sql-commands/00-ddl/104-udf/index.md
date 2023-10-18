---
title: User-Defined Function
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

User-Defined Functions (UDFs) enable you to define your custom operations using anonymous lambda expressions to process data within Databend. Key features of user-defined functions include:

- Customized Data Transformations: UDFs empower you to perform data transformations that may not be achievable through built-in Databend functions alone. This customization is particularly valuable for handling unique data formats or implementing specific business logic.

- Code Reusability: UDFs can be easily reused across multiple queries, saving time and effort in coding and maintaining data processing logic.

## Managing UDFs

To manage UDFs in Databend, use the following commands:

<IndexOverviewList />

## Usage Examples

This example creates UDFs to extract specific values from JSON data within a table using SQL queries.

```sql
-- Define UDFs
CREATE FUNCTION get_v1 AS (json) -> json["v1"];
CREATE FUNCTION get_v2 AS (json) -> json["v2"];

-- Create a table
CREATE TABLE json_table(time TIMESTAMP, data JSON);

-- Insert a time event
INSERT INTO json_table VALUES('2022-06-01 00:00:00.00000', PARSE_JSON('{"v1":1.5, "v2":20.5}'));

-- Get v1 and v2 value from the event
SELECT get_v1(data), get_v2(data) FROM json_table;
+------------+------------+
| data['v1'] | data['v2'] |
+------------+------------+
| 1.5        | 20.5       |
+------------+------------+
```