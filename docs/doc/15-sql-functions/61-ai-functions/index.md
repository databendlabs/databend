---
title: 'AI Functions'
---

Databend can utilize the OpenAI `Code-Davinci-002` engine to translate natural language into SQL queries.

By integrating OLAP and AI, Databend simplifies the process of writing SQL queries based on your table schema.

The `ai_to_sql` function allows us to effortlessly generate SQL queries using natural language.

## Syntax

```sql
USE <your-database>;
SELECT * FROM ai_to_sql('<prompt>', '<openai-api-key>');
```

## Example

:::note
Please note that the generated SQL query may need to be adapted to match Databend's syntax and functionality, as it might be based on PostgreSQL-standard SQL.
:::


```sql
CREATE DATABASE openai;
USE openai;

CREATE TABLE users(
    id INT,
    name VARCHAR,
    age INT
);

SELECT * FROM ai_to_sql('List all users older than 30 years', '<openai-api-key>');
```

Output:
```sql
*************************** 1. row ***************************
     database: simple_example
generated_sql:  SELECT * FROM users WHERE age > 30;
```
