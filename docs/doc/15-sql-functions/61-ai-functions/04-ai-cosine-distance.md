---
title: 'COSINE_DISTANCE'
description: 'Measuring similarity using the cosine_distance function in Databend'
---

This document provides an overview of the cosine_distance function in Databend and demonstrates how to measure document similarity using this function.

:::info

The cosine_distance function performs vector computations within Databend and does not rely on the (Azure) OpenAI API. 

:::

The cosine_distance function in Databend is a built-in function that calculates the cosine distance between two vectors. It is commonly used in natural language processing tasks, such as document similarity and recommendation systems.

Cosine distance is a measure of similarity between two vectors, based on the cosine of the angle between them. The function takes two input vectors and returns a value between 0 and 1, with 0 indicating identical vectors and 1 indicating orthogonal (completely dissimilar) vectors.

## Examples

**Creating a Table and Inserting Sample Data**

Let's create a table to store some sample text documents and their corresponding embeddings:
```sql
CREATE TABLE articles (
    id INT,
    title VARCHAR,
    content VARCHAR,
    embedding ARRAY(FLOAT32)
);
```

Now, let's insert some sample documents into the table:
```sql
INSERT INTO articles (id, title, content, embedding)
VALUES
    (1, 'Python for Data Science', 'Python is a versatile programming language widely used in data science...', ai_embedding_vector('Python is a versatile programming language widely used in data science...')),
    (2, 'Introduction to R', 'R is a popular programming language for statistical computing and graphics...', ai_embedding_vector('R is a popular programming language for statistical computing and graphics...')),
    (3, 'Getting Started with SQL', 'Structured Query Language (SQL) is a domain-specific language used for managing relational databases...', ai_embedding_vector('Structured Query Language (SQL) is a domain-specific language used for managing relational databases...'));
```

**Querying for Similar Documents**

Now, let's find the documents that are most similar to a given query using the cosine_distance function:
```sql
SELECT
    id,
    title,
    content,
    cosine_distance(embedding, ai_embedding_vector('How to use Python in data analysis?')) AS similarity
FROM
    articles
ORDER BY
    similarity ASC
    LIMIT 3;
```

Result:
```sql
+------+--------------------------+---------------------------------------------------------------------------------------------------------+------------+
| id   | title                    | content                                                                                                 | similarity |
+------+--------------------------+---------------------------------------------------------------------------------------------------------+------------+
|    1 | Python for Data Science  | Python is a versatile programming language widely used in data science...                               |  0.1142081 |
|    2 | Introduction to R        | R is a popular programming language for statistical computing and graphics...                           | 0.18741018 |
|    3 | Getting Started with SQL | Structured Query Language (SQL) is a domain-specific language used for managing relational databases... | 0.25137568 |
+------+--------------------------+---------------------------------------------------------------------------------------------------------+------------+
```