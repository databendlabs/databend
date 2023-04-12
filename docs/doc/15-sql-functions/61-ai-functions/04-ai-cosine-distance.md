---
title: 'COSINE_DISTANCE'
description: 'Measuring document similarity using the cosine_distance function in Databend'
---

This document provides an overview of the `cosine_distance` function in Databend and demonstrates how to measure document similarity using this function.

:::info
The `cosine_distance` function performs vector computations within Databend and does not rely on the OpenAI API.
:::

## Overview of cosine_distance

The `cosine_distance` function in Databend is a built-in function that calculates the cosine distance between two vectors. It is commonly used in natural language processing tasks, such as document similarity and recommendation systems.

Cosine distance is a measure of similarity between two vectors, based on the cosine of the angle between them. The function takes two input vectors and returns a value between 0 and 1, with 0 indicating identical vectors and 1 indicating orthogonal (completely dissimilar) vectors.

## Measuring similarity using cosine_distance

To measure document similarity using the cosine_distance function, follow the example below. This example assumes that you have already created document embeddings using the ai_embedding_vector function and stored them in a table with the `ARRAY(FLOAT32)` column type.

1. Create a table to store the documents and their embeddings:
```sql
CREATE TABLE documents (
    doc_id INT,
    text_content TEXT,
    embedding ARRAY(FLOAT32)
);

```

2. Insert example documents and their embeddings into the table:
```sql
INSERT INTO documents (doc_id, text_content, embedding)
VALUES
    (1, 'Artificial intelligence is a fascinating field.', ai_embedding_vector('Artificial intelligence is a fascinating field.')),
    (2, 'Machine learning is a subset of AI.', ai_embedding_vector('Machine learning is a subset of AI.')),
    (3, 'I love going to the beach on weekends.', ai_embedding_vector('I love going to the beach on weekends.'));
```

3. Measure the similarity between a query document and the stored documents using the `cosine_distance` function:
```sql
SELECT doc_id, text_content, cosine_distance(embedding, ai_embedding_vector('What is a subfield of artificial intelligence?')) AS distance
FROM embeddings
ORDER BY distance ASC
LIMIT 5;
```
This SQL query calculates the cosine distance between the query document's embedding and the embeddings of the stored documents. The results are ordered by ascending distance, with the smallest distance indicating the highest similarity.

Result:
```sql
+--------+-------------------------------------------------+------------+
| doc_id | text_content                                    | distance   |
+--------+-------------------------------------------------+------------+
|      1 | Artificial intelligence is a fascinating field. | 0.10928339 |
|      2 | Machine learning is a subset of AI.             | 0.13584924 |
|      3 | I love going to the beach on weekends.          | 0.30774158 |
+--------+-------------------------------------------------+------------+
```
