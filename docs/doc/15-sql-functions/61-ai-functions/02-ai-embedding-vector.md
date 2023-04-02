---
title: 'AI_EMBEDDING_VECTOR'
description: 'Creating embeddings using the ai_embedding_vector function in Databend'
---

This document provides an overview of the ai_embedding_vector function in Databend and demonstrates how to create document embeddings using this function.

The main code implementation can be found [here](https://github.com/datafuselabs/databend/blob/1e93c5b562bd159ecb0f336bb88fd1b7f9dc4a62/src/common/openai/src/embedding.rs).

:::caution
Databend relies on OpenAI for `AI_EMBEDDING_VECTOR` and sends the embedding column data to OpenAI.

They will only work when the Databend configuration includes the `openai_api_key`, otherwise they will be inactive.

This function is available by default on [Databend Cloud](https://databend.com) using our self OpenAI key. If you use them, you acknowledge that your table schema will be sent to OpenAI by us.
:::

## Overview of ai_embedding_vector


The `ai_embedding_vector` function in Databend is a built-in function that generates vector embeddings for text data. It is useful for natural language processing tasks, such as document similarity, clustering, and recommendation systems.

The function takes a text input and returns a high-dimensional vector that represents the input text's semantic meaning and context. The embeddings are created using pre-trained models on large text corpora, capturing the relationships between words and phrases in a continuous space.

## Creating embeddings using ai_embedding_vector

To create embeddings for a text document using the `ai_embedding_vector` function, follow the example below.
1. Create a table to store the documents:
```sql
CREATE TABLE documents (
    doc_id INT,
    text_content TEXT
);
```

2. Insert example documents into the table:
```sql
INSERT INTO documents (doc_id, text_content)
VALUES
    (1, 'Artificial intelligence is a fascinating field.'),
    (2, 'Machine learning is a subset of AI.'),
    (3, 'I love going to the beach on weekends.');
```

3. Create a table to store the embeddings:
```sql
CREATE TABLE embeddings (
    doc_id INT,
    text_content TEXT,
    embedding ARRAY(FLOAT32)
);
```

4. Generate embeddings for the text content and store them in the embeddings table:
```sql
INSERT INTO embeddings (doc_id, text_content, embedding)
SELECT doc_id, text_content, ai_embedding_vector(text_content)
FROM documents;

```
After running these SQL queries, the embeddings table will contain the generated embeddings for each document in the documents table. The embeddings are stored as an array of `FLOAT32` values in the embedding column, which has the `ARRAY(FLOAT32)` column type.

You can now use these embeddings for various natural language processing tasks, such as finding similar documents or clustering documents based on their content.