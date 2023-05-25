---
title: 'AI_EMBEDDING_VECTOR'
description: 'Creating embeddings using the ai_embedding_vector function in Databend'
---

This document provides an overview of the ai_embedding_vector function in Databend and demonstrates how to create document embeddings using this function.

The main code implementation can be found [here](https://github.com/datafuselabs/databend/blob/1e93c5b562bd159ecb0f336bb88fd1b7f9dc4a62/src/common/openai/src/embedding.rs).

By default, Databend leverages the [text-embedding-ada](https://platform.openai.com/docs/models/embeddings) model for generating embeddings.

:::info
Starting from Databend v1.1.47, Databend supports the [Azure OpenAI service](https://azure.microsoft.com/en-au/products/cognitive-services/openai-service).

This integration offers improved data privacy.

To use Azure OpenAI, add the following configurations to the `[query]` section:
```sql
# Azure OpenAI
openai_api_chat_base_url = "https://<name>.openai.azure.com/openai/deployments/<name>/"
openai_api_embedding_base_url = "https://<name>.openai.azure.com/openai/deployments/<name>/"
openai_api_version = "2023-03-15-preview"
```
:::

:::caution
Databend relies on (Azure) OpenAI for `AI_EMBEDDING_VECTOR` and sends the embedding column data to (Azure) OpenAI.

They will only work when the Databend configuration includes the `openai_api_key`, otherwise they will be inactive.

This function is available by default on [Databend Cloud](https://databend.com) using our Azure OpenAI key. If you use them, you acknowledge that your data will be sent to Azure OpenAI by us.
:::


## Overview of ai_embedding_vector

The `ai_embedding_vector` function in Databend is a built-in function that generates vector embeddings for text data. It is useful for natural language processing tasks, such as document similarity, clustering, and recommendation systems.

The function takes a text input and returns a high-dimensional vector that represents the input text's semantic meaning and context. The embeddings are created using pre-trained models on large text corpora, capturing the relationships between words and phrases in a continuous space.

## Creating embeddings using ai_embedding_vector

To create embeddings for a text document using the `ai_embedding_vector` function, follow the example below.
1. Create a table to store the documents:
```sql
CREATE TABLE documents (
                           id INT,
                           title VARCHAR,
                           content VARCHAR,
                           embedding ARRAY(FLOAT32)
);
```

2. Insert example documents into the table:
```sql
INSERT INTO documents(id, title, content)
VALUES
    (1, 'A Brief History of AI', 'Artificial intelligence (AI) has been a fascinating concept of science fiction for decades...'),
    (2, 'Machine Learning vs. Deep Learning', 'Machine learning and deep learning are two subsets of artificial intelligence...'),
    (3, 'Neural Networks Explained', 'A neural network is a series of algorithms that endeavors to recognize underlying relationships...'),
```

3. Generate the embeddings:
```sql
UPDATE documents SET embedding = ai_embedding_vector(content) WHERE length(embedding) = 0;
```
After running the query, the embedding column in the table will contain the generated embeddings.

The embeddings are stored as an array of `FLOAT32` values in the embedding column, which has the `ARRAY(FLOAT32)` column type.

You can now use these embeddings for various natural language processing tasks, such as finding similar documents or clustering documents based on their content.

4. Inspect the embeddings:

```sql
SELECT length(embedding) FROM documents;
+-------------------+
| length(embedding) |
+-------------------+
|              1536 |
|              1536 |
|              1536 |
+-------------------+
```
The query above shows that the generated embeddings have a length of 1536(dimensions) for each document.
