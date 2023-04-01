---
title: 'AI Functions'
description: 'SQL-based Knowledge Base Search and Completion using Databend'
---

This document demonstrates how to leverage Databend's built-in AI functions for creating document embeddings, searching for similar documents, and generating text completions based on context. 

We will guide you through a simple example that shows how to create and store embeddings using the `ai_embedding_vector` function, find related documents with the `cosine_distance` function, and generate completions using the `ai_text_completion` function.

## Introduction to embeddings

Embeddings are vector representations of text data that capture the semantic meaning and context of the original text. They can be used to compare and analyze text in various natural language processing tasks, such as document similarity, clustering, and recommendation.

## How do embeddings work?

Embeddings work by converting text into high-dimensional vectors in such a way that similar texts are closer together in the vector space. This is achieved by training a model on a large corpus of text, which learns to represent the words and phrases in a continuous space that captures their semantic relationships.
Embeddings are vector representations of text data that capture the semantic meaning and context of the original text. They are widely used in various natural language processing tasks, such as document similarity, clustering, and recommendation systems.

## Databend AI Functions

Databend provides built-in AI functions for various natural language processing tasks. The main functions covered in this document are:

- [ai_embedding_vector](./02-ai-embedding-vector.md): Generates embeddings for text documents.
- [cosine_distance](./03-ai-cosine-distance.md): Calculates the cosine distance between two embeddings.
- [ai_text_completion](./04-ai-text-completion.md): Generates text completions based on a given prompt.
These functions are powered by open-source natural language processing models and can be used directly within SQL queries.

## Creating and storing embeddings using Databend

To create embeddings for a text document using Databend, you can use the built-in ai_embedding_vector function directly in your SQL query. Here's an example:

```sql
CREATE TABLE documents (
    doc_id INT,
    text_content TEXT
);

INSERT INTO documents (doc_id, text_content)
VALUES
    (1, 'Artificial intelligence is a fascinating field.'),
    (2, 'Machine learning is a subset of AI.'),
    (3, 'I love going to the beach on weekends.');

CREATE TABLE embeddings (
    doc_id INT,
    text_content TEXT,
    embedding ARRAY(FLOAT32)
);

INSERT INTO embeddings (doc_id, text_content, embedding)
SELECT doc_id, text_content, ai_embedding_vector(text_content)
FROM documents;
```

This SQL script creates a documents table, inserts the example documents, and then generates embeddings using the ai_embedding_vector function. The embeddings are stored in the embeddings table with the ARRAY(FLOAT32) column type.

## Searching for related documents using cosine distance

Suppose you have a question, "What is a subfield of artificial intelligence?", and you want to find the most related document from the stored embeddings. First, generate an embedding for the question using the ai_embedding_vector function:
```sql
SELECT doc_id, text_content, cosine_distance(embedding, ai_embedding_vector('What is a subfield of artificial intelligence?')) AS distance
FROM embeddings
ORDER BY distance ASC
LIMIT 5;
```
This query will return the top 5 most similar documents to the input question, ordered by their cosine distance, with the smallest distance indicating the highest similarity.

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

## Generating text completions with Databend

Databend also supports a text completion function, ai_text_completion. For example, from the above output, we choose the document with the smallest cosine distance: "Artificial intelligence is a fascinating field." We can use this as context and provide the original question to the ai_text_completion function to generate a completion:

```sql
SELECT ai_text_completion('Artificial intelligence is a fascinating field. What is a subfield of artificial intelligence?') AS completion;
```

Result:
```sql
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| completion                                                                                                                                                                                                                                                                        |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|
| A subfield of artificial intelligence is machine learning, which is the study of algorithms that allow computers to learn from data and improve their performance over time. Other subfields include natural language processing, computer vision, robotics, and deep learning.   |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```


You can experience these functions on our [Databend Cloud](https://databend.com), where you can sign up for a free trial and start using these AI functions right away. Databend's AI functions are designed to be easy to use, even for users who are not familiar with machine learning or natural language processing. With Databend, you can quickly and easily add powerful AI capabilities to your SQL queries and take your data analysis to the next level.
