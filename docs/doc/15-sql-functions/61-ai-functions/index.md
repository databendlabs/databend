---
title: 'AI Functions'
description: 'Using SQL-based AI Functions for Knowledge Base Search and Text Completion'
---

This document demonstrates how to leverage Databend's built-in AI functions for creating document embeddings, searching for similar documents, and generating text completions based on context. We will guide you through a simple example that shows how to create and store embeddings, find related documents, and generate completions using various AI functions.

:::caution

Databend relies on OpenAI for embeddings and text completions, which means your data will be sent to OpenAI. Exercise caution when using these functions.

They will only work when the Databend configuration includes the `openai_api_key`, otherwise they will be inactive.

These functions are available by default on [Databend Cloud](https://databend.com) using our self OpenAI key. If you use them, you acknowledge that your data will be sent to OpenAI by us.

:::

## Introduction to embeddings

Embeddings are vector representations of text data that capture the semantic meaning and context of the original text. They can be used to compare and analyze text in various natural language processing tasks, such as document similarity, clustering, and recommendation systems.

## How do embeddings work?

Embeddings work by converting text into high-dimensional vectors in such a way that similar texts are closer together in the vector space.

This is achieved by training a model on a large corpus of text, which learns to represent the words and phrases in a continuous space that captures their semantic relationships.

To illustrate how embeddings work, let's consider a simple example. Suppose we have the following sentences:
1. `"The cat sat on the mat."`
2. `"The dog sat on the rug."`
3. `"The quick brown fox jumped over the lazy dog."`

When creating embeddings for these sentences, the model will convert the text into high-dimensional vectors in such a way that similar sentences are closer together in the vector space.

For instance, the embeddings of sentences 1 and 2 will be closer to each other because they share a similar structure and meaning (both involve an animal sitting on something). On the other hand, the embedding of sentence 3 will be farther from the embeddings of sentences 1 and 2 because it has a different structure and meaning.

The embeddings could look like this (simplified for illustration purposes):

1. `[0.2, 0.3, 0.1, 0.7, 0.4]`
2. `[0.25, 0.29, 0.11, 0.71, 0.38]`
3. `[-0.1, 0.5, 0.6, -0.3, 0.8]`

In this simplified example, you can see that the embeddings of sentences 1 and 2 are closer to each other in the vector space, while the embedding of sentence 3 is farther away. This illustrates how embeddings can capture semantic relationships and be used to compare and analyze text data.


## What is a Vector Database?

A vector database is a specialized database designed to store, manage, and search high-dimensional vector data efficiently. These databases are optimized for similarity search operations, such as finding the nearest neighbors of a given vector. They are particularly useful in scenarios where the data has high dimensionality, like embeddings in natural language processing tasks, image feature vectors, and more.

Typically, embedding vectors are stored in specialized vector databases like milvus, pinecone, qdrant, or weaviate. Databend can also store embedding vectors using the ARRAY(FLOAT32) data type and perform similarity computations with the cosine_distance function in SQL. To create embeddings for a text document using Databend, you can use the built-in ai_embedding_vector function directly in your SQL query.

## Databend AI Functions

Databend provides built-in AI functions for various natural language processing tasks. The main functions covered in this document are:

- [ai_embedding_vector](./02-ai-embedding-vector.md): Generates embeddings for text documents.
- [ai_text_completion](./03-ai-text-completion.md): Generates text completions based on a given prompt.
- [cosine_distance](./04-ai-cosine-distance.md): Calculates the cosine distance between two embeddings.

## Creating and storing embeddings using Databend


Here's an example:
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

This SQL script creates a `documents` table, inserts the example documents, and then generates embeddings using the `ai_embedding_vector` function. The embeddings are stored in the embeddings table with the `ARRAY(FLOAT32)` column type.

## Searching for similarity documents using cosine distance

Suppose you have a question, "What is a subfield of artificial intelligence?", and you want to find the most related document from the stored embeddings. First, generate an embedding for the question using the `ai_embedding_vector` function:
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


## Examples(https://ask.databend.rs)

We have utilized [Databend Cloud](https://databend.com) and its AI functions to create an interactive Q&A system for the https://databend.rs website. The demo site, https://ask.databend.rs, allows users to ask questions about any topic related to the https://databend.rs website.

:::note
You can also deploy Databend and configure the `openai_api_key`.
:::

Here's a step-by-step guide to how https://ask.databend.rs was built:

### Step 1: Create Table

First, create a table with the following structure to store document information and embeddings:
```sql
CREATE TABLE doc (
                     path VARCHAR,
                     content VARCHAR,
                     embedding ARRAY(FLOAT32)
);
```

### Step 2: Insert Raw Data

Insert sample data into the table, including the path and content for each document:
```sql
INSERT INTO doc (path, content) VALUES
    ('ai-function', 'ai_embedding_vector, ai_text_completion, cosine_distance'),
    ('string-function', 'ASCII, BIN, CHAR_LENGTH');
```

### Step 3: Generate Embeddings

Update the table to generate embeddings for the content using the [ai_embedding_vector](./02-ai-embedding-vector.md) function:
```sql
UPDATE doc SET embedding = ai_embedding_vector(content)
WHERE LENGTH(embedding) = 0;
```

### Step 4: Ask a Question and Retrieve Relevant Answers

```sql
-- Define the question as a CTE (Common Table Expression)
WITH question AS (
    SELECT 'Tell me the ai functions' AS q
),
-- Calculate the question's embedding vector
question_embedding AS (
    SELECT ai_embedding_vector((SELECT q FROM question)) AS q_vector
),
-- Retrieve the top 3 most relevant documents
top_3_docs AS (
    SELECT content,
           cosine_distance((SELECT q_vector FROM question_embedding), embedding) AS dist
    FROM doc
    ORDER BY dist ASC
    LIMIT 3
),
-- Combine the content of the top 3 documents
combined_content AS (
    SELECT string_agg(content, ' ') AS aggregated_content
    FROM top_3_docs
),
-- Concatenate a custom prompt, the combined content, and the original question
prompt AS (
    SELECT CONCAT(
               'Utilizing the sections provided from the Databend documentation, answer the questions to the best of your ability. ',
               'Documentation sections: ',
               (SELECT aggregated_content FROM combined_content),
               ' Question: ',
               (SELECT q FROM question)
           ) as p
)
-- Pass the concatenated text to the ai_text_completion function to generate a coherent and relevant response
SELECT ai_text_completion((SELECT p FROM prompt)) AS answer;
```

Result:
```sql
+------------------------------------------------------------------------------------------------------------------+
| answer                                                                                                           |
+------------------------------------------------------------------------------------------------------------------+
| Answer: The ai functions mentioned in the Databend documentation are ai_embedding_vector and ai_text_completion. |
+------------------------------------------------------------------------------------------------------------------+
```
