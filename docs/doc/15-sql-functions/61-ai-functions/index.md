---
title: 'Databend AI Functions using OpenAI'
description: 'Learn how to use AI functions in Databend with the help of the OpenAI engine.'
---

Databend integrates AI functions with the help of the [OpenAI](https://openai.com/) engine, allowing users to generate SQL queries and interact with databases using natural language. This makes it easier for users to work with databases and query data without in-depth knowledge of SQL syntax.

## AI-Powered SQL Query Generation

Databend leverages the OpenAI Code-Davinci-002 engine to convert natural language into SQL queries and provide assistance with database interactions. By integrating OLAP and AI, Databend streamlines the process of crafting SQL queries based on your table schema. The ai_to_sql function enables the effortless generation of SQL queries using natural language, and the chat capabilities of the OpenAI engine can assist in answering database-related questions.