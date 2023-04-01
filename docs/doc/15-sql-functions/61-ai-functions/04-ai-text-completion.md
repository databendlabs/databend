---
title: 'AI_TEXT_COMPLETION'
description: 'Generating text completions using the ai_text_completion function in Databend'
---

This document provides an overview of the ai_text_completion function in Databend and demonstrates how to generate text completions using this function.

## Overview of ai_text_completion

The `ai_text_completion` function in Databend is a built-in function that generates text completions based on a given prompt. It is useful for natural language processing tasks, such as question answering, text generation, and autocompletion systems.

The function takes a text prompt as input and returns a generated completion for the prompt. The completions are created using pre-trained models on large text corpora, capturing the relationships between words and phrases in a continuous space.

## Generating text completions using ai_text_completion

Here is a simple example using the `ai_text_completion` function in Databend to generate a text completion:
```sql
SELECT ai_text_completion('What is artificial intelligence?') AS completion;
```

Result:
```sql
+--------------------------------------------------------------------------------------------------------------------+
| completion                                                                                                          |
+--------------------------------------------------------------------------------------------------------------------+
| Artificial intelligence (AI) is the field of study focused on creating machines and software capable of thinking, learning, and solving problems in a way that mimics human intelligence. This includes areas such as machine learning, natural language processing, computer vision, and robotics. |
+--------------------------------------------------------------------------------------------------------------------+
```

In this example, we provide the prompt "What is artificial intelligence?" to the ai_text_completion function, and it returns a generated completion that briefly describes artificial intelligence.