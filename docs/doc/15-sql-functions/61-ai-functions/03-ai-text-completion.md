---
title: 'AI_TEXT_COMPLETION'
description: 'Generating text completions using the ai_text_completion function in Databend'
---

This document provides an overview of the `ai_text_completion` function in Databend and demonstrates how to generate text completions using this function.

The main code implementation can be found [here](https://github.com/datafuselabs/databend/blob/1e93c5b562bd159ecb0f336bb88fd1b7f9dc4a62/src/common/openai/src/completion.rs).

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
Databend relies on (Azure) OpenAI for `AI_TEXT_COMPLETION` and sends the completion prompt data to (Azure) OpenAI.

They will only work when the Databend configuration includes the `openai_api_key`, otherwise they will be inactive.

This function is available by default on [Databend Cloud](https://databend.com) using our Azure OpenAI key. If you use them, you acknowledge that your data will be sent to Azure OpenAI by us.
:::


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

In this example, we provide the prompt "What is artificial intelligence?" to the `ai_text_completion` function, and it returns a generated completion that briefly describes artificial intelligence.