<!-- Notes for Contributors

To create a document using this template:

1. Replace the x.x.xx in the <FunctionDescription> to indicate the version when the function will be introduced. For example, 
<FunctionDescription description="Introduced: v1.2.34"/>
2. If the function is only available for Databend Enterprise, remove the comment tags for <EEFeature>, and replace FUNCTION_NAME with the name of the function (ALL CAPS).
3. Replace <FUNCTION_NAME> with the name of the function (ALL CAPS).
4. Replace <FUNCTION_DESCRIPTION> with a brief description of the function. If the function is related to existing functions, provide references in the See also section. Otherwise, remove the See also section.
5. Replace <FUNCTION_SYNTAX> with the function syntax.
6. If the function takes one or more arguments, fill the table in the Arguments section. Otherwise, remove the Arguments section.
7. Replace <RETURN_TYPE_DESCRIPTION> with a description of the value that the function returns.
8. Replace <EXAMPLES> with one or more examples demonstrating the usage of the function.
9. Edit any other sections or add additional information as needed.
-->

---
title: <FUNCTION_NAME>
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: vx.x.xx"/>

<!-- 
import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='FUNCTION_NAME'/>
-->

<FUNCTION_DESCRIPTION>

See also: <RELEVANT_FUNCTIONS>

## Syntax

```sql
<FUNCTION_SYNTAX>
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| ...       | ...         |

## Return Type

<RETURN_TYPE_DESCRIPTION>

## Examples

<EXAMPLES>