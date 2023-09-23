---
title: JSON_PATH_EXISTS
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.119"/>

Checks whether a specified path exists in JSON data.

## Syntax

```sql
JSON_PATH_EXISTS(<json_data>, <json_path_expression>)
```

- json_data: Specifies the JSON data you want to search within. It can be a JSON object or an array.

- json_path_expression: Specifies the path, starting from the root of the JSON data represented by `$`, that you want to check within the JSON data. You can also include conditions within the expression, using `@` to refer to the current node or element being evaluated, to filter the results.

## Return Type

The function returns:

- 1 if the specified JSON path (and conditions if any) exists within the JSON data.
- 0 if the specified JSON path (and conditions if any) does not exist within the JSON data.
- NULL if either the json_data or json_path_expression is NULL or invalid.

## Examples

```sql
SELECT JSON_PATH_EXISTS(parse_json('{"a": 1, "b": 2}'), '$.a ? (@ == 1)')

----
1


SELECT JSON_PATH_EXISTS(parse_json('{"a": 1, "b": 2}'), '$.a ? (@ > 1)')

----
0

SELECT json_path_exists(NULL, '$.a')

----
NULL

SELECT json_path_exists(parse_json('{"a": 1, "b": 2}'), NULL)

----
NULL
```