---
title: JSON_STRIP_NULLS
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.89"/>

Removes all properties with null values from a JSON object. 

## Syntax

```sql
JSON_STRIP_NULLS(<json_string>)
```

## Return Type

Returns a value of the same type as the input JSON value.

## Examples

```sql
SELECT JSON_STRIP_NULLS(PARSE_JSON('{"name": "Alice", "age": 30, "city": null}'));

json_strip_nulls(parse_json('{"name": "alice", "age": 30, "city": null}'))|
--------------------------------------------------------------------------+
{"age":30,"name":"Alice"}                                                 |
```