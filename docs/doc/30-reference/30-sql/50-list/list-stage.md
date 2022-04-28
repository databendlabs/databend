---
title: LIST FILES IN STAGE
---


## Syntax

```
LIST { internalStage | externalStage } [ PATTERN = '<regex_pattern>' ]
```

Where:
```text
  internalStage ::= @<internal_stage_name>
```

```text
  externalStage ::= @<external_stage_name>
```

## Examples

```sql
list @named_external_stage PATTERN = 'ontime.*parquet';
+-----------------------+
| file_name             |
+-----------------------+
| ontime_200.parquet    |
| ontime_200_v1.parquet |
+-----------------------+
```
