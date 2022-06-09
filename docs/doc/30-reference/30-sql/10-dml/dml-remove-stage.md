---
title: REMOVE { internalStage | externalStage }
sidebar_label: REMOVE STAGE FILES
---

Removes files from either an external (external cloud storage) or internal stage.
## Syntax

```sql
REMOVE stage [ PATTERN = '<regex_pattern>' ]
```
Where:

### internalStage

```sql
internalStage ::= @<internal_stage_name>[/<file>]
```

### externalStage

```sql
externalStage ::= @<external_stage_name>[/<file>]
```

### PATTERN = 'regex_pattern'

A regular expression pattern string, enclosed in single quotes, specifying the file names to match.


## Examples

```sql
REMOVE @named_external_stage  PATTERN = 'ontime.*'
```
