---
title: CREATE FILE FORMAT 
---

Creates a named file format.

## Syntax

```sql
CREATE FILE FORMAT [ IF NOT EXISTS ] <format_name> FileFormatOptions
```

For details about `FileFormatOptions`, see [Input & Output File Formats](../../../13-sql-reference/50-file-format-options.md).

## Examples

```sql
CREATE FILE FORMAT my_custom_csv TYPE=CSV  FIELD_DELIMITER='\t' 
```