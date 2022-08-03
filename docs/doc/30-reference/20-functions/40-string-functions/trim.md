---
title: TRIM
---

Returns the string without leading or trailing occurrences of the specified remove string. If remove string
is omitted, spaces are removed.

## Syntax

```sql
TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM ] str)
```

## Examples

Please note that ALL the examples in this section will return the string 'databend'.

The following example removes the leading and trailing string 'xxx' from the string 'xxxdatabendxxx':

```sql
SELECT TRIM(BOTH 'xxx' FROM 'xxxdatabendxxx');
```

The following example removes the leading string 'xxx' from the string 'xxxdatabend':

```sql
SELECT TRIM(LEADING 'xxx' FROM 'xxxdatabend' );
```
The following example removes the trailing string 'xxx' from the string 'databendxxx':

```sql
SELECT TRIM(TRAILING 'xxx' FROM 'databendxxx' );
```

If no remove string is specified, the function removes all leading and trailing spaces. The following examples remove the leading and/or trailing spaces:

```sql
SELECT TRIM('   databend   ');
SELECT TRIM('   databend');
SELECT TRIM('databend   ');
```