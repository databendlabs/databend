---
title: SPLIT
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.164"/>

Splits a string using a specified delimiter and returns the resulting parts as an array.

See also: [SPLIT_PART](split_part.md)

## Syntax

```sql
SPLIT('<input_string>', '<delimiter>')
```

## Return Type

Array of strings. SPLIT returns NULL when either the input string or the delimiter is NULL.

## Examples

```sql
-- Use a space as the delimiter
-- SPLIT returns an array with two parts.
SELECT SPLIT('Databend Cloud', ' ');

split('databend cloud', ' ')|
----------------------------+
['Databend','Cloud']        |

-- Use an empty string as the delimiter or a delimiter that does not exist in the input string
-- SPLIT returns an array containing the entire input string as a single part.
SELECT SPLIT('Databend Cloud', '');

split('databend cloud', '')|
---------------------------+
['Databend Cloud']         |

SELECT SPLIT('Databend Cloud', ',');

split('databend cloud', ',')|
----------------------------+
['Databend Cloud']          |

-- Use '	' (tab) as the delimiter
-- SPLIT returns an array with timestamp, log level, and message.

SELECT SPLIT('2023-10-19 15:30:45	INFO	Log message goes here', '	');

split('2023-10-19 15:30:45\tinfo\tlog message goes here', '\t')|
---------------------------------------------------------------+
['2023-10-19 15:30:45','INFO','Log message goes here']         |
```