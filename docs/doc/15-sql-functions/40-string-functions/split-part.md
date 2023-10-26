---
title: SPLIT_PART
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.164"/>

Splits a string using a specified delimiter and returns the specified part.

See also: [SPLIT](split.md)

## Syntax

```sql
SPLIT_PART('<input_string>', '<delimiter>', '<position>')
```

The *position* argument specifies which part to return. It uses a 1-based index but can also accept positive, negative, or zero values:

- If *position* is a positive number, it returns the part at the position from the left to the right, or NULL if it doesn't exist.
- If *position* is a negative number, it returns the part at the position from the right to the left, or NULL if it doesn't exist.
- If *position* is 0, it is treated as 1, effectively returning the first part of the string.

## Return Type

String. SPLIT_PART returns NULL when either the input string, the delimiter, or the position is NULL.

## Examples

```sql
-- Use a space as the delimiter
-- SPLIT_PART returns a specific part.
SELECT SPLIT_PART('Databend Cloud', ' ', 1);

split_part('databend cloud', ' ', 1)|
------------------------------------+
Databend                            |

-- Use an empty string as the delimiter or a delimiter that does not exist in the input string
-- SPLIT_PART returns the entire input string.
SELECT SPLIT_PART('Databend Cloud', '', 1);

split_part('databend cloud', '', 1)|
-----------------------------------+
Databend Cloud                     |

SELECT SPLIT_PART('Databend Cloud', ',', 1);

split_part('databend cloud', ',', 1)|
------------------------------------+
Databend Cloud                      |

-- Use '    ' (tab) as the delimiter
-- SPLIT_PART returns individual fields.
SELECT SPLIT_PART('2023-10-19 15:30:45   INFO   Log message goes here', '   ', 3);

split_part('2023-10-19 15:30:45   info   log message goes here', '   ', 3)|
--------------------------------------------------------------------------+
Log message goes here                                                     |

-- SPLIT_PART returns an empty string as the specified part does not exist at all.
SELECT SPLIT_PART('2023-10-19 15:30:45   INFO   Log message goes here', '   ', 4);

split_part('2023-10-19 15:30:45   info   log message goes here', '   ', 4)|
--------------------------------------------------------------------------+
                                                                          |
```