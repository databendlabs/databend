---
title: JSON_TYPEOF
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.91"/>

Returns the type of the main-level of a JSON structure.

## Syntax

```sql
JSON_TYPEOF(<json_string>)
```

## Return Type

The return type of the json_typeof function (or similar) is a string that indicates the data type of the parsed JSON value. The possible return values are: 'null', 'boolean', 'string', 'number', 'array', and 'object'.

## Examples

```sql
-- Parsing a JSON value that is NULL
SELECT JSON_TYPEOF(PARSE_JSON(NULL));

--
json_typeof(parse_json(null))|
-----------------------------+
                             |

-- Parsing a JSON value that is the string 'null'
SELECT JSON_TYPEOF(PARSE_JSON('null'));

--
json_typeof(parse_json('null'))|
-------------------------------+
null                           |

SELECT JSON_TYPEOF(PARSE_JSON('true'));

--
json_typeof(parse_json('true'))|
-------------------------------+
boolean                        |

SELECT JSON_TYPEOF(PARSE_JSON('"Databend"'));

--
json_typeof(parse_json('"databend"'))|
-------------------------------------+
string                               |


SELECT JSON_TYPEOF(PARSE_JSON('-1.23'));

--
json_typeof(parse_json('-1.23'))|
--------------------------------+
number                          |

SELECT JSON_TYPEOF(PARSE_JSON('[1,2,3]'));

--
json_typeof(parse_json('[1,2,3]'))|
----------------------------------+
array                             |

SELECT JSON_TYPEOF(PARSE_JSON('{"name": "Alice", "age": 30}'));

--
json_typeof(parse_json('{"name": "alice", "age": 30}'))|
-------------------------------------------------------+
object                                                 |
```