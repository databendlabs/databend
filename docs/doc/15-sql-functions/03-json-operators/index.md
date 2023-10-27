---
title: 'JSON Operators'
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.178"/>

| Operator | Description | Example | Result |
|----------|-------------|---------|--------|
| -> | Retrieves a JSON array or object using an index or key, returning a JSON object. | - **Using a key**:<br/>PARSE_JSON('{"Databend": "Cloud Native Warehouse"}')->'Databend'<br/>- **Using an index**:<br/>PARSE_JSON('["Databend", "Cloud Native Warehouse"]')->1 | Cloud Native Warehouse |
| ->> | Retrieves a JSON array or object using an index or key, returning a string. | - **Using a key**:<br/>PARSE_JSON('{"Databend": "Cloud Native Warehouse"}')->>'Databend'<br/>- **Using an index**:<br/>PARSE_JSON('["Databend", "Cloud Native Warehouse"]')->>1 | Cloud Native Warehouse |
| #> | Retrieves a JSON array or object by specifying a key path, returning a JSON object. | PARSE_JSON('{"example": {"Databend": "Cloud Native Warehouse"}}')#>'{example, Databend}' | Cloud Native Warehouse |
| #>> | Retrieves a JSON array or object by specifying a key path, returning a string. | PARSE_JSON('{"example": {"Databend": "Cloud Native Warehouse"}}')#>>'{example, Databend}' | Cloud Native Warehouse |
