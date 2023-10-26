---
title: 'JSON Operators'
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.178"/>

| Operator | Description | Example | Result |
|----------|-------------|---------|--------|
| -> | Retrieves a JSON array or object using an index or key, returning a JSON value. | PARSE_JSON('{"Databend": "Cloud Native Warehouse"}')->'Databend' | Databend |
| ->> | Retrieves a JSON array or object using an index or key, returning a string. | PARSE_JSON('[24, "Databend"]')->>1 | Databend |
| #> | Extracts JSON Array or Object with path, returning a JSON value. | `{"person": {"name": "John"}} #> '{person,name}'` | {"name": "John"} |
| #>> | Extracts JSON Array or Object with path, returning a string. | `{"person": {"name": "John"}} #>> '{person,name}'` | John |
