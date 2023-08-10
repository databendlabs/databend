---
title: CREATE MASKING POLICY
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.45"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='MASKING POLICY'/>

Creates a new masking policy in Databend.

## Syntax

```sql
CREATE MASKING POLICY [IF NOT EXISTS] <policy_name> AS 
    ( <arg_name_to_mask> <arg_type_to_mask> [ , <arg_1> <arg_type_1> ... ] )
    RETURNS <arg_type_to_mask> -> <expression_on_arg_name>
    [ COMMENT = '<comment>' ]
```

| Parameter              	| Description                                                                                                                           	|
|------------------------	|---------------------------------------------------------------------------------------------------------------------------------------	|
| policy_name              	| The name of the masking policy to be created.                                                                                          	|
| arg_name_to_mask       	| The name of the original data parameter that needs to be masked.                                                                      	|
| arg_type_to_mask       	| The data type of the original data parameter to be masked.                                                                            	|
| expression_on_arg_name 	| An expression that determines how the original data should be treated to generate the masked data.                                    	|
| comment                   | An optional comment providing information or notes about the masking policy.                                                          	|

:::note
Ensure that *arg_type_to_mask* matches the data type of the column where the masking policy will be applied.
:::

## Examples

This example creates a masking policy named *email_mask* that, based on the user's role, either reveals an email address or masks it with asterisks.

```sql
CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() IN ('MANAGERS') THEN VAL ELSE '*********'END comment = 'hide_email';
```