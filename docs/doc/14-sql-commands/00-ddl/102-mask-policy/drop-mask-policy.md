---
title: DROP MASKING POLICY
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.45"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='MASKING POLICY'/>

Deletes an existing masking policy from Databend. When you drop a masking policy, it is removed from Databend, and its associated masking rules are no longer in effect. Please note that, before dropping a masking policy, ensure that this policy is not associated with any columns.

## Syntax

```sql
DROP MASKING POLICY [IF EXISTS] <policy_name>
```

## Examples

```sql
CREATE MASKING POLICY email_mask
AS
  (val string)
  RETURNS string ->
  CASE
  WHEN current_role() IN ('MANAGERS') THEN
    val
  ELSE
    '*********'
  END
  COMMENT = 'hide_email';

DROP MASKING POLICY email_mask;
```