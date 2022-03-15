---
title: CREATE Stage
---

Create a new stage.

## Syntax

- Create Internal Stage

```sql
CREATE STAGE [ IF NOT EXISTS ] <stage_name>;
```

- Create External Stage
```sql
CREATE STAGE [ IF NOT EXISTS ] <stage_name> uri = 's3://<bucket>[/<path>]' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z');
```

## Examples

```sql
mysql> CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z');

mysql> desc STAGE;
```
