---
title: SHOW CREATE CATALOG
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.53"/>

Returns the detailed configuration of a specified catalog, including its type and storage parameters.

## Syntax

```sql
SHOW CREATE CATALOG <catalog_name>;
```

### Examples

```sql
CREATE CATALOG hive_ctl 
TYPE = HIVE 
CONNECTION =(
    ADDRESS = '127.0.0.1:9083' 
    URL = 's3://databend-toronto/' 
    AWS_KEY_ID = '<your_key_id>' 
    AWS_SECRET_KEY = '<your_secret_key>' 
);

SHOW CREATE CATALOG hive_ctl;

Catalog |Type|Option                |
--------+----+----------------------+
hive_ctl|hive|ADDRESS¶127.0.0.1:9083|

CREATE CATALOG iceberg_ctl
TYPE = ICEBERG
CONNECTION = (
    URL = 's3://databend/iceberg/'
    AWS_KEY_ID = 'minioadmin'
    AWS_SECRET_KEY = 'minioadmin'
    ENDPOINT_URL = 'https://127.0.0.1:9000'
);

SHOW CREATE CATALOG iceberg_ctl;

Name   |Value                                                                             |
-------+----------------------------------------------------------------------------------+
Catalog|iceberg_ctl                                                                       |
Type   |iceberg                                                                           |
Option |STORAGE PARAMS¶s3 | bucket=databend,root=/iceberg/,endpoint=https://127.0.0.1:9000|
```