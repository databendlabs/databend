---
title: Loading files from S3 External Location
---

Load files from a storage location (Amazon S3) using [COPY command](../01-sql-statement/03-dml/dml-copy.md) .

:::note Notes
1. Only CSV Format supported.
2. Only privately S3 bucket supported.
:::

## Amazon S3

### Example: Loading a file from an External Location

First, let's try to read 5 rows(`SIZE_LIMIT=5`) from the csv file to make sure it works:
```sql

COPY INTO ontime FROM 's3://databend-external/t_ontime/t_ontime.csv'
    CREDENTIALS=(aws_key_id='<your-key-id>' aws_secret_key='<your-secret-key>')
    FILE_FORMAT = (type = "CSV" field_delimiter = '\t'  record_delimiter = '\n' skip_header = 1)
    SIZE_LIMIT=5; /* only read 5 rows */
    
/* Check. */
SELECT * FROM ontime;

/* Clean the table. */
TRUNCATE TABLE ontime;
```

Let's go:
```
COPY INTO ontime FROM 's3://databend-external/t_ontime/t_ontime.csv'
    CREDENTIALS=(aws_key_id='<your-key-id>' aws_secret_key='<your-secret-key>')
    FILE_FORMAT = (type = "CSV" field_delimiter = '\t'  record_delimiter = '\n' skip_header = 1);
```

### Example: Loading files from an External Directory

```
COPY INTO ontime FROM 's3://databend-external/t_ontime/'
    CREDENTIALS=(aws_key_id='<your-key-id>' aws_secret_key='<your-secret-key>')
    FILES=('1.csv', '2.csv')
    FILE_FORMAT = (type = "CSV" field_delimiter = '\t'  record_delimiter = '\n' skip_header = 1);
```
