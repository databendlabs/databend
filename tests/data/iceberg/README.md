This iceberg data was created from `Spark3` in minio bucket.

The minio bucket url:

```
s3://testbucket/
```

Please move the `iceberg_data` directory to the bucket root,
the iceberg table data _ relies on absolute path URIs _ like `s3://testbucket/iceberg_data/iceberg_ctl/iceberg_tbl/...`,
any changes to it may cause the table to be unreadable.

## Reproduction of table data

Initiate a `spark-sql` shell, and run following SQLs:

1. create table

```sql
CREATE TABLE iceberg_ctl.iceberg_db.iceberg_tbl (id INT NOT NULL, data STRING NOT NULL) USING ICEBERG;
```

2. insert data

```sql
-- First transaction
INSERT INTO iceberg_ctl.iceberg_db.iceberg_tbl VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- Second transaction
INSERT INTO iceberg_ctl.iceberg_db.iceberg_tbl VALUES (4, 'd'), (5, 'e'), (6, 'd');
```

**----- The following steps are not applied to the test data now. -----**

3. make a schema evolution

```sql
-- Add a column to the table
ALTER TABLE iceberg_ctl.iceberg_db.iceberg_tbl ADD COLUMNS (comment STRING);
```

4. overwrite table data

```sql
INSERT OVERWRITE iceberg_ctl.iceberg_db.iceberg_tbl VALUES (1, 'a', 'AC/DC'), (2, 'b', 'Bob Dylan'), (3, 'c', 'Coldplay'), (4, 'd', 'David Bowie');
```

5. insert data

```sql
-- First transaction
INSERT INTO iceberg_ctl.iceberg_db.iceberg_tbl VALUES (5, 'e', NULL);
-- Second transaction
INSERT INTO iceberg_ctl.iceberg_db.iceberg_tbl VALUES (6, 'f', 'Fender');
```

## Docker compose file used

To recreate this data in your own environment, you should have `docker` and `docker-compose` installed.

The docker compose file used to create test data:

```yaml
# for testing propose only
# don't use such weak authentication in production environments
# thanks to `https://iceberg.apache.org/spark-quickstart/`

version: "3"

services:
  spark-iceberg:
    image: tabulario/spark-iceberg
    container_name: spark-iceberg
    build: spark/
    depends_on:
      - rest
      - minio
    volumes:
      - ./warehouse:/iceberg/warehouse
      - ./notebooks:/iceberg/notebooks/notebooks
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    ports:
      - 8888:8888
      - 8080:8080
    links:
      - rest:rest
      - minio:minio
  rest:
    image: tabulario/iceberg-rest:0.1.0
    ports:
      - 8181:8181
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - CATALOG_WAREHOUSE=s3://testbucket/iceberg_data/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000
  minio:
    image: minio/minio
    container_name: minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
    ports:
      - 9001:9001
      - 9000:9000
        # volumes:
        # - ./minio/data:/data
    command: ["server", "/data", "--console-address", ":9001"]
  mc:
    depends_on:
      - minio
    image: minio/mc
    container_name: mc
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
        # volumes:
        # - ./minio/data:/data
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/testbucket;
      /usr/bin/mc mb minio/testbucket;
      /usr/bin/mc policy set public minio/testbucket;
      exit 0;
      "
```

To initiate the `spark-sql` shell:

```bash
# cd to the directory containing your docker-compose.yml
# make sure you have the compose running
docker exec -it spark-iceberg spark-sql pyspark-notebook
```
