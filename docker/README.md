# Databend All-in-One Docker Image

Support Platform: `linux/amd64`, `linux/arm64`


## Available Environment Variables

* QUERY_CONFIG_FILE
* QUERY_DEFAULT_USER
* QUERY_DEFAULT_PASSWORD
* QUERY_STORAGE_TYPE

* AWS_S3_ENDPOINT
* AWS_S3_BUCKET
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY

* MINIO_ENABLED


## How to use


### Run default config with fs backend
```
docker run -p 8000:8000 datafuselabs/databend
```

### Adding built-in query user
```
docker run \
    -p 8000:8000 \
    -e QUERY_DEFAULT_USER=databend \
    -e QUERY_DEFAULT_PASSWORD=databend \
    datafuselabs/databend
```

> NOTE:
> if `QUERY_DEFAULT_USER` or `QUERY_DEFAULT_PASSWORD` is not specified, a default root user with no password will be added.

### Run with MinIO as backend
*NOTE:* setting `MINIO_ENABLED` will trigger a runtime MinIO binary download.

```
docker run \
    -p 8000:8000 \
    -p 9000:9000 \
    -e MINIO_ENABLED=true \
    -v minio_data_dir:/var/lib/minio \
    datafuselabs/databend
```

### Run with external S3 service

```
docker run \
    -p 8000:8000 \
    -e QUERY_STORAGE_TYPE=s3 \
    -e AWS_S3_ENDPOINT="http://some_s3_endpoint" \
    -e AWS_S3_BUCKET=some_bucket \
    -e AWS_ACCESS_KEY_ID=some_key \
    -e AWS_SECRET_ACCESS_KEY=some_secret \
    datafuselabs/databend
```

### Run with persistent local storage & logs
```
docker run \
    -p 8000:8000 \
    -v meta_storage_dir:/var/lib/databend/meta \
    -v query_storage_dir:/var/lib/databend/query \
    -v log_dir:/var/log/databend \
    datafuselabs/databend
```

### Run with self managed query config
```
docker run \
    -p 8000:8000 \
    -e QUERY_CONFIG_FILE=/etc/databend/mine.toml \
    -v query_config_file:/etc/databend/mine.toml \
    datafuselabs/databend
```


## How to connect

There are two ways connecting to databend with docker:

### Using default added root user

```shell
docker run \
    --net=host \
    datafuselabs/databend


❯ bendsql
Welcome to BendSQL.
Trying connect to localhost:8000 as user root.
Connected to DatabendQuery v1.1.2-nightly-8ade21e4669e0a2cc100615247705feacdf76c5b(rust-1.70.0-nightly-2023-04-15T16:08:52.195357424Z)

bendsql>


❯ mysql -P3307 -uroot --protocol=tcp
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 10
Server version: 8.0.26-v0.9.41-nightly-0edcc16(rust-1.68.0-nightly-2023-02-17T01:35:15.271479Z) 0

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

### Adding an built-in user

```shell
docker run \
    -p 8000:8000 \
    -p 3307:3307 \
    -e QUERY_DEFAULT_USER=databend \
    -e QUERY_DEFAULT_PASSWORD=databend \
    datafuselabs/databend


❯ bendsql -u databend -p databend
Welcome to BendSQL.
Trying connect to localhost:8000 as user databend.
Connected to DatabendQuery v1.1.2-nightly-8ade21e4669e0a2cc100615247705feacdf76c5b(rust-1.70.0-nightly-2023-04-15T16:08:52.195357424Z)

bendsql>


❯ mysql -P3307 -udatabend -pdatabend --protocol=tcp
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 10
Server version: 8.0.26-v0.9.41-nightly-0edcc16(rust-1.68.0-nightly-2023-02-17T01:35:15.271479Z) 0

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

> This method is also available with `--net=host`.
