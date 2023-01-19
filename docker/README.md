# Databend All-in-One Docker Image

Support Platform: `linux/amd64`, `linux/arm64`


## Available Enviroment Variables

* DATABEND_QUERY_CONFIG_FILE
* DATABEND_QUERY_DEFAULT_USER
* DATABEND_QUERY_DEFAULT_PASSWORD
* DATABEND_QUERY_STORAGE_TYPE

* AWS_S3_ENDPOINT
* AWS_S3_PRESIGNED_ENDPOINT
* AWS_S3_BUCKET
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY

* MINIO_ENABLED


## How to use


### Run default config with fs backend
```
docker run -p 8000:8000 datafuselabs/databend
```

### Run with MinIO as backend
```
docker run \
    -p 8000:8000 \
    -p 9000:9000 \
    -e MINIO_ENABLED=true \
    datafuselabs/databend
```

### Adding built-in query user
```
docker run \
    -p 8000:8000 \
    -e DATABEND_QUERY_DEFAULT_USER=databend \
    -e DATABEND_QUERY_DEFAULT_PASSWORD=databend \
    datafuselabs/databend
```

### Run with external S3 service

```
docker run \
    -p 8000:8000 \
    -e DATABEND_QUERY_STORAGE_TYPE=s3 \
    -e AWS_S3_ENDPOINT="http://some_s3_endpoint \
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
    -e DATABEND_QUERY_CONFIG_FILE=/etc/databend/mine.toml \
    -v query_config_file:/etc/databend/mine.toml \
    datafuselabs/databend
```
