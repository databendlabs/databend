# Databend All-in-One Docker Image

Support Platform: `linux/amd64`, `linux/arm64`

## How to use

```
docker run -p 8000:8000 datafuselabs/databend
```

Available Enviroment Variables:
* DATABEND_LOG_DIR
* QUERY_CONFIG_FILE
* QUERY_STORAGE_DATA_DIR
* DATABEND_DEFAULT_USER
* DATABEND_DEFAULT_PASSWORD


## Image with MinIO

```
docker run -p 9000:9000 -p 8000:8000 datafuselabs/databend:minio
```

Available Enviroment Variables:
* MINIO_DATA_DIR
* MINIO_ROOT_USER
* MINIO_ROOT_PASSWORD
* MINIO_QUERY_BUCKET
* MINIO_PRESIGNED_ENDPOINT
* DATABEND_LOG_DIR
* QUERY_CONFIG_FILE
* DATABEND_DEFAULT_USER
* DATABEND_DEFAULT_PASSWORD
