# Databend Bendsave

bendsave is a tool built by Databend Labs to backup and restore data from a Databend cluster.

## Architecture

```txt
        +-------------------------+
        |    Databend Cluster    |
        +-----------+------------+
                    |
                    | Read/Write Data
                    v
        +-----------+------------+
        |   databend-bendsave    |
        +-----------+------------+
                    |
                    | Storage Operations
                    v
+-------------------+-------------------+
|                                       |
|       Storage Backend (S3 etc.)       |
|                                       |
+---------------------------------------+
```

## Usage

```shell
# Backup
bendsave backup --from /path/to/query-node-1.toml --to s3://backup/
# Restore
bendsave restore --from s3://backup/manifests/20250115_201500.manifest --to /path/to/query-node-1.toml
```

### Backup

```shell
databend-bendsave backup \
  --from /path/to/databend-query-config.toml \
  --to s3://backup?endpoint=http://127.0.0.1:9900/
```

The `backup` command creates a backup of data from a Databend cluster to a specified location.

The `--from` flag specifies the path to the Databend query configuration file, while the `--to` flag defines the destination for storing the backup. The `--to` flag should be a URL indicating where the backup will be stored, with all relevant backup configurations embedded in the URL.

For example, the URL `s3://backup?endpoint=http://127.0.0.1:9900/` specifies that the backup will be stored in an S3 bucket named `backupbucket`, with the S3 service endpoint set to `http://127.0.0.1:9900/`.

Users can provide the `access_key_id` directly in the URL, such as `s3://backup?access_key_id=xxx&secret_access_key=xxx`. However, it is recommended to use environment variables like `AWS_ACCESS_KEY_ID` instead.

### Restore

```shell
databend-bendsave restore \
  --from s3://backup?endpoint=http://127.0.0.1:9900/ \
  --to-query /path/to/databend-query-config.toml \
  --to-meta /path/to/databend-meta-config.toml \
  --confirm
```

The `restore` command recovers data from a backup and applies it to a Databend cluster.

The `--from` flag defines the URL of the backup location, incorporating all necessary configurations within the URL. This should match the URL used during the backup process.

The `--to-query` flag specifies the path to the Databend query configuration file, while the `--to-meta` flag designates the path to the Databend meta configuration file.

The `--confirm` flag confirms the restore operation.
