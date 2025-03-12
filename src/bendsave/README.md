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
|   ┌── data/                           |
|   │   └── [content-addressed          |
|   │       deduplicated chunks]        |
|   │                                   |
|   └── logs/                           |
|       └── [backup manifests]          |
+---------------------------------------+
```

### Backup

```txt
+-----------------+      +------------------+      +------------------+
| Databend Cluster|----->| databend-bendsave|----->| Storage Backend  |
|                 |      | (backup)         |      | (S3, etc.)       |
+-----------------+      +------------------+      +------------------+
        |                         |                         |
        |                         |                         |
        v                         v                         v
+----------------+     +--------------------+    +--------------------+
| Source Data    |     | Processing Steps:  |    | Storage Structure: |
|                |     |                    |    |                    |
| - Metadata     |     | 1. Read config     |    | data/              |
| - Data         |     | 2. Connect to DB   |    | ├── a1b2c3d4...    |
|                |     | 3. Collect data    |    | ├── e5f6g7h8...    |
|                |     | 4. Deduplicate     |    | └── ...            |
|                |     | 5. Create chunks   |    |                    |
|                |     | 6. Generate log    |    | logs/              |
+----------------+     +--------------------+    | └── 20250115_201500|
                                                 +--------------------+
```

### Restore

```txt
+------------------+      +------------------+      +-----------------+
| Storage Backend  |----->| databend-bendsave|----->| Databend Cluster|
| (S3, etc.)       |      | (restore)        |      |                 |
+------------------+      +------------------+      +-----------------+
        |                         |                         |
        |                         |                         |
        v                         v                         v
+--------------------+    +--------------------+    +--------------------+
| Storage Structure: |    | Processing Steps:  |    | Target Restoration:|
|                    |    |                    |    |                    |
| logs/              |    | 1. Read checkpoint |    | 1. Apply data      |
| └── 20250115_201500|--->| 2. Locate chunks   |--->| 2. Rebuild meta    |
|                    |    | 3. Validate data   |    | 3. Restore state   |
| data/              |    | 4. Prepare restore |    |                    |
| ├── a1b2c3d4...    |    |                    |    |                    |
| ├── e5f6g7h8...    |    |                    |    |                    |
| └── ...            |    |                    |    |                    |
+--------------------+    +--------------------+    +--------------------+
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
  --checkpoint=$CHECKPOINT \
  --confirm
```

The `restore` command recovers data from a backup and applies it to a Databend cluster.

The `--from` flag defines the URL of the backup location, incorporating all necessary configurations within the URL. This should match the URL used during the backup process.

The `--to-query` flag specifies the path to the Databend query configuration file, while the `--to-meta` flag designates the path to the Databend meta configuration file.

The `--checkpoint` flag indicates the checkpoint ID to restore, and the `--confirm` flag confirms the restore operation.

NOTE: the backup list is still under developement, so we need to get the checkpoint by hand from the backup storage. Take S3 as an example, we can get the checkpoint like this:

```shell
CHECKPOINT=$(aws --endpoint-url http://127.0.0.1:9900/ s3 ls s3://backup/logs/ --recursive | sort -k 1,2 | tail -n 1 | awk -F'[/.]' '{print $(NF-1)}')
```
