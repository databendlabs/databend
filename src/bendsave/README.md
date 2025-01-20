# BendSave

bendsave is a tool built by Databend Labs to backup and restore data from a Databend cluster.

## Usage

```shell
# Backup
bendsave backup --from /path/to/query-node-1.toml --to s3://backup/
# Restore
bendsave restore --from s3://backup/manifests/20250115_201500.manifest --to /path/to/query-node-1.toml
```
