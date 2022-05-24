---
title: Back Up and Restore Databend Schema Data
sidebar_label: Backup and Restore Schema Data
description: How to back up and restore schema data ---
---

This guideline will introduce how to back up and restore the schema data which in meta service with mydumper tool.

## Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* **mydumper**: [How to Install mydumper](https://github.com/mydumper/mydumper).

:::caution

mydumper only export the Databend schema(including database and table) which stored in Databend meta service, PLEASE DON'T USE IT TO EXPORT DATA!

:::

If you don't have a Databend user for dumping, please create one:

```shell
mysql -h127.0.0.1 -uroot -P3307
```

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL on *.* TO user1;
```

## Export Schema from Databend

```shell
mydumper --host 127.0.0.1 --user user1 --password abc123 --port 3307 \
--no-locks \
--no-data \
--database test_db \
--outputdir /tmp/test_db
```

:::tip `--host`: Do not dump or import table data.

`--no-locks`: Do not execute the temporary shared read lock.

`--no-data`: Do not dump or import table data.

`--database`: Database to dump.

`--outputdir`: Directory to output files to. :::

The `/tmp/test_db` directory seems look:
```shell
tree /tmp/test_db/ 
├── metadata
├── test_db-schema-create.sql
└── test_db.t1-schema.sql
```

## Restore Schema into Databend

To restore schema into a new Databend, use `myloader` to import the `/tmp/test_db` directory.

```shell
myloader --host 127.0.0.1 --user user1 --password abc123 --port 3307 \
--directory /tmp/test_db/
```
:::tip `--directory`: Directory of the dump to import. :::
