---
title: Upgrade Databend-query from 0.8 to 0.9
sidebar_label: Upgrade Databend-query from 0.8 to 0.9 
description:
    Meta data migration when upgrading databend-query from 0.8 to 0.9
---

This topic explains how to migrate metadata when upgrading databend-query from 0.8 to 0.9

Databend-query-0.9 introduces incompatible changes in metadata, these metadata has to be manually migrated.
Databend provides a program for this job: `databend-meta-upgrade-09`, which you can find in a [release package](https://github.com/datafuselabs/databend/releases) or can be built from source.

`databend-meta-upgrade-09` upgrades metadata written by databend-query before 0.9 to a databend-query-0.9 compatible format.
It loads metadata from a `raft-dir` and upgrade `TableMeta` record to 0.9 compatible version and write them back.
Both raft-log data and state-machine data will be converted.

# Usage

- Shut down all databend-meta processes.

- Before proceeding, backup your data. See: [Backup and resotre metadata](../06-metasrv/30-metasrv-backup-restore.md).

- Download `databend-meta-upgrade-09` from [Databend release](https://github.com/datafuselabs/databend/releases)
  or build it with `cargo build --bin databend-meta-upgrade-09`

- (Optional) To view the current `TableMeta` version, print all `TableMeta` records with the following command:
  It should display a list of `TableMeta` record.
  You need to upgrade only if there is a `ver` that is lower than 24.

   ```text
   databend-meta-upgrade-09 --cmd print --raft-dir "<./your/raft-dir/>"
   # output:
   # TableMeta { ver: 23, ..
   ```
  
- Upgrade metadata:

  ```text
  databend-meta-upgrade-09 --cmd upgrade --raft-dir "<./your/raft-dir/>"
  ```

- To assert upgrade has finished successfully, print all `TableMeta` records that are found in meta dir with the following command:
  It should display a list of TableMeta record with a `ver` that is greater or equal 24.

   ```text
   databend-meta-upgrade-09 --cmd print --raft-dir "<./your/raft-dir/>"
   # output:
   # TableMeta { ver: 25, ..
   # TableMeta { ver: 25, ..
   ```
