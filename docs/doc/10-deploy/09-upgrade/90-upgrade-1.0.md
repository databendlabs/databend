---
title: Upgrade Databend to 1.0
sidebar_label: Upgrade Databend to 1.0
description:
    Upgrade Databend to 1.0 from 0.8 or later
---

This topic explains how to upgrade Databend to version 1.0 from a version 0.8 or later.

## Before You Start

Before upgrading, make sure you have completed the following tasks:

- Read and understand the [General Principles](50-upgrade.md#general-principles) about upgrading Databend.

- Back up your Meta data. For how to back up and restore Meta data, see [Backup and Restore Schema Data](10-backup-and-restore-schema.md).

- Go to the download page and download the right installation package for your platform.
  Note that `databend-meta-upgrade-09` is only packaged in [`v1.1.33`](https://github.com/datafuselabs/databend/releases/tag/v1.1.33-nightly) or previous release.

## Step 1: Upgrade databend-query

Upgrade the databend-query service cluster by cluster. Follow the steps below to upgrade the databend-query service in each node:

1. Shut down the running databend-query service.

```shell
killall databend-query
```

2. Replace the binary file `databend-query` in the folder where Databend is installed with the newer version.

3. Remove the following deprecated settings from your configuration file `databend-query.toml`:

```toml
database_engine_github_enabled = true
table_cache_enabled = true
table_memory_cache_mb_size = 1024
table_disk_cache_root = "_cache"
table_disk_cache_mb_size = 10240
table_cache_bloom_index_meta_count=3000
table_cache_bloom_index_data_bytes=1073741824
```

4. Add the following cache settings to the end of your configuration file `databend-query.toml`:

```toml
# Cache config.
[cache]
# Type of storage to keep the table data cache
#
# available options: [none|disk]
# default is "none", which disable table data cache
# use "disk" to enabled disk cache
data_cache_storage = "none"

[cache.disk]
# cache path
path = "./.databend/_cache"
# max bytes of cached data 20G
max_bytes = 21474836480
```

5. Start the new databend-query service.

```shell
cp ~/tmp/bin/databend-query bin/
./bin/databend-query --version # Confirm the version number
ulimit -n 65535
nohup bin/databend-query --config-file=configs/databend-query.toml 2>&1 >query.log &
```

## Step 2: Upgrade databend-meta

To upgrade the databend-meta service from version 0.8.xx, you need to use the binary file `databend-meta-upgrade-09`, which can be located in the `bin` folder of your current Databend installation directory.

Follow the steps below to upgrade the databend-meta service:

1. Shut down the running databend-meta service for all the clusters.

```shell
killall databend-meta
```

2. Execute the following command in each node:

```shell
./bin/databend-meta-upgrade-09 --cmd upgrade --raft-dir .databend/meta1/

███╗   ███╗███████╗████████╗ █████╗
████╗ ████║██╔════╝╚══██╔══╝██╔══██╗
██╔████╔██║█████╗     ██║   ███████║
██║╚██╔╝██║██╔══╝     ██║   ██╔══██║
██║ ╚═╝ ██║███████╗   ██║   ██║  ██║
╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝
--- Upgrade to 0.9 ---

config: {
"log_level": "INFO",
"cmd": "upgrade",
"raft_config": {
"raft_dir": ".databend/meta1/"
}
}
Start to write back converted record, tree: __sled__default, records count: 0
Done writing back converted record, tree: __sled__default
Start to write back converted record, tree: raft_log, records count: 7
Done writing back converted record, tree: raft_log
Start to write back converted record, tree: raft_state, records count: 0
Done writing back converted record, tree: raft_state
Start to write back converted record, tree: state_machine/0, records count: 12
Done writing back converted record, tree: state_machine/0
All converted
```

3. Replace the binary file `databend-meta` in the folder where Databend is installed with the newer version.

4. Start the new databend-meta service in each node.