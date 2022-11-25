---
title: Deploying a Local Databend (for Non-Production Use)
sidebar_label: Deploying a Local Databend
description:
  Deploying a Local Databend
---

To evaluate Databend and get some hands-on experience, you can deploy Databend locally and use the file system as storage if you don't have object storage yet. This topic explains how to deploy a local Databend and connect to Databend from a MySQL client.

:::note
Object storage is required for production. Use the file system only for evaluation, testing, and other non-production purposes.
:::

## Before You Begin

Make sure you have installed a MySQL client.

## Download Databend

1. Go to https://databend.rs/download and download the installation package for your platform.

    If you're using a Mac with an Apple Silicon CPU, select the package named `databend-<version>-nightly-aarch64-apple-darwin.tar.gz`.

2. Extract the installation package to a local directory.

## Start Databend

1. Open a terminal and navigate to the folder where the extracted files and folders are stored.

2. Run the script `start.sh` in the folder `scripts`:

    MacOS might prompt an error saying "*databend-meta can't be opened because Apple cannot check it for malicious software.*". To proceed, open **System Settings** on your Mac, select **Privacy & Security** on the left menu, and click **Open Anyway** for databend-meta in the **Security** section on the right side. Do the same for the error on databend-query.

```shell
./scripts/start.sh
```

3. Run the following command to verify Databend has been started successfully:

```shell
ps aux | grep databend

---
eric             12789   0.0  0.0 408495808   1040 s003  U+    2:16pm   0:00.00 grep databend
eric             12781   0.0  0.5 408790416  38896 s003  S     2:15pm   0:00.05 bin/databend-query --config-file=configs/databend-query.toml
eric             12776   0.0  0.3 408654368  24848 s003  S     2:15pm   0:00.06 bin/databend-meta --config-file=configs/databend-meta.toml
```

## Connect to Databend

1. Create a connection from your MySQL client using port 3307:

```shell
mysql -h 127.0.0.1 -P3307 -uroot
```

2. Query the Databend version to verify the connection:

```sql
SELECT VERSION();

---
version()                                                                             |
--------------------------------------------------------------------------------------+
DatabendQuery v0.8.99-nightly-2fdfcaa(rust-1.66.0-nightly-2022-11-02T18:06:39.712775Z)|
```
