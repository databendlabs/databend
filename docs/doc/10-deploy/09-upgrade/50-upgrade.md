---
title: Upgrade
sidebar_label: Upgrade query or meta
description:
  Upgrade databend-query or databend-meta without downtime
---

This guideline will introduce how to upgrade databend-query or databend-meta with new version.

## Check compatibility

Before upgrading, make sure the compatibility is still held:

- To find out the build version of databend-query and its compatible databend-meta version:

  ```shell
  databend-query --cmd ver

  # output:
  version: 0.7.61-nightly
  min-compatible-metasrv-version: 0.7.59
  ```

  Which means this build of databend-query(`0.7.61-nightly`) can talk to a databend-meta of at least version `0.7.59`, inclusive.

- To find out the build version of databend-meta and its compatible databend-query version:

  ```shell
  databend-meta --cmd ver

  # output:
  version: 0.7.61-nightly
  min-compatible-client-version: 0.7.57
  ```

  Which means this build of databend-meta(`0.7.61-nightly`) can talk to a databend-query of at least version `0.7.57`, inclusive.

A databend-query and databend-meta are compatible iff the following statements hold:

```
databend-query.version >= databend-meta.min-compatible-client-version
databend-bend.version  >= databend-query.min-compatible-metasrv-version
```

:::caution

If incompatible versions are deployed, an error `InvalidArgument` will occur when databend-query tries to connect to databend-meta,
which can be found in databend-query log.
Then databend-query will stop working.

:::

## Upgrade

- To Upgrade databend-query:

  Upgrading databend-query is simple since it is a stateless service.
  Just kill and re-start every node one by one:

  ```shell
  # Shutdown old binary
  killall databend-query

  # Bring up new binary
  databend-query -c ...
  ```

  Then make sure everything goes well by checking the databend-query log.

- To upgrade databend-meta:

  Only upgrading databend-meta node one by one.

  Since databend-meta is a stateful service and is consensus cluster,
  taking too many databend-meta nodes offline(e.g., 3 offline nodes in a cluster of 5) affects the availability.

  Kill and re-start every node one by one:

  ```shell
  # Shutdown old binary
  killall databend-meta

  # Bring up new binary
  databend-meta -c ...
  ```

  Then make sure everything goes well by checking the databend-query log and databend-meta log.
