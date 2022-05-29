---
title: Query-Meta Compatibility
sidebar_label: Query-Meta Compatibility
description:
  Investigate and manage the compatibility between databend-query and databend-meta
---

This guideline will introduce how to investigate and manage the compatibility between databend-query and databend-meta.

## Find out the versions

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

## Deploy compatible versions of databend-query and databend-meta

A databend cluster has to be deployed with compatible versions of databend-query and databend-meta.
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

### How compatibility is checked

Compatibility will be checked when a connection is established between meta-client(databend-query) and databend-meta, in a `handshake` RPC.

The client `C`(databend-query) and the server `S`(databend-meta) maintains two semantic-version:

- `C` maintains the its own semver(`C.ver`) and the minimal compatible `S` semver(`C.min_srv_ver`).
- `S` maintains the its own semver(`S.ver`) and the minimal compatible `S` semver(`S.min_cli_ver`).

When handshaking:

- `C` sends its ver `C.ver` to `S`,
- When `S` receives handshake request, `S` asserts that `C.ver >= S.min_cli_ver`.
- Then `S` replies handshake-reply with its `S.ver`.
- When `C` receives the reply, `C` asserts that `S.ver >= C.min_srv_ver`.

Handshake succeeds if both of these two assertions hold.

E.g.:
- `S: (ver=3, min_cli_ver=1)` is compatible with `C: (ver=3, min_srv_ver=2)`.
- `S: (ver=4, min_cli_ver=4)` is **NOT** compatible with `C: (ver=3, min_srv_ver=2)`.
  Because although `S.ver(4) >= C.min_srv_ver(3)` holds,
  but `C.ver(3) >= S.min_cli_ver(4)` does not hold.

```text
C.ver:    1             3      4
C --------+-------------+------+------------>
          ^      .------'      ^
          |      |             |
          '-------------.      |
                 |      |      |
                 v      |      |
S ---------------+------+------+------------>
S.ver:           2      3      4
```
