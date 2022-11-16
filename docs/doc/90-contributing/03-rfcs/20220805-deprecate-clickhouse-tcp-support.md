---
title: Deprecate ClickHouse's tcp support
description: 
  RFC for deprecate ClickHouse's tcp support
---

## Summary

Databend now supports ClickHouse's tcp protocol but it's limited implementation. Many users gave us feedback that `clickhouse-client` did not work well with databend. Usually, we told them to use MySQL instead.

`clickhouse-client` is a very excellent and powerful tool to work with `ClickHouse-server`. Yet it has some limitations if we integrate it with databend using tcp protocol.

1. ClickHouse's tcp protocol is biased towards the bottom layer of ClickHouse. The historical burden inside is relatively heavy, and it wastes a lot of energy to troubleshoot problems because it's not our focus point.

2. There is no one-to-one correspondence between the type system and the databend type system and it's costable to convert blocks between databend and ClickHouse. For example, the DateTime type ClickHouse has two sets, and the json/variant type and the ClickHouse binary format are also inconsistent.

3. `clickhouse-client` will do SQL parsing, syntax/functions validations, data deserialization and serialization on the client side. But there are many differences between databend and ClickHouse. It's really hard for us to make them full compatible.

4. The ClickHouse's tcp protocol will continue to be upgraded in the future. We do not have the energy to be compatible with the new version. For example, in https://github.com/datafuselabs/databend/issues/6951 the old version can work, but the new version has some compatibility issues that are more difficult to check.

5. We alreay supported ClickHouse http protocol, MySQL protocol and databend's own HTTP handler. It's time to shrink the meaningless endpoints to make the other useful endpoints better.


So we plan to deprecate ClickHouse's tcp support and focus on compatibility with ClickHouse http protocol.

