---
title: Transaction support in databend-meta
description: 
  Transaction support in databend-meta
---

Databend-meta support transaction with `KVApi`, transaction parameters are:

* condition: `condition` is a list of predicates. If all of them succeed, the `if_then` will be executed, otherwise `else_then` op will be executed.
* if_then: `if_then` is a list of operations that will be executed when all condition evaluates to be true.
* else_then: `else_then` is a list of operations that will be executed when not all condition evaluates to be true.

The `condition` is a list of `TxnCondition`, which includes these fields:

* key: String format key to compare with.
* target: The target of key value to be compared with, can be the last success upsert operation sequence number of the key, or the bytes value of the key.
* expected: The expected result of condition, the result includes `EQ`, `GT`, etc.

The `is_then` and `else_then` is a list of `TxnOp`, which is one of these types:

* TxnGetRequest get: Get the value and sequence of a key.
* TxnPutRequest put: Put the value of a key.
* TxnDeleteRequest delete: Delete a key.
* TxnDeleteByPrefixRequest delete_by_prefix: Delete all the keys with the prefix string.
