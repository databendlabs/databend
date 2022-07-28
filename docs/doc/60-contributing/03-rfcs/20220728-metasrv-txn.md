---
title: Transaction support in MetaService
description: 
  Transaction support in MetaService
---

MetaService support transaction with `KVApi`, transaction parameters are:

* condition: `condition` is a list of predicates. If all of them success, the `if_then` will be executed, otherwise `else_then` op will be executed.
* if_then: `if_then` is a list of operations will be executed when all condition evaluates to true.
* else_then: `else_then` is a list of operations will be executed when not all condition evaluates to true.

The `condition` is a list of `TxnCondition`, which includes these fields:

* key: String format key to compared with.
* target: The target of key value to be compared with, it can be the last success upsert operation sequence number of the key, or the bytes value of the key.
* expected: The expected result of condition, result includes `EQ`, `GT`, etc.

The `is_then` and `else_then` is a list of `TxnOp`, which is one of these types:

* TxnGetRequest get: Get the value and sequence of a key.
* TxnPutRequest put: Put the value of a key.
* TxnDeleteRequest delete: Delete a key.
* TxnDeleteByPrefixRequest delete_by_prefix: Delete all the keys with the prefix string.
