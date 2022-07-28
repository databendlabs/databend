---
title: Watch key support in MetaService
description: 
  Watch key support in MetaService
---

Metasrv uses GRPC stream to watch the key changes, users can add a watch key through `KVApi`, and the `WatchRequest` parameters include:

* key: the key to register for watching.
* key_end: `key_end`` is the end of the range [key, key_end) to watch.
If key_end is None, then watch only key. If want to watch prefix of key, use`get_start_and_end_of_prefix` to generate [key, key_end).
* filter_type: the filter type of the watch key change events, which is one of the following:
  * ALL: receives all kinds of update events.
  * UPDATE: filter only updates event.
  * DELETE: filter only deletes event.

Every time a watch request is received, a Grpc stream is created. Every time the watched key changes, metasrv uses the created stream to notify the client.

![](/img/rfc/20220728-metasrv-watch/watchstream.png)
