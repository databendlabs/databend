---
title: Idempotent Copy
description: Avoid duplicating when copy stage files into a table
---

- Tracking Issue: https://github.com/datafuselabs/databend/issues/6338

## Summary

When streaming copy stage files into a table, there is a chance that some files have already been copied, So it needs some ways to avoid duplicate copying files, make it an `idempotent` operation.

## Save copy into table stage files meta information in meta service

Whenever copy stage files into a table, save the stage file meta information into the meta service:

- key: combined with `(tenant, database, table, file name)`.
- value: value MUST includes all the meta of a stage file, such as `content-length`,`etag`,`last modified`.



![](/img/rfc/20220909-Idempotent-Copy/stage-file-meta.png)



The expiration time of the stage file meta information is 64 days by default.

## Avoiding duplicates when copy stage files into a table

Using the stage file meta information, whenever copy stage files into a table, follow these steps:

* First, get all the table file meta information of the copy stage files that want to copy into the table(if any).
* Second, get all the stage file meta information.
* Third, compare the table file meta information with stage file meta information:
  * If they matched, this file is just ignored without copying.
  * Else, copy the stage file and up-insert into the table stage file meta.



![](/img/rfc/20220909-Idempotent-Copy/example.png)



Take the image above as an example:

* Client make a request to copy thress files (file1, file2, file3) into table.

* Get the table stage file meta of (file1, file2, file3).

* In the meta service, only found (file1,file3) stage file information.

* Compare the table stage file information with stage file information, and found that file1 has not been changed, so file1 will be ignored in this copy operation, and (file2,file3) will be copied.

* After copying new files, (file2, file3) stage file information will be saved into table file information.



