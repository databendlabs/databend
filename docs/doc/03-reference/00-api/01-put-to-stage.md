---
title: PUT to Stage
sidebar_label: PUT to Stage
description:
  Uploads data files from a local file system directory/folder on a client machine to Databend named internal stages.
---

## Overview

Uploads data files from a local file system directory/folder on a client machine to Databend named internal stages.


## HTTP Methods

* A POST to `/v1/upload_to_stage` uploads the file to the server stage with stream in the POST body, and returns a JSON containing the query status.

## Quick Example

```sql
curl  -H "stage_name:mystage" -F "upload=@myfile.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```
