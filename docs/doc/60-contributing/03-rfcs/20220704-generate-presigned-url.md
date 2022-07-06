---
title: Presign
description: Add new SQL statement for PRESIGN
---

- RFC PR: [datafuselabs/databend#0000](https://github.com/datafuselabs/databend/pull/0000)
- Tracking Issue: [datafuselabs/databend#0000](https://github.com/datafuselabs/databend/issues/0000)

# Summary

Add new SQL statement for `PRESIGN`, so users can generate a presigned url for uploading or downloading.

# Motivation

Databend supports [loading data](https://databend.rs/doc/load-data) via internal stage:

- Call HTTP API `upload_to_stage` to upload files: `curl -H "stage_name:my_int_stage" -F "upload=@./books.csv" -XPUT http://localhost:8081/v1/upload_to_stage`
- Call `COPY INTO` to copy data: `COPY INTO books FROM '@my_int_stage'`

This workflow's throughout is limited by databend's HTTP API: `upload_to_stage`. We can improve the throughout via allowing users to upload to our backend storage directly. For example, we can use [AWS Authenticating Requests: Using Query Parameters](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html) to generate a presigned URL. In this way, users upload content to aws s3 directly without go through databend.




# Guide-level explanation

TBD

# Reference-level explanation

TBD

# Drawbacks

TBD

# Rationale and alternatives

TBD

# Prior art

TBD

# Unresolved questions

TBD

# Future possibilities

TBD
