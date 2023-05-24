---
title: File Upload API
sidebar_label: File Upload API
description:
  Uploads local data files to the user stage or a named internal / external stage.
---

You can utilize the File Upload API to upload local data files to a stage, whether it is the user stage or a named internal/external stage. To initiate the upload process, you need to submit a POST request to the endpoint `http://<http_handler_host>:<http_handler_port>/v1/upload_to_stage`.

:::tip
By default, <http_handler_host>:<http_handler_port> is set to 0.0.0.0:8000, and it can be customized in the databend-query.toml configuration file.
:::

In the body of the POST request, include the [Required Parameters](#required-parameters) to specify the target stage and the files you want to upload. Once the server receives your request, it will handle the upload process and respond with a JSON object that indicates the upload result.

## Required Parameters

| Parameter  | Description |
| ----------- | ----------- |
| `stage_name:<your-stage-name>`  | Stage name. To specify a stage named **sales**, use "stage_name:sales". For the user stage, use "stage_name:~". |
| `upload=@<your-file-path>`  | File you want to upload.|

## Examples

See examples in [Staging Files](../../12-load-data/00-stage/01-manage-stages.md).