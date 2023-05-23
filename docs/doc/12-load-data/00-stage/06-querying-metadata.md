---
title: Querying Metadata for Staged Files
---

Databend also provides a capability for querying metadata through a function called INFER_SCHEMA. This function allows users to retrieve metadata from staged data files in Databend. By leveraging INFER_SCHEMA, users can query and load metadata into tables along with regular data columns, similar to Snowflake's approach. This enables seamless querying and analysis of both the metadata and the actual data stored in staged files within Databend.


:::note
This feature is currently only available for the Parquet file format.
:::