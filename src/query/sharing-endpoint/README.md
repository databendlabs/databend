# Open Sharing
Open Sharing is a cheap and secure data sharing protocol for databend query on multi-cloud environments.

## Features
* **Cheap**: Open Sharing allow data sharing via simple RESTful API sharing protocol, which is cheap and easy to understand.
* **Secure**: Open Sharing protocol would verify allow incoming requesters identity and access permission and provide audit log.
* **Multi-cloud**: Open Sharing is designed to work with different cloud platforms, including AWS, Azure, GCP, etc.
* **Open source**: Open Sharing is an open source project

## Protocol
Databend Sharing protocol is a RESTful protocol, which would understand database and table semantics for data sharing.
and provide short lived presigned url for data under the table.

For detailed descriptions, please take a look at
- [`protocol`](./protocol.md) -> provides detailed descriptions on sharing protocol api

## How to build?

To build open-sharing for stateful tests, run the following command
```bash
cargo build --bin open-sharing
```

## How to use it?

### Setup the sharing endpoint

Please ensure that the sharing endpoint has read and list access for the bucket being shared.

configure the sharing endpoint
```bash
export STORAGE_TYPE=s3
export STORAGE_S3_REGION=<the shared bucket region>
export STORAGE_S3_BUCKET=<the shared bucket name>
export STORAGE_S3_ACCESS_KEY_ID=<the shared bucket access key id>
export STORAGE_S3_SECRET_ACCESS_KEY=<the shared bucket secret access key>
export STORAGE_S3_ROOT=<the shared bucket root path>
export TENANT_ID=<the tenant id which shares the table>
./open-sharing
```

### Setup the sharing endpoint address for databend query

add `share_endpoint_address` field on your databend query config file

```toml
# Usage:
# databend-query -c databend_query_config_spec.toml
[query]
...
share_endpoint_address = "127.0.0.1:13003" # receive shared information from open sharing
...
```

### How to share a table?

For the tenant who wants to share the table `table1` from database `db1` to tenant `vendor`

```sql
CREATE SHARE myshare;
GRANT USAGE ON DATABASE db1 TO SHARE myshare;
GRANT SELECT ON TABLE db1.table1 TO SHARE myshare;
ALTER SHARE myshare ADD TENANTS = vendor;
```

From tenant `vendor` side, the table `db1.table1` would be visible and can be queried.

```sql
CREATE DATABASE db2 FROM SHARE myshare;
SELECT * FROM db2.table1;
```

## How to contribute?

For code changes feel free to open a PR and add necessary unit tests and integration tests.

For **API** changes, please follow the following steps:
1. provide a RFC to explain the reason why we need the additional api or why we need to change the existing api.
2. update the protocol.md to reflect the changes.
3. update the implementation to reflect the changes.
