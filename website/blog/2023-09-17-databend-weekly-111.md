---
title: "This Week in Databend #111"
date: 2023-09-17
slug: 2023-09-17-databend-weekly
cover_url: 'weekly/weekly-111.jpg'
image: 'weekly/weekly-111.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: Chasen-Zhang
  - name: ct20000901
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: gitccl
  - name: JackTan25
  - name: nagarajatantry
  - name: nange
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: ZhiHanZ
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Understanding SHARE ENDPOINT

SHARE ENDPOINT is an important concept in Databend data sharing, used to specify the endpoint and tenant name of a data sharing instance. The accessors and users of the data need to define SHARE POINT to help the Databend instance access the shared data.

For example, if Tenant A shares data with Tenant B, then Tenant B needs to create a corresponding SHARE ENDPOINT so that the instance where Tenant B is located can access the shared data.

```SQL
CREATE SHARE ENDPOINT IF NOT EXISTS from_TenantA
    URL = '<share_endpoint_url>'
    TENANT = A
    COMMENT = 'Share endpoint to access data from Tenant A';
```

If you are interested in learning more, please check out the resources listed below.

- [Docs | SHARE ENDPOINT](https://databend.rs/doc/sql-commands/ddl/share-endpoint/)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Using Python to create UDF

Databend now supports UDF Server, which allows users to implement more flexible and useful UDFs. By combining your preferred programming language with the Apache Arrow Flight API, you can interact with Databend and expand the capabilities of the database for richer and more efficient data workflows.

To enable this feature, you need to use version `v1.2.116-nightly` or later and enable UDF Server support in the configuration of `databend-query`:

```toml
[query]
...
enable_udf_server = true
# use your udf server address here
udf_server_allow_list = ['http://0.0.0.0:8815']
...
```

The following code snippet demonstrates how to create a UDF Server in Python for calculating the greatest common divisor.

```python
from udf import *

# Define a function that accpets nullable values, and set skip_null to True to enable it returns NULL if any argument is NULL.
@udf(
    input_types=["INT", "INT"],
    result_type="INT",
    skip_null=True,
)
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x

if __name__ == '__main__':
    # create a UDF server listening at '0.0.0.0:8815'
    server = UdfServer("0.0.0.0:8815")
    # add defined functions
    server.add_function(gcd)
    # start the UDF server
    server.serve()
```

In Databend, you can register UDF using the following SQL statement.

```SQL
CREATE FUNCTION gcd (INT, INT) RETURNS INT LANGUAGE python HANDLER = 'gcd' ADDRESS = 'http://0.0.0.0:8815';
```

This feature is currently in the preview stage. We offer a Python SDK file for demonstration and testing purposes. More SDKs will be released in the future. Feel free to join the UDF ecosystem.

If you are interested in learning more, please check out the resources listed below:

- [PR #12802 | feat: implement udf server in databend](https://github.com/datafuselabs/databend/issues/12802)
- [Docs | UDF Server](https://github.com/datafuselabs/databend/blob/main/tests/udf-server/README.md)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for infer filter.
- Added support for idempotent deletion.
- Supported the generation of aggregate functions, scalar functions, and expressions for SQLSmith testing.
- Read the documentation [Docs | INSERT INTO](https://databend.rs/doc/sql-commands/dml/dml-insert) and [Docs | REPLACE INTO](https://databend.rs/doc/sql-commands/dml/dml-replace) to learn how to use SQL statements to insert data from Stage into a table.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Providing Cache Support for Parquet Files in Stage

Databend offers different types of caching to enhance query performance. These include Query Cache, File Metadata Cache, and Data Cache.

However, there is currently a lack of effective cache support for files located in Stage. If metadata cache or object cache can be provided for Parquet files in Stage, it will help enhance the performance of querying external data.

```SQL
select * from 's3://aa/bb/cc/' (pattern => '.*.parquet')
```

[Issue #12762 | feat: add object cache for stage parquet file](https://github.com/datafuselabs/databend/issues/12762)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@nagarajatantry](https://github.com/nagarajatantry) made their first contribution in [#12836](https://github.com/datafuselabs/databend/pull/12836). Fixed the broken hyperlink in the document.
* [@ct20000901](https://github.com/ct20000901) made their first contribution in [#12827](https://github.com/datafuselabs/databend/pull/12827). Fixed the issue with `array_sort` not correctly handling empty arrays and `NULL`.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.109-nightly...v1.2.116-nightly>
