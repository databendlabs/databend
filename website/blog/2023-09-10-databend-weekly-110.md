---
title: "This Week in Databend #110"
date: 2023-09-10
slug: 2023-09-10-databend-weekly
cover_url: 'weekly/weekly-110.jpg'
image: 'weekly/weekly-110.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: lichuang
  - name: nange
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: ZhiHanZ
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Using BendSQL to Manage Files in Stage

Databend recommends uploading files to a stage with`PRESIGN` or `PUT / GET` command. These methods enable direct file transfer between the client and your storage, eliminating intermediaries and resulting in cost savings by reducing traffic between Databend and your storage.

![](https://databend.rs/assets/images/staging-file-ff259b7f65e99faec0ca69fcdf732512.png)

If you're using BendSQL to manage files in a stage, you can use the PUT command for uploading files and the GET command for downloading files.

```SQL
root@localhost:8000/default> PUT fs:///books.parquet @~

PUT fs:///books.parquet @~

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /books.parquet                       │ SUCCESS │
└───────────────────────────────────────────────┘

GET @~/ fs:///fromStage/

┌─────────────────────────────────────────────────────────┐
│                      file                     │  status │
│                     String                    │  String │
├───────────────────────────────────────────────┼─────────┤
│ /fromStage/books.parquet                      │ SUCCESS │
└─────────────────────────────────────────────────────────┘
```

If you are interested in learning more, please check out the resources listed below.

- [Docs | Staging Files](https://databend.rs/doc/load-data/stage/stage-files)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Using Databend Python Binding in Jupyter Notebook

Databend provides Python Binding, allowing you to access Databend's features without deploying a Databend instance. Its DataFrames can also be easily converted to the Polars and Pandas formats, facilitating integrations with common data science tools.

To use the Databend Python Binding, simply run the following command to install the library:

```bash
pip install databend
```

The code below demonstrates how to use Databend Python Binding in Jupyter Notebook and plot a bar chart using `matplotlib`.

```python
# Create a table in Databend
ctx.sql("CREATE TABLE IF NOT EXISTS user (created_at Date, count Int32)")

# Create a table in Databend
ctx.sql("CREATE TABLE IF NOT EXISTS user (created_at Date, count Int32)")

# Insert multiple rows into the table
ctx.sql("INSERT INTO user VALUES ('2022-04-01', 5), ('2022-04-01', 3), ('2022-04-03', 4), ('2022-04-03', 1), ('2022-04-04', 10)")

# Execute a query
result = ctx.sql("SELECT created_at as date, count(*) as count FROM user GROUP BY created_at")

# Display the query result
result.show()

# Import libraries for data visualization
import matplotlib.pyplot as plt

# Convert the query result to a Pandas DataFrame
df = result.to_pandas()

# Create a bar chart to visualize the data
df.plot.bar(x='date', y='count')
plt.show()
```

![](https://databend.rs/assets/images/localhost_8888_notebooks_Untitled.ipynb-3992998c3577fa62ff83a36bcee494f8.png)

If you are interested in learning more, please check out the resources listed below:

- [Docs | Tutorial-3: Integrate with Jupyter Notebook with Python Binding Library](https://databend.rs/doc/visualize/jupyter#tutorial-3-integrate-with-jupyter-notebook-with-python-binding-library)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added initial support for ownership model.
- Added support for Hash Join spill.
- Columns in Databend are now nullable by default. Please refer to the documentation [Docs | NULL Values and NOT NULL Constraint](https://databend.rs/doc/sql-reference/data-types/#null-values-and-not-null-constraint) for more details.
- Read the document [Docs | databend-local](https://databend.rs/doc/sql-clients/databend-local) to learn the Databend Local mode.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Creating UDFs in JavaScript

[PR #12729 | feat: implement udf server in databend](https://github.com/datafuselabs/databend/pull/12729) is expected to be merged this week. This means that Databend will soon support creating user-defined functions in Python.

```SQL
CREATE FUNCTION [IF NOT EXISTS] <udf_name> (<arg_type>, ...) RETURNS <return_type> LANGUAGE <language> HANDLER=<handler> ADDRESS=<udf_server_address>
```

We will provide support for UDFs created in various languages, with JavaScript being the next one in line.

[Issue #12746 | Feature: support javascript udf ](https://github.com/datafuselabs/databend/issues/12746)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.96-nightly...v1.2.109-nightly>
