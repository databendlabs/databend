---
title: "This Week in Databend #106"
date: 2023-08-13
slug: 2023-08-13-databend-weekly
cover_url: 'weekly/weekly-106.jpg'
image: 'weekly/weekly-106.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: hantmac
  - name: JackTan25
  - name: lichuang
  - name: nange
  - name: parkma99
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: Xuanwo
  - name: xudong963
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Masking Policy

A masking policy refers to rules and settings that control the display or access to sensitive data in a way that safeguards confidentiality while allowing authorized users to interact with the data. Databend enables you to define masking policies for displaying sensitive columns in a table, thus protecting confidential data while still permitting authorized roles to access specific parts of the data.

```sql
-- Create a masking policy
CREATE MASKING POLICY email_mask
AS
  (val string)
  RETURNS string ->
  CASE
  WHEN current_role() IN ('MANAGERS') THEN
    val
  ELSE
    '*********'
  END
  COMMENT = 'hide_email';

-- Associate the masking policy with the 'email' column
ALTER TABLE user_info MODIFY COLUMN email SET MASKING POLICY email_mask;
```

`Masking Policy` requires **Enterprise Edition**. To inquire about upgrading, please contact [Databend Support](https://www.databend.com/contact-us).

If you are interested in learning more, please check out the resources listed below.

- [Docs | MASKING POLICY](https://databend.rs/doc/sql-commands/ddl/mask-policy/)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Adding `show()` Method to Python Binding

In Python packages such as PySpark, DuckDB, and DataFusion, the `show()` method is supported for outputting the first `n` rows of results.

Recently, Databend has also implemented corresponding support for Python binding through PyO3. The code snippet is as follows:

```rust
#[pyo3(signature = (num=20))]
fn show(&self, py: Python, num: usize) -> PyResult<()> {
    let blocks = self.collect(py)?;
    let bs = self.get_box();
    let result = blocks.box_render(num, bs.bs_max_width, bs.bs_max_width);

    // Note that println! does not print to the Python debug console and is not visible in notebooks for instance
    let print = py.import("builtins")?.getattr("print")?;
    print.call1((result,))?;
    Ok(())
}
```

If you are interested in learning more, please check out the resources listed below:

- [Issue #12255 | For Python compatibility, please add show() method](https://github.com/datafuselabs/databend/issues/12255)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for distributed `REPLACE INTO`.
- Added support for the operator `<->` to calculate the L2 norm (Euclidean norm) of a vector.
- Added Geo functions: `h3_to_center_child`, `h3_exact_edge_length_m`, `h3_exact_edge_length_km`, `h3_exact_edge_length_rads`, `h3_num_hexagons`, `h3_line`, `h3_distance`, `h3_hex_ring` and `h3_get_unidirectional_edge`.
- Read document [Docs | ALTER TABLE COLUMN](https://databend.rs/doc/sql-commands/ddl/table/alter-table-column) to learn how to modify a table by adding, converting, renaming, changing, or removing a column.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Adding Storage Backend Support for Hive Catalog

Previously, Databend's implementation of the Hive Catalog lacked support for configuring its own storage backend and could only fall back to the storage backend corresponding to the Default Catalog. This caused issues when the storage service pointed to by Hive MetaStore was inconsistent with the configuration of Default Catalog, resulting in an inability to read data.

Now there are plans to introduce `CONNECTION` options for Hive Catalog, allowing configuration of the storage backend and resolving issues with heterogeneous storage.

```sql
CREATE CATALOG hive_ctl
TYPE=HIVE
HMS_ADDRESS='127.0.0.1:9083'
CONNECTION=(
    URL='s3://warehouse/'
    AWS_KEY_ID='admin'
    AWS_SECRET_KEY='password'
    ENDPOINT_URL='http://localhost:9000'
);
```

[Issue #12407 | Feature: Add storage support for Hive catalog](https://github.com/datafuselabs/databend/issues/12407)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@parkma99](https://github.com/parkma99) made their first contribution in [#12341](https://github.com/datafuselabs/databend/pull/12341). Fixed parsing issue of `CREATE ROLE`.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.51-nightly...v1.2.62-nightly>
