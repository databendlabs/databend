---
title: "This Week in Databend #114"
date: 2023-10-08
slug: 2023-10-08-databend-weekly
cover_url: 'weekly/weekly-114.jpg'
image: 'weekly/weekly-114.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTang
  - name: dantengsky
  - name: dependabot[bot]
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: xudong963
  - name: youngsofun
  - name: zenus
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### MERGE

The `MERGE` statement performs `INSERT`, `UPDATE`, or `DELETE` operations on rows within a target table, all in accordance with conditions and matching criteria specified within the statement, using data from a specified source.

![](https://databend.rs/assets/images/merge-into-single-clause-96616a67419ab6991f8cb16526c81d4b.jpeg)

A `MERGE` statement usually contains a `MATCHED` and / or a `NOT MATCHED` clause, instructing Databend on how to handle matched and unmatched scenarios. For a `MATCHED` clause, you have the option to choose between performing an `UPDATE` or `DELETE` operation on the target table. Conversely, in the case of a `NOT MATCHED` clause, the available choice is `INSERT`.

![](https://databend.rs/assets/images/merge-into-multi-clause-a0013edb335b7f6c45f9b338a68c50c8.jpeg)

```SQL
-- Merge data into 'salaries' based on employee details from 'employees'
MERGE INTO salaries
USING (SELECT * FROM employees)
ON salaries.employee_id = employees.employee_id
WHEN MATCHED AND employees.department = 'HR' THEN
    UPDATE SET
        salaries.salary = salaries.salary + 1000.00
WHEN MATCHED THEN
    UPDATE SET
        salaries.salary = salaries.salary + 500.00
WHEN NOT MATCHED THEN
    INSERT (employee_id, salary)
    VALUES (employees.employee_id, 55000.00);
```

> `MERGE` is currently in an experimental state. Before using the `MERGE` command, you need to run `SET enable_experimental_merge_into = 1;` to enable the feature.

If you are interested in learning more, please check out the resources below:

- [Docs | MERGE](https://databend.rs/doc/sql-commands/dml/dml-merge)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Introducing `DATABEND_DATA_PATH` to Python Binding and Local Mode

Databend's local mode now allows users to control the storage location of metadata and data files by setting the `DATABEND_DATA_PATH` environment variable.

```shell
DATABEND_DATA_PATH=/tmp/data/ databend-query local -q "create table abc(a int); insert into abc values(3);"
```

`DATABEND_DATA_PATH` also works with Databend Python Binding, but it must be defined before using `databend`.

```python
import os

os.environ["DATABEND_DATA_PATH"] = "/tmp/def/"

from databend import SessionContext
```

If you are interested in learning more, please check out the resources below:

- [PR #13089 | feat: support DATABEND_DATA_PATH in bendpy and databend-local](https://github.com/datafuselabs/databend/pull/13089)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Improved Hash Join, resulting in a 10% performance improvement in certain scenarios.
- Enhanced distributed execution of MERGE.
- Improved CI by using `quickinstall` to install relevant binary tools and executing unit tests with `nextest`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Delete Files When Dropping Internal Stage

In Databend, an Internal Stage stores data files in the storage backend specified in `databend-query.toml`. 

Considering that users will not be able to access the staged files after dropping an Internal Stage, it is necessary to remove the staged files when dropping the Internal Stage.

[Issue #12986 | remove files at the same time of drop internal stage](https://github.com/datafuselabs/databend/issues/12986)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.137-nightly...v1.2.147-nightly>
