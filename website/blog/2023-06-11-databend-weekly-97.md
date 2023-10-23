---
title: "This Week in Databend #97"
date: 2023-06-11
slug: 2023-06-11-databend-weekly
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: b41sh
  - name: Chasen-Zhang
  - name: dantengsky
  - name: dependabot[bot]
  - name: Dousir9
  - name: drmingdrmer
  - name: JackTan25
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: tisonkun
  - name: Xuanwo
  - name: youngsofun
  - name: zhang2014
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Column Position

Databend now offers support for utilizing syntax like $N to represent column positions. For instance, $2 indicates the second column. Additionally, Databend allows the usage of column positions alongside column names in SQL statements. Here is a simple example:

```SQL
CREATE TABLE IF NOT EXISTS t1(a int, b varchar);
INSERT INTO t1 values (1, 'a'), (2, 'b');
select $1, $2, a, b from t1;

┌─────────────────────────────────┐
│   $1  │   $2   │   a   │    b   │
│ Int32 │ String │ Int32 │ String │
├───────┼────────┼───────┼────────┤
│     1 │ a      │     1 │ a      │
│     2 │ b      │     2 │ b      │
└─────────────────────────────────┘
```

You can also use column positions when you SELECT FROM a staged NDJSON file. We are also actively working on extending this support to other formats. When using the COPY INTO statement to copy data from a stage, Databend matches the field names at the top level of the NDJSON file with the column names in the destination table, rather than relying on column positions.

```sql
SELECT $1 FROM @my_stage (FILE_FORMAT=>'ndjson')

COPY INTO my_table FROM (SELECT $1 SELECT @my_stage t) FILE_FORMAT = (type = NDJSON)
```

It is important to note that when using the SELECT statement for NDJSON in Databend, only $1 is allowed, representing the entire row and having the data type variant.

```sql
-- Select the entire row using column position:
SELECT $1 FROM @my_stage (FILE_FORMAT=>'ndjson')

--Select a specific field named "a" using column position:
SELECT $1:a FROM @my_stage (FILE_FORMAT=>'ndjson')
```

If you are interested in learning more, please check out the resources listed below:

- [Issue | Feature: support $<col_position>](https://github.com/datafuselabs/databend/issues/11585)
- [Issue | Feature: copy/select from stage by pos](https://github.com/datafuselabs/databend/issues/11581)
- [PR | feat: support column position like $N](https://github.com/datafuselabs/databend/pull/11672)
- [PR | feat: select from stage support NDJson](https://github.com/datafuselabs/databend/pull/11701)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Learn Databend Workflows - Typos Check

Databend now has a very complex workflow for handling code auditing, testing, benchmarking and release. Typos Check is undoubtedly the simplest part of it. Let's take a look at some of its contents together.

Like other workflows, we need to use actions/checkout to check out the code.

```yaml
- uses: actions/checkout@v4
  with:
    clean: "true"
```

`typos-cli` is a source code spell checker that finds and corrects spelling mistakes in source code. It is fast enough to run on monorepos and has low false positives, making it suitable for use on PRs.

```yaml
- uses: baptiste0928/cargo-install@v1
  with:
    crate: typos-cli
    args: --locked
    cache-key: typos-check
```

We use `baptiste0928/cargo-install` to install dependencies. It is essentially the same as using `cargo install` in your GitHub workflows. Additionally, it allows for automatic caching of resulting binaries to speed up subsequent builds.

```yaml
- name: do typos check with typos-cli
  run: typos
```

One thing to note is that `typos-cli` is the name of the crate, but the corresponding executable binary name is `typos`.

If you are interested in learning more, please check out the resources listed below:

- [Workflows | typos.yml](https://github.com/datafuselabs/databend/blob/main/.github/workflows/typos.yml)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for distributed Top-N.
- The lazy_topn_threshold setting is now active by default, with a default value of 1,000.
- For enhanced security measures, the ability to change the password has been added to the root user.
- Read *[Blog | Databend X Tableau](https://databend.rs/blog/2023-06-01-tableau)* to learn how to connect Databend for BI data analysis in Tableau.
- Read *[Docs | Integrating Databend as a Sink for Vector](https://databend.rs/doc/load-data/load-db/vector)* to understand how to integrate Vector with Databend.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Add a Deduplication Label Field to the Rest API

To ensure that data ingestion is idempotent, Databend now supports deduplication of DML through the use of a deduplication label. You can find more information on this feature at [Docs | Setting Commands - SET_VAR](https://databend.rs/doc/sql-commands/setting-cmds/set-var).

To facilitate cross-language driver integration, we could add a REST API field for the label.

[Issue #11710 | Feature: support to bring deduplication label on stage attachment api](https://github.com/datafuselabs/databend/issues/11710)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.55-nightly...v1.1.56-draft2>
