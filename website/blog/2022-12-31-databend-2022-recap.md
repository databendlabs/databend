---
title: 'Databend 2022 Recap'
date: 2022-12-31
slug: 2022-12-31-databend-2022-recap
tags: [weekly]
authors:
- name: PsiACE
  url: https://github.com/psiace
  image_url: https://github.com/psiace.png
---

The year is coming to an end, and Databend is about to enter its third year. Before we count down the new year, it's a good idea to look back and see how Databend did in 2022.

### Open Source: Receiving Increased Attention

[Databend](https://github.com/datafuselabs/databend) is a powerful cloud data warehouse. Built for elasticity and efficiency. Free and open. Also available in the cloud: <https://app.databend.com> .

![databend](https://user-images.githubusercontent.com/172204/193307982-a286c574-80ef-41de-b52f-1b064ae7fccd.png)

The open-source philosophy has guided Databend from the very beginning. The entire team works seamlessly on GitHub where the Rust community and many data pros are fully involved. In 2022, the Databend repository:

- Got **2,000+** stars, totaling **5,000** .
- Merged **2,400+** PRs, totaling **5,600** .
- Solved **1,900** issues, totaling **3,000** .
- Received **16,000** commits, totaling **23,000** .
- Attracted more contributors, totaling **138** .


## Development: Inspired by Real Scenarios

Databend brought many new features and improvements in 2022 to help customers with their real work scenarios.

![databend arch](https://user-images.githubusercontent.com/172204/181448994-2b7c1623-6b20-4398-8917-45acca95ba90.png)

### Brand-New Data Warehouse

As a data warehouse inspired by and benchmarking itself against Snowflake and Clickhouse, Databend fully took advantage of "Cloud Native" to bring you a new design and implementation without breaking the balance between performance and maintainability:

- Added support for [Stage](https://databend.rs/doc/reference/sql/ddl/stage/) and [Data Sharing](https://databend.rs/doc/sql-commands/ddl/share/), helping users manage their data life cycle with more options.
- Introduced a [new planner](https://databend.rs/blog/new-planner) with user-friendly error prompts and efficient optimization techniques for the execution plan.
- [Redesigned the type system](https://github.com/datafuselabs/databend/discussions/5438) to support type checking and type-safe downward casting.
- Enhanced the new processor framework: It can now work in both Pull and Push modes.
- Added experimental support for [Native Format](https://github.com/sundy-li/pa) to improve performance when running on a local disk.

### Databend as Lakehouse

Storing and managing massive data is key to our vision "[Databend as Lakehouse](https://github.com/datafuselabs/databend/issues/7592)" . A lot of efforts have been made in 2022 for a larger data payload and a wider range of accepted data sources:

- Adopted OpenDAL in the data access layer as a unified interface.
- Expanded support for structured and [semi-structured](https://databend.rs/doc/contributing/rfcs/semi-structured-data-types) data.
- Added the ability to keep [multiple catalogs](https://databend.rs/doc/contributing/rfcs/multiple-catalog): This makes integrations with custom catalogs such as Hive much easier.
- Added the ability to query data directly from a local, staged, or remote file.

### Optimal Efficiency Ratio

After a year of continuous tuning, we brought Databend to a new stage featuring elastic scheduling and separating storage from compute. We're thrilled to see a significant improvement in the efficiency ratio:

- In some scenarios, Databend works as efficiently as Clickhouse.
- Lowered costs by 90% compared to Elasticsearch, and by 30% compared to Clickhouse.

## Testing: Put Us at Ease

Comprehensive tests help make a database management system robust. While optimizing performance, we also care about the accuracy and reproducibility of SQL results returned from Databend.

![databend perf](/img/blog/databend-perf.png)

### Correctness Testing

In 2022, we replaced stateless tests with [SQL Logic Tests](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) for Databend in the first place. We also migrated a large number of mature test cases to cover as many scenarios as possible. Afterward, we started to use a Rust native test program called [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) instead of the previous Python one, which saved us a lot of time on CI without losing the maintainability of the tests.

Furthermore, we also planned and implemented three types of automated testing (TLP, QPS, and NoREC) supported by [SQLancer](https://github.com/sqlancer/sqlancer). All of them have been successfully merged into the main branch with dozens of bug fixes.

### Performance Testing

Performance testing is also essential for us. In 2022, we launched a website (<https://perf.databend.rs/>) to track daily performance changes and spot potential issues. Meanwhile, we actively evaluated Databend against [Clickbench](https://benchmark.clickhouse.com/) and some other benchmarks.

## Ecosystem: Give and Take

The Databend ecosystem and users benefit from each other. More and more users were attracted to the ecosystem and joined the community in 2022. As they brought their own creative ideas to Databend and made them come true, the Databend ecosystem made tremendous progress and started to flourish in the field.

### Positive Expansion

We build and value the Databend ecosystem. Databend is now compatible with the MySQL protocol and Clickhouse HTTP Handler, and can seamlessly integrate with the following data services or utilities:

- Airbyte
- DBT
- Addax (Datax)
- Vector
- Jupyter Notebook
- DBeaver

To help users develop and customize services based on Databend, we developed drivers in multiple languages, including Python and Go.

### Growing with Users

Users are the basis of Databend. They help develop Databend and stir up the whole community.

In 2022, Databend added support for the Hive Catalog with the help of **Kuaishou Technology**. This connected Databend to the Hive ecosystem and encouraged us to consider the possibility of multiple catalogs. **DMALL** implemented and verified data archiving with Databend. We also appreciate **SHAREit**, **Voyance**, **DigiFinex**, and **Weimob** for their trust and support.

The Databend ecosystem includes a few projects that are loved and trusted by other products:

- [OpenDAL](https://github.com/datafuselabs/opendal) now manages the data access layer for [sccache](https://github.com/mozilla/sccache) , which provides further support for **Firefox CI** . Other database and data analysis projects, such as [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) and [deepeth/mars](https://github.com/deepeth/mars) , also used OpenDAL for data access.
- [OpenRaft](https://github.com/datafuselabs/openraft) was used to implement a Feature Registry (database to hold feature metadata) in [Azure/Feathr](https://github.com/Azure/Feathr). **SAP**, **Huobi**, and **Meituan** also used it in a few internal projects.
- The MySQL protocol implementation in [OpenSrv](https://github.com/datafuselabs/opensrv) has been applied to multiple database projects such as [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) and [CeresDB](https://github.com/CeresDB/ceresdb) .

### Knowledge Sharing

In 2022, the Databend community launched the "Data Infra Club" for knowledge sharing. Our friends from **PingCAP**, **Kuaishou Technology**, **DMALL**, and **SHAREit** were invited to share their insights on big data platforms, Data Mesh, and Modern Data Stack. You can find all the video replays on Bilibili if you're interested.

## Going Cloud: Sky's the Limit

Going cloud is part of Databend's business strategy where most Databend users come from the cloud.

Built on top of Databend, **Databend Cloud** is a big-data analytics platform of the next generation, featuring *easy-to-use* , *low-cost* , and *high-performance* . Two versions of Databend Cloud are now available and open for trial:

- <https://databend.com> (for international users)
- <https://databend.cn>  (for China users only)
