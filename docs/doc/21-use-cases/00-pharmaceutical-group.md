---
title: "Revolutionizing Data Archival and Query Performance for Pharmaceutical Group"
---

A prominent pharmaceutical retail chain laid its foundation in 1999, gradually evolving into a significant player. Accumulating substantial data volumes through expansive supply chain operations, the business managed extensive datasets, including tables with billions of records. As the enterprise embraced digital evolution, the imperative for efficient data utilization and intelligent technology solutions grew. However, a notable challenge emerged as their existing CDH ([Cloudera Distribution for Hadoop](https://www.cloudera.com/products/open-source/apache-hadoop/key-cdh-components.html)) necessitated an upgrade due to architectural reasons.

**Challenge Faced:**

With a growing data platform exceeding 30 terabytes, the CDH faced hardware limitations and escalating costs. The business needed a solution that could retain historical data for audit and tracing purposes, while also facilitating efficient data analysis.

**Technical Implementation:**

Historical data from CDH was exported as Parquet files using Tencent Cloud's COS migration tool. These backups were then transferred to [Tencent Cloud COS](https://www.tencentcloud.com/products/cos). The data seamlessly transitioned into Databend through the establishment of a storage stage connected to a cloud storage bucket. Following this, data files were extracted based on specific patterns, enabling the creation of new tables that use the files' structure. This data was then loaded into the tables, making use of parallel processing for enhanced efficiency.

![Alt text](../../public/img/usecase/cdh.png)

**Achieved Outcomes:**

Databend proved transformative for the Pharmaceutical Group. **The migration resulted in a 2x increase in query and loading speed for large table data. Storage costs on Tencent Cloud COS plummeted around 15x compared to CDH's local storage and replication costs. **

By embracing Databend, the Pharmaceutical Group achieved remarkable outcomes. The platform's user-friendly interactions, swift queries, and seamless transition for historical data queries significantly shortened project timelines, improved efficiency, and alleviated business concerns. In a rapidly evolving technological landscape, Databend emerged as a low-cost, high-performance solution, empowering companies like the Pharmaceutical Group to streamline data archiving, bolster query performance, and enhance business operations.