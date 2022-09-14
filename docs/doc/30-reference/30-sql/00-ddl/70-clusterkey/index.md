---
title: What is a Cluster Key?
sidebar_position: 1
slug: ./
---

The cluster key is a data object for tables in Databend. It explicitly tells Databend how to divide and group rows of a table into the storage partitions rather than using the data ingestion order. 

A table's cluster key is usually one or more columns or expressions. If you define a cluster key for a table, Databend reorganizes your data based on the cluster key and stores similar rows into the same or adjacent storage partitions.

The benefit of defining a cluster key is optimizing the query performance.   The cluster key acts as a link between the metadata in the Databend's Meta Service Layer and the storage partitions. After the cluster key is defined for a table, the table's metadata implements a key-value-like list that shows the correspondences between the column or expression values and their storage partitions. When a query comes, Databend can quickly locate the storage partition by the metadata and fetch the results. To make this work, the cluster key you set must match the way how you filter the data in queries. For example, if you're most likely to query a table that holds all the employees' profile information by their first names, set the cluster key to the first name column.

In Databend, you [SET CLUSTER KEY](dml-set-cluster-key.md) when you create a table, and you can [ALTER CLUSTER KEY](https://databend.rs/doc/reference/sql/ddl/clusterkey/dml-alter-cluster-key) if necessary. A fully-clustered table might become chaotic if it continues to have ingestion or Data Manipulation Language operations (such as INSERT, UPDATE, DELETE), you will need to [RECLUSTER TABLE](./dml-recluster-table.md) to fix the chaos.

It's important to note that, most of the time you do not need to set the cluster key. Clustering or re-clustering a table consumes time and your credits if you're in Databend Cloud. Databend recommends setting cluster keys for large tables with slow query issues.