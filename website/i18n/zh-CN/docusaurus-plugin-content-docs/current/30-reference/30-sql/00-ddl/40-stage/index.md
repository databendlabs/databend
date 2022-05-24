---
title: What are Databend Stages
sidebar_position: 1
slug: ./
---

## What are Databend Stages?

Databend can both store data locally(Fuse engine) and access data stored in other storage systems, the location where data is saved or from is known as a **Stage**.

## What are the Types of Databend Stages?

Databend's data can be stored internally or externally, based on this, the Databend Stages are categorized into two types:

* Internal Stages
* External Stages

### Internal Stages

In Internal Databend Stages, the data is stored internally, the data location likes: `/<bucket>/<tenant_id>/stage/<stage_name>/<stage_file>`

:::tip Databend table data path: `/<bucket>/<tenant_id>/<database_id>/<table_id>/` :::

### External Stages

**External Stages** are storage in another external location that is not part of the Databend, this might be Amazon S3 Storage or MySQL/HDFS. 


