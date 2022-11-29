---
title: Time Travel - Going Back to Move Forward
description: time travel
slug: time-travel2
date: 2022-10-10
tags: [databend, time travel]
authors:
- name: wubx
  url: https://github.com/wubx
  image_url: https://github.com/wubx.png
---

Life is a journey, sometimes you must go back to move forward. When working with databases, you need to access the data history now and then. Time Travel is one of the most valuable features that Databend has rolled out. The time travel feature acts as a data recovery utility that enables you to restore a dropped table or get back a previous version of your data in a table. For example, when you accidentally delete a table or update some rows by mistake, you will need the help from Time Travel. This post sheds some light on what you can do with Time Travel in Databend.

![](/img/blog/timetravel.png)

First things first, you must know that not all the historical data can be restored based on the Databend retention policy. The default retention period is 24 hours, which means you can restore your historical data within 24 hours after it is deleted or outdated.

If you run SHOW TABLES HISTORY against a database, you will find the dropped tables (if any) and their drop time. The command does not list the dropped tables that have passed their retention period.

## Restore a Dropped Table

When you delete a file from your computer, the file goes to the trash bin and you can restore the file by putting it back to its original folder.

In Databend, restoring a table is as easy as you restore a file from the trash bin. The [UNDROP TABLE](https://databend.rs/doc/reference/sql/ddl/table/ddl-undrop-table) command makes a dropped table become available again with the data of the latest version. The "latest version" means that Databend recovers a table as well as the data that the table was holding when you deleted it. 

## Query Old Data

This is the most glorious part of the Time Travel story in Databend. When we say that you can get back a previous version of your data in a table, it does not mean that you roll back your table to an earlier point in time, it shows you the table's data at that point instead. 

Databend automatically takes and saves a snapshot of your tables after each transaction that updates your table data. A version of a table's data practically refers to a snapshot that saves the data of the table when the snapshot was taken.

Databend provides a system function named [FUSE_SNAPSHOT](https://databend.rs/doc/reference/functions/system-functions/fuse_snapshot) that enables you to find the saved snapshots. Each snapshot comes with a snapshot ID and a timestamp. 

The saved snapshots are the behind-the-scenes heroes that make the time travel become true. So when you try to get back your history data, you need to tell Databend which version you want by the snapshot ID or the timestamp with an [AT clause](https://databend.rs/doc/reference/sql/query-syntax/dml-at) in the SELECT statement.

## Create a New Table from Old Data

The Time Travel feature makes it possible to create an OLD table, which means you can create a table to hold and move on from a previous version of your data. 

The [CREATE TABLE](https://databend.rs/doc/reference/sql/ddl/table/ddl-create-table) statement can include a [SNAPSHOT_LOCATION](https://databend.rs/doc/reference/sql/ddl/table/ddl-create-table#create-table--snapshot_location) clause that allows you to specify a snapshot file that holds your old data. This command enables you to insert the data stored in the snapshot file when you create a table. Please note that the table you create must have same column definations as the data from the snapshot.

## Go without Time Travel

Tables in Databend support Time Travel out-of-the-box. However, you might not need it for some cases, for example, when you're running low of your storage space or the data is big but unimportant. Databend currently does not provide a setting to switch it off, but you can [CREATE TRANSIENT TABLE](https://databend.rs/doc/reference/sql/ddl/table/ddl-create-table#create-transient-table-).

Transient tables are used to hold transitory data that does not require a data protection or recovery mechanism. Dataebend does not hold historical data for a transient table so you will not be able to query from a previous version of the transient table with the Time Travel feature, for example, the AT clause in the SELECT statement will not work for transient tables. Please note that you can still drop and undrop a transient table.