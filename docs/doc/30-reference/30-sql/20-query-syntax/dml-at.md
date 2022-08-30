---
title: AT
---

The SELECT statement can include an AT clause that allows you to  query previous versions of your data by a specific snapshot ID or timestamp.

Databend automatically creates snapshots when data updates occur, so a snapshot can be considered as a view of your data at a time point in the past. You can access a snapshot by the snapshot ID or the timestamp at which the snapshot was created. For how to obtain the snapshot ID and timestamp, see [Obtaining Snapshot ID and Timestamp](#obtaining-snapshot-id-and-timestamp).

This is part of the Databend's Time Travel feature that allows you to query, back up, and restore from a previous version of your data within the retention period (24 hours by default).

## Syntax

```sql    
SELECT ...
FROM ...
AT ( { SNAPSHOT => <snapshot_id> | TIMESTAMP => <timestamp> } );
```

## Obtaining Snapshot ID and Timestamp

To return the snapshot IDs and timestamps of all the snapshots of a table, execute the following statement:

```sql
SELECT snapshot_id, 
       timestamp 
FROM   fuse_snapshot('<database_name>', '<table_name>'); 
```

You can find more information related to the snapshots by executing the following statement:

```sql
SELECT * 
FROM   Fuse_snapshot('<database_name>', '<table_name>'); 
```

## Examples

### Query with a snapshot ID

```sql
-- Return snapshot IDs
select snapshot_id,timestamp from fuse_snapshot('default', 'ontime2');
+----------------------------------+----------------------------+
| snapshot_id                      | timestamp                  |
+----------------------------------+----------------------------+
| 16729481923640f9864c1c8ddd0861e3 | 2022-06-28 09:09:40.190662 |
+----------------------------------+----------------------------+

-- Query with the snapshot ID
select * from ontime2 at (snapshot=>'16729481923640f9864c1c8ddd0861e3');
```

### Query with a timestamp

```sql
-- Create a table
create table demo(c varchar);

-- Insert two rows
insert into demo values('batch1.1'),('batch1.2');

-- Insert another row
insert into demo values('batch2.1');

-- Return timestamps
select timestamp from fuse_snapshot('default', 'demo'); 
+----------------------------+
| timestamp                  |
+----------------------------+
| 2022-06-22 08:58:54.509008 |
| 2022-06-22 08:58:36.254458 |
+----------------------------+

-- Travel to the time when the last row was inserted
select * from demo at (TIMESTAMP => '2022-06-22 08:58:54.509008'::TIMESTAMP); 
+----------+
| c        |
+----------+
| batch1.1 |
| batch1.2 |
| batch2.1 |
+----------+

-- Travel to the time when the first two rows were inserted
select * from demo at (TIMESTAMP => '2022-06-22 08:58:36.254458'::TIMESTAMP); 
+----------+
| c        |
+----------+
| batch1.1 |
| batch1.2 |
+----------+
```