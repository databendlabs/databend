---
title: FLASHBACK TABLE
---

Flashback a table to an earlier version with a snapshot ID or timestamp, only involving metadata operations, making it a fast process.

By the snapshot ID or timestamp you specify in the command, Databend flashback the table to a prior state where the snapshot was created. To retrieve snapshot IDs and timestamps of a table, use [FUSE_SNAPSHOT](../../../15-sql-functions/111-system-functions/fuse_snapshot.md).

The capability to flash back a table is subject to these conditions:

- The command only existing tables to their prior states. To recover a dropped table, use [UNDROP TABLE](21-ddl-undrop-table.md).

- Flashback a table is part of Databend's time travel feature. Before using the command, make sure the table you want to flashback is eligible for time travel. For example, the command doesn't work for transient tables because Databend does not create or store snapshots for such tables.

- You cannot roll back after flashback a table to a prior state, but you can flash back the table again to an earlier state.

- Databend recommends this command for emergency recovery only. To query the history data of a table, use the [AT](../../20-query-syntax/03-query-at.md) clause.

## Syntax

```sql
-- Restore with a snapshot ID
ALTER TABLE <table> FLASHBACK TO (SNAPSHOT => '<snapshot-id>');

-- Restore with a snapshot timestamp
ALTER TABLE <table> FLASHBACK TO (TIMESTAMP => '<timestamp>'::TIMESTAMP);
```

## Example

### Step 1: Create a sample users table and insert data
```sql
-- Create a sample users table
CREATE TABLE users (
    id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    registration_date TIMESTAMP
);

-- Insert sample data
INSERT INTO users (id, first_name, last_name, email, registration_date)
VALUES (1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01 00:00:00'),
       (2, 'Jane', 'Doe', 'jane.doe@example.com', '2023-01-02 00:00:00');
```

Data:
```sql
SELECT * FROM users;
+------+------------+-----------+----------------------+----------------------------+
| id   | first_name | last_name | email                | registration_date          |
+------+------------+-----------+----------------------+----------------------------+
|    1 | John       | Doe       | john.doe@example.com | 2023-01-01 00:00:00.000000 |
|    2 | Jane       | Doe       | jane.doe@example.com | 2023-01-02 00:00:00.000000 |
+------+------------+-----------+----------------------+----------------------------+
```

Snapshots:
```sql
SELECT * FROM Fuse_snapshot('default', 'users')\G;
*************************** 1. row ***************************
         snapshot_id: c5c538d6b8bc42f483eefbddd000af7d
   snapshot_location: 29356/44446/_ss/c5c538d6b8bc42f483eefbddd000af7d_v2.json
      format_version: 2
previous_snapshot_id: NULL
       segment_count: 1
         block_count: 1
           row_count: 2
  bytes_uncompressed: 150
    bytes_compressed: 829
          index_size: 1028
           timestamp: 2023-04-19 04:20:25.062854
```

### Step 2: Simulate an accidental delete operation

```sql
-- Simulate an accidental delete operation
DELETE FROM users WHERE id = 1;
```

Data:
```sql
+------+------------+-----------+----------------------+----------------------------+
| id   | first_name | last_name | email                | registration_date          |
+------+------------+-----------+----------------------+----------------------------+
|    2 | Jane       | Doe       | jane.doe@example.com | 2023-01-02 00:00:00.000000 |
+------+------------+-----------+----------------------+----------------------------+
```

Snapshots:
```sql
SELECT * FROM Fuse_snapshot('default', 'users')\G;
*************************** 1. row ***************************
         snapshot_id: 7193af51a4c9423ebd6ddbb04327b280
   snapshot_location: 29356/44446/_ss/7193af51a4c9423ebd6ddbb04327b280_v2.json
      format_version: 2
previous_snapshot_id: c5c538d6b8bc42f483eefbddd000af7d
       segment_count: 1
         block_count: 1
           row_count: 1
  bytes_uncompressed: 87
    bytes_compressed: 778
          index_size: 1028
           timestamp: 2023-04-19 04:22:20.390430
*************************** 2. row ***************************
         snapshot_id: c5c538d6b8bc42f483eefbddd000af7d
   snapshot_location: 29356/44446/_ss/c5c538d6b8bc42f483eefbddd000af7d_v2.json
      format_version: 2
previous_snapshot_id: NULL
       segment_count: 1
         block_count: 1
           row_count: 2
  bytes_uncompressed: 150
    bytes_compressed: 829
          index_size: 1028
           timestamp: 2023-04-19 04:20:25.062854
```

### Step 3: Find the snapshot ID before the delete operation
```sql
-- Assume the snapshot_id from the previous query is 'xxxxxx'
-- Restore the table to the snapshot before the delete operation
ALTER TABLE users FLASHBACK TO (SNAPSHOT => 'c5c538d6b8bc42f483eefbddd000af7d');
```

Data:
```sql
SELECT * FROM users;
+------+------------+-----------+----------------------+----------------------------+
| id   | first_name | last_name | email                | registration_date          |
+------+------------+-----------+----------------------+----------------------------+
|    1 | John       | Doe       | john.doe@example.com | 2023-01-01 00:00:00.000000 |
|    2 | Jane       | Doe       | jane.doe@example.com | 2023-01-02 00:00:00.000000 |
+------+------------+-----------+----------------------+----------------------------+
```

Snapshot:
```sql
SELECT * FROM Fuse_snapshot('default', 'users')\G;
*************************** 1. row ***************************
         snapshot_id: c5c538d6b8bc42f483eefbddd000af7d
   snapshot_location: 29356/44446/_ss/c5c538d6b8bc42f483eefbddd000af7d_v2.json
      format_version: 2
previous_snapshot_id: NULL
       segment_count: 1
         block_count: 1
           row_count: 2
  bytes_uncompressed: 150
    bytes_compressed: 829
          index_size: 1028
           timestamp: 2023-04-19 04:20:25.062854
```