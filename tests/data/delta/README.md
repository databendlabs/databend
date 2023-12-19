
To generate table for tests, create tables use the following SQLs in the spark-sql and copy the data directory here.

```SQL:

```SQL
CREATE TABLE default.simple USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4;
```

```SQL
CREATE TABLE default.partitioned (p0 Int, c1 Int, p2 Int, c3 INT, p4 INT, c5 Int ) USING DELTA
PARTITIONED BY (p0, p2, p4);
---- so we can test tables with checkpoint files.
alter table partitioned SET TBLPROPERTIES ("delta.checkpointInterval" = "2");
insert into default.partitioned VALUES (10, 11, 12, 13, 14, 15 );
insert into default.partitioned VALUES (10, 21, 12, 23, 24, 25 );
insert into default.partitioned VALUES (10, 31, 32, 33, 34, 35 );
insert into default.partitioned VALUES (20, 41, 42, 43, 44, 45 );
```