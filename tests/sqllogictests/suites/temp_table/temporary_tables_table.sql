statement ok
CREATE TEMP TABLE t0(a int not null, b int not null);

statement ok
CREATE OR REPLACE DATABASE d1;

statement ok
CREATE TEMP TABLE d1.t1(a int not null, b int not null);

statement ok
CREATE OR REPLACE DATABASE d2;

statement ok
CREATE TEMP TABLE d2.t2(a int not null, b int not null) ENGINE = Memory;

query T
select * from system.temporary_tables order by table_id;
----
default t0 4611686018427407904 FUSE
d1 t1 4611686018427407905 FUSE
d2 t2 4611686018427407906 MEMORY

