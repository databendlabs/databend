statement ok
USE default

statement ok
set sql_dialect = 'PostgreSQL'

statement ok
CREATE TEMP TABLE `05_0003_at_t0`(a int not null)

statement ok
INSERT INTO TABLE `05_0003_at_t0` values(1)

query I
SELECT * FROM `05_0003_at_t0`
----
1

statement ok
ALTER TABLE `05_0003_at_t0` RENAME TO `05_0003_at_t1`

statement error 1025
ALTER TABLE `05_0003_at_t0` RENAME TO `05_0003_at_t1`

statement ok
ALTER TABLE IF EXISTS `05_0003_at_t0` RENAME TO `05_0003_at_t1`

statement error 1025
DROP TABLE `05_0003_at_t0`

query I
SELECT * FROM `05_0003_at_t1`
----
1

statement error 1005
ALTER TABLE `05_0003_at_t1` RENAME TO system.`05_0003_at_t1`

statement error 1025
ALTER TABLE system.abc drop column c1

statement ok
DROP TABLE IF EXISTS `05_0003_at_t1`

statement ok
CREATE TEMP TABLE `05_0003_at_t2`(a int not null, c int not null)

statement ok
INSERT INTO TABLE `05_0003_at_t2` values(1,2)

statement error 1065
ALTER TABLE `05_0003_at_t2` rename column a to a

statement error 1110
ALTER TABLE `05_0003_at_t2` rename column a to _row_id

statement error 1065
ALTER TABLE `05_0003_at_t2` rename column a to c

statement error 1065
ALTER TABLE `05_0003_at_t2` rename column d to e

statement ok
ALTER TABLE `05_0003_at_t2` rename column a to b

query I
SELECT b FROM `05_0003_at_t2`
----
1

statement ok
DROP TABLE IF EXISTS `05_0003_at_t2`

statement ok
set hide_options_in_show_create_table=0

statement ok
CREATE TEMP TABLE `05_0003_at_t3`(a int not null, b int not null, c int not null) bloom_index_columns='a,b,c' COMPRESSION='zstd' STORAGE_FORMAT='native'

query TT
SHOW CREATE TABLE `05_0003_at_t3`
----
05_0003_at_t3 CREATE TEMP TABLE "05_0003_at_t3" ( a INT NOT NULL, b INT NOT NULL, c INT NOT NULL ) ENGINE=FUSE BLOOM_INDEX_COLUMNS='a,b,c' COMPRESSION='zstd' STORAGE_FORMAT='native'

statement ok
ALTER TABLE `05_0003_at_t3` drop column c

query TT
SHOW CREATE TABLE `05_0003_at_t3`
----
05_0003_at_t3 CREATE TEMP TABLE "05_0003_at_t3" ( a INT NOT NULL, b INT NOT NULL ) ENGINE=FUSE BLOOM_INDEX_COLUMNS='a,b' COMPRESSION='zstd' STORAGE_FORMAT='native'

statement ok
ALTER TABLE `05_0003_at_t3` rename column b to c

query TT
SHOW CREATE TABLE `05_0003_at_t3`
----
05_0003_at_t3 CREATE TEMP TABLE "05_0003_at_t3" ( a INT NOT NULL, c INT NOT NULL ) ENGINE=FUSE BLOOM_INDEX_COLUMNS='a,c' COMPRESSION='zstd' STORAGE_FORMAT='native'

statement error 1301
ALTER TABLE `05_0003_at_t3` MODIFY COLUMN c decimal(5,2) not null

statement ok
ALTER TABLE `05_0003_at_t3` MODIFY COLUMN c float not null

statement ok
DROP TABLE IF EXISTS `05_0003_at_t3`

statement ok
set hide_options_in_show_create_table=1

statement ok
CREATE TEMP TABLE "05_0003_at_t4" ( a string not null, b string null, c array(string) null, d tuple(string, string) null ) ENGINE=FUSE COMPRESSION='zstd' STORAGE_FORMAT='native'

statement ok
INSERT INTO TABLE `05_0003_at_t4` values('a', 'b', ['c1', 'c2'], ('d1', 'd2'))

query TT
SHOW CREATE TABLE `05_0003_at_t4`
----
05_0003_at_t4 CREATE TEMP TABLE "05_0003_at_t4" ( a VARCHAR NOT NULL, b VARCHAR NULL, c ARRAY(STRING) NULL, d TUPLE(1 STRING, 2 STRING) NULL ) ENGINE=FUSE

query TTTT
SELECT * FROM `05_0003_at_t4`
----
a b ['c1','c2'] ('d1','d2')

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN a binary not null

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN b binary null

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN c array(binary) null

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN d tuple(binary, binary) null

query TT
SHOW CREATE TABLE `05_0003_at_t4`
----
05_0003_at_t4 CREATE TEMP TABLE "05_0003_at_t4" ( a BINARY NOT NULL, b BINARY NULL, c ARRAY(BINARY) NULL, d TUPLE(1 BINARY, 2 BINARY) NULL ) ENGINE=FUSE

query 
SELECT * FROM `05_0003_at_t4`
----
61 62 [6331,6332] (6431,6432)

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN a string not null

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN b string null

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN c array(string) null

statement ok
ALTER TABLE `05_0003_at_t4` MODIFY COLUMN d tuple(string, string) null

query TT
SHOW CREATE TABLE `05_0003_at_t4`
----
05_0003_at_t4 CREATE TEMP TABLE "05_0003_at_t4" ( a VARCHAR NOT NULL, b VARCHAR NULL, c ARRAY(STRING) NULL, d TUPLE(1 STRING, 2 STRING) NULL ) ENGINE=FUSE

query TTTT
SELECT * FROM `05_0003_at_t4`
----
a b ['c1','c2'] ('d1','d2')

statement ok
DROP TABLE IF EXISTS `05_0003_at_t4`

statement ok
drop table if exists t;

statement ok
create temp table t(c1 int, c2 int);

statement ok
alter table t modify c1 varchar, c2 varchar;

query TT
DESC t;
----
c1 VARCHAR YES NULL (empty)
c2 VARCHAR YES NULL (empty)

statement ok
alter table t modify c1 varchar comment 'c1-column', c2 int comment 'test';

query TTTTT
select database,table,name,data_type,comment from system.columns where table='t' and database='default';
----
default t c1 VARCHAR c1-column
default t c2 INT test

statement ok
alter table t comment='s1';

query TT
select name, comment from system.tables where name='t' and database='default';
----
t s1

query TT
show create table t;
----
t CREATE TEMP TABLE t ( c1 VARCHAR NULL COMMENT 'c1-column', c2 INT NULL COMMENT 'test' ) ENGINE=FUSE COMMENT = 's1'

statement ok
drop table if exists t1;

statement ok
create temp table t1(id int) comment ='t1-comment';

query TT
show create table t1;
----
t1 CREATE TEMP TABLE t1 ( id INT NULL ) ENGINE=FUSE COMMENT = 't1-comment'

query TT
select name, comment from system.tables where name='t1' and database='default';
----
t1 t1-comment

statement ok
alter table t1 comment='t1-new-comment';

query TT
show create table t1;
----
t1 CREATE TEMP TABLE t1 ( id INT NULL ) ENGINE=FUSE COMMENT = 't1-new-comment'

query TT
select name, comment from system.tables where name='t1' and database='default';
----
t1 t1-new-comment