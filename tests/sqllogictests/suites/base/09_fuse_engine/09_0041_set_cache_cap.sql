statement ok
create or replace database db_09_0041;

statement ok
use db_09_0041;

statement ok
call system$set_cache_capacity('memory_cache_block_meta', 1000);

query II
select count()>=1 from system.caches where name = 'memory_cache_block_meta';
----
1
