-- This will be enabled after the new processor framework supports the broadcast plan (same as the window function)
SET enable_new_processor_framework = 0;

select number from numbers_mt(10) where number > 5  and exists (select name from system.settings) order by number asc;
select number from numbers_mt(10) where number > 5  and exists (select name from system.settings) and exists (select number from numbers_mt(10)) order by number asc;
select number from numbers_mt(10) where number > 5  and exists (select name from system.settings where exists (select number from numbers_mt(10))) order by number asc;
select number from numbers_mt(20) where number > 15  and not exists (select number from numbers_mt(5) where number > 10) order by number asc;
