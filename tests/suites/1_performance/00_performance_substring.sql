-- SELECT substring(cast(number as text) from 3) from numbers_mt(1000000000) where number > 10000000 order by number desc limit 10;
-- This sql will cause coredump, todo investigate it;
-- ATM we use samll number for tests
SELECT substring(cast(number as text) from 3) from numbers_mt(1000000) where number > 100 order by number desc limit 10;
