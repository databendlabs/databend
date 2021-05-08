SELECT substring(cast(number as text) from 3) from numbers_mt(100000000) where number > 10000000 order by number desc limit 10;
