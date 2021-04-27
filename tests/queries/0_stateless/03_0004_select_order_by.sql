SELECT number, number + 3 FROM numbers_mt (1000) where number > 5 order by number desc limit 3;
