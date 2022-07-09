set max_threads = 16;
SELECT number, number + 3 FROM numbers_mt (1000) where number > 5 order by number desc limit 3;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1 desc, c2 asc;
EXPLAIN SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1, number desc;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1, number desc;

create table t1(id int);
insert into t1 select number as id from numbers(10);
select * from t1 order by id asc limit 3,3;
select * from t1 order by id desc limit 3,3;
drop table t1;

-- sort with null
SELECT number, null from numbers(3) order by number desc;

SELECT '==Variant==';
CREATE TABLE IF NOT EXISTS t2(id Int null, var Variant null) Engine = Fuse;

INSERT INTO t2 VALUES(1, parse_json('{"k":"v"}')),
                     (2, parse_json('"abcd"')),
                     (3, parse_json('[1,2,3]')),
                     (4, parse_json('10')),
                     (5, parse_json('null')),
                     (6, parse_json('true'));

SELECT id, var FROM t2 ORDER BY var ASC;
SELECT id, var FROM t2 ORDER BY var DESC;

DROP TABLE t2;

SELECT '==Array(Int32)==';

CREATE TABLE IF NOT EXISTS t3(id Int null, arr Array(Int32) null) Engine = Fuse;

INSERT INTO t3 VALUES(1, [1,2,3]),
                     (2, [1,2,4]),
                     (3, []),
                     (4, [3,4,5]),
                     (5, [4]),
                     (6, [4,5]);

SELECT id, arr FROM t3 ORDER BY arr ASC;
SELECT id, arr FROM t3 ORDER BY arr DESC;

DROP TABLE t3;
