DROP TABLE IF EXISTS default.test_csv;

create table default.test_csv (id int,name varchar(255),rank int) Engine = CSV location = 'tests/data/sample.csv';
select avg(rank), max(id), name from default.test_csv group by name order by name desc;

DROP TABLE IF EXISTS default.test_csv;
