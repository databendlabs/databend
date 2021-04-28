create table default.test_csv (id int,name varchar(255),rank int) Engine = CSV location = 'tests/data/sample.csv';
select avg(rank), max(id), min(name) as c1 from default.test_csv group by c1 order by c1 desc;
