drop table if exists default.test_csv;

create table default.test_csv (id int,name varchar(255),rank int);
copy into default.test_csv from '@s3_stage/tests/data/sample.csv' format CSV field_delimitor = ',';

select max(id), min(name), avg(rank)  from default.test_csv;

drop table default.test_csv;
