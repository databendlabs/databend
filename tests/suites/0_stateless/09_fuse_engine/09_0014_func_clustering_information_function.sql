-- Create table t09_0014
create table t09_0014(a int, b int) cluster by(b,a);

insert into t09_0014 values(0,3),(1,1);
insert into t09_0014 values(1,3),(2,1);
insert into t09_0014 values(4,4);

select *  from t09_0014 order by b, a;

select * from clustering_information('default','t09_0014');

-- Drop table.
drop table t09_0014;
