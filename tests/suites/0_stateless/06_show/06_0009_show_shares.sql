DROP DATABASE IF EXISTS show_shares;
DROP SHARE IF EXISTS test_share;
CREATE DATABASE show_shares;
USE show_shares;
DROP TABLE IF EXISTS test_tb;
create table test_tb (a int);

create share test_share comment = 'comment';

alter share test_share add tenants = x;
grant USAGE on DATABASE show_shares TO SHARE test_share; 
grant SELECT on TABLE show_shares.test_tb TO SHARE test_share; 
show shares;
desc share test_share;
show grants on DATABASE show_shares;
show grants on TABLE show_shares.test_tb;

DROP TABLE IF EXISTS test_tb;
DROP DATABASE IF EXISTS show_shares;
DROP SHARE IF EXISTS test_share;
