SET enable_planner_v2 = 1;
DROP USER IF EXISTS 'test-grant'@'localhost';
DROP DATABASE IF EXISTS `db01`;
DROP ROLE IF EXISTS 'test-grant-role';
CREATE DATABASE IF NOT EXISTS `db01`;
CREATE TABLE IF NOT EXISTS `db01`.`tb1` (id int);

CREATE USER 'test-grant'@'localhost' IDENTIFIED BY 'password';

GRANT SELECT ON * TO 'test-grant'@'localhost';
GRANT SELECT ON * TO 'test-grant1'@'localhost'; -- {ErrorCode 2201}

GRANT SELECT, CREATE ON * TO 'test-grant'@'localhost';
GRANT ALL ON * TO 'test-grant'@'localhost';
GRANT ALL PRIVILEGES ON * TO 'test-grant'@'localhost';

GRANT SELECT ON db01.* TO 'test-grant'@'localhost';
GRANT SELECT ON db01.tb1 TO 'test-grant'@'localhost';
GRANT SELECT ON `db01`.`tb1` TO 'test-grant'@'localhost';
GRANT SELECT ON db01.tbnotexists TO 'test-grant'@'localhost'; -- {ErrorCode 1025}
GRANT SELECT ON dbnotexists.* TO 'test-grant'@'localhost'; -- {ErrorCode 1003}
SHOW GRANTS FOR 'test-grant'@'localhost';

REVOKE SELECT ON db01.* FROM 'test-grant'@'localhost';
SHOW GRANTS FOR 'test-grant'@'localhost';
REVOKE ALL PRIVILEGES ON * FROM 'test-grant'@'localhost';
SHOW GRANTS FOR 'test-grant'@'localhost';

CREATE ROLE 'test-grant-role';
GRANT SELECT ON * TO ROLE 'test-grant-role';
GRANT SELECT ON * TO ROLE 'test-grant-role1'; -- {ErrorCode 2204}
SHOW GRANTS FOR ROLE 'test-grant-role';
DROP ROLE 'test-grant-role';

DROP DATABASE `db01`;
SET enable_planner_v2 = 0;
