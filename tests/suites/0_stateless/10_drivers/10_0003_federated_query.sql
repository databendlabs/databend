/*!40101*/select number from numbers_mt(2) ORDER BY number;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101*/select number from numbers_mt(1) ORDER BY number;
COMMIT;
ROLLBACK;
-- mydumper
START;
SET SQL_LOG_BIN=0;
SHOW MASTER STATUS;
SHOW ALL SLAVES STATUS;
--charset and collation
SHOW CHARSET;
SHOW COLLATION;
--function
SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP())
