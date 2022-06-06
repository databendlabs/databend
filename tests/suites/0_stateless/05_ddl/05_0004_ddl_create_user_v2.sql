set enable_planner_v2 = 1;
DROP USER IF EXISTS 'test-a'@'localhost';
DROP USER IF EXISTS 'test-b'@'localhost';
DROP USER IF EXISTS 'test-c'@'localhost';
DROP USER IF EXISTS 'test-d@localhost';

CREATE USER 'test-a'@'localhost' IDENTIFIED BY 'password';
CREATE USER 'test-a'@'localhost' IDENTIFIED BY 'password'; -- {ErrorCode 2202}
CREATE USER 'test-b'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
CREATE USER 'test-c'@'localhost' IDENTIFIED WITH double_sha1_password BY 'password';
CREATE USER 'test-d@localhost' IDENTIFIED WITH sha256_password BY 'password';
CREATE USER IF NOT EXISTS 'test-d@localhost' IDENTIFIED WITH sha256_password BY 'password';

DROP USER 'test-a'@'localhost';
DROP USER 'test-b'@'localhost';
DROP USER 'test-c'@'localhost';
DROP USER 'test-d@localhost';
