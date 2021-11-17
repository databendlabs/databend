CREATE USER 'test-grant'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT ON * TO 'test-grant'@'localhost';
GRANT SELECT ON * TO 'test-grant1'@'localhost'; -- {ErrorCode 3000}

GRANT SELECT, CREATE ON * TO 'test-grant'@'localhost';
GRANT ALL ON * TO 'test-grant'@'localhost';
GRANT ALL PRIVILEGES ON * TO 'test-grant'@'localhost';
