CREATE USER 'test-a'@'localhost' IDENTIFIED BY 'password';
CREATE USER 'test-a'@'localhost' IDENTIFIED BY 'password'; -- {ErrorCode 2202}
CREATE USER 'test-b'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
CREATE USER 'test-c'@'localhost' IDENTIFIED WITH double_sha1_password BY 'password';
CREATE USER 'test-d@localhost' IDENTIFIED WITH sha256_password BY 'password';
CREATE USER IF NOT EXISTS 'test-d@localhost' IDENTIFIED WITH sha256_password BY 'password';
