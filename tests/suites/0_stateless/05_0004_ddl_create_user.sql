CREATE USER 'test'@'localhost' IDENTIFIED BY 'password';
CREATE USER 'test'@'localhost' IDENTIFIED BY 'password'; -- {ErrorCode 2202}
CREATE USER 'test-a'@'localhost' IDENTIFIED WITH plaintext_password BY 'password';
CREATE USER 'test-b'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
CREATE USER 'test-c'@'localhost' IDENTIFIED WITH double_sha1_password BY 'password';
CREATE USER 'test-d@localhost' IDENTIFIED WITH sha256_password BY 'password';