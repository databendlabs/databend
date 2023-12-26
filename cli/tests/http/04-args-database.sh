#!/bin/bash

cat <<SQL | ${BENDSQL}
DROP DATABASE IF EXISTS books_04;
CREATE DATABASE IF NOT EXISTS books_04;
SQL

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS books_04_d;
CREATE TABLE books_04_d
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
SQL

cat <<SQL | ${BENDSQL} -D books_04
DROP TABLE IF EXISTS books_04_t;
CREATE TABLE books_04_t
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
SQL

echo "---- tables ----"
cat <<SQL | ${BENDSQL}
SHOW TABLES;
SQL

echo "---- databases ----"
cat <<SQL | ${BENDSQL}
SHOW DATABASES;
SQL

echo "---- tables in books_04 ----"
cat <<SQL | ${BENDSQL} -D books_04
SHOW TABLES;
SQL

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS books_04_d;
DROP DATABASE IF EXISTS books_04;
SQL
