#!/bin/bash

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS http_books_01;
SQL

cat <<SQL | ${BENDSQL}
CREATE TABLE http_books_01 (title VARCHAR NULL, author VARCHAR NULL, date VARCHAR NULL, publish_time TIMESTAMP NULL);
SQL

${BENDSQL} --query='INSERT INTO http_books_01 VALUES;' --format=csv --data=@- <cli/tests/data/books.csv

${BENDSQL} --query='SELECT * FROM http_books_01 LIMIT 10;' --output=tsv

cat <<SQL | ${BENDSQL}
DROP TABLE http_books_01;
SQL
