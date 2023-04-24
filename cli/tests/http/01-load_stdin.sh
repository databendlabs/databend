#!/bin/bash

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS test_books;
SQL

cat <<SQL | ${BENDSQL}
CREATE TABLE test_books (title VARCHAR NULL, author VARCHAR NULL, date VARCHAR NULL, publish_time TIMESTAMP NULL);
SQL

${BENDSQL} --query='INSERT INTO test_books VALUES;' --format=csv --data=@- <cli/tests/data/books.csv

${BENDSQL} --query='SELECT * FROM test_books LIMIT 10;' --output=tsv

cat <<SQL | ${BENDSQL}
DROP TABLE test_books;
SQL
