#!/bin/bash

echo "DROP STAGE IF EXISTS ss_01" | ${BENDSQL}
echo "CREATE STAGE ss_01" | ${BENDSQL}

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS books_01;
CREATE TABLE books_01
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
SQL

cat <<SQL | ${BENDSQL}
SELECT * FROM books_01;
SQL

mkdir -p /tmp/abc
cp "${PWD}/cli/tests/data/books.parquet" /tmp/abc/books.parquet

echo "put fs:///tmp/abc/b*.parquet @ss_01/abc/" | ${BENDSQL}
echo 'get @ss_01/abc fs:///tmp/edf' | ${BENDSQL}

cat <<SQL | ${BENDSQL}
COPY INTO books_01 FROM @ss_01/abc/ files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
SQL

cat <<SQL | ${BENDSQL}
SELECT * FROM books_01 LIMIT 2;
SQL

echo "DROP STAGE IF EXISTS ss_01" | ${BENDSQL}
echo "DROP TABLE IF EXISTS books_01" | ${BENDSQL}
