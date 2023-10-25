#!/bin/bash

echo "DROP STAGE IF EXISTS ss_temp" | ${BENDSQL}
echo "CREATE STAGE ss_temp" | ${BENDSQL}

cat <<SQL | ${BENDSQL}
DROP TABLE IF EXISTS books;
CREATE TABLE books
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
SQL

cat <<SQL | ${BENDSQL}
SELECT * FROM books;
SQL

mkdir -p /tmp/abc
cp "${PWD}/cli/tests/data/books.parquet" /tmp/abc/books.parquet

echo "put fs:///tmp/abc/b*.parquet @ss_temp/abc/" | ${BENDSQL}
echo 'get @ss_temp/abc fs:///tmp/edf' | ${BENDSQL}

cat <<SQL | ${BENDSQL}
COPY INTO books FROM @ss_temp/abc/ files=('books.parquet') FILE_FORMAT = (TYPE = PARQUET);
SQL

cat <<SQL | ${BENDSQL}
SELECT * FROM books LIMIT 2;
SQL
