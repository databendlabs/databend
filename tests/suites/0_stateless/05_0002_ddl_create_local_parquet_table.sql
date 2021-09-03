DROP TABLE IF EXISTS default.test_parquet;

create table test_parquet (
    id int,
    bool_col boolean,
    tinyint_col int,
    smallint_col int,
    int_col int,
    bigint_col bigint,
    float_col float,
    double_col double,
    date_string_col varchar(255),
    string_col varchar(255),
    timestamp_col Timestamp
) Engine = Parquet location = 'tests/data/alltypes_plain.parquet';

select date_string_col, avg(id), max(bigint_col) from default.test_parquet group by date_string_col order by date_string_col desc;

DROP TABLE IF EXISTS default.test_parquet;
