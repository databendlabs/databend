create table test_csv (
    id int,
    name varchar(255),
    rank int
) Engine = CSV location = 'tests/data/sample.csv';


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

select * from system.tables;
