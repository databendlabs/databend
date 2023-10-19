// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use common_ast::display_parser_error;
use common_ast::parser::expr::*;
use common_ast::parser::parse_sql;
use common_ast::parser::query::*;
use common_ast::parser::quote::quote_ident;
use common_ast::parser::quote::unquote_ident;
use common_ast::parser::token::*;
use common_ast::parser::tokenize_sql;
use common_ast::rule;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_ast::Input;
use common_exception::Result;
use goldenfile::Mint;
use nom::Parser;

macro_rules! run_parser {
    ($file:expr, $parser:expr, $source:expr $(,)*) => {
        let tokens = Tokenizer::new($source).collect::<Result<Vec<_>>>().unwrap();
        let backtrace = Backtrace::new();
        let parser = $parser;
        let mut parser = rule! { #parser ~ &EOI };
        match parser.parse(Input(&tokens, Dialect::PostgreSQL, &backtrace)) {
            Ok((i, (output, _))) => {
                assert_eq!(i[0].kind, TokenKind::EOI);
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", output).unwrap();
                writeln!($file, "---------- AST ------------").unwrap();
                writeln!($file, "{:#?}", output).unwrap();
                writeln!($file, "\n").unwrap();
            }
            Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
                let report = display_parser_error(err, $source).trim_end().to_string();
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", report).unwrap();
                writeln!($file, "\n").unwrap();
            }
            Err(nom::Err::Incomplete(_)) => unreachable!(),
        }
    };
}

#[test]
fn test_statement() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement.txt").unwrap();
    let cases = &[
        r#"show databases"#,
        r#"show databases format TabSeparatedWithNamesAndTypes;"#,
        r#"show tables"#,
        r#"show tables format TabSeparatedWithNamesAndTypes;"#,
        r#"describe "name""with""quote";"#,
        r#"describe "name""""with""""quote";"#,
        r#"show full tables"#,
        r#"show full tables from db"#,
        r#"show full tables from ctl.db"#,
        r#"show full columns in t in db"#,
        r#"show columns in t from ctl.db"#,
        r#"show full columns from t from db like 'id%'"#,
        r#"show processlist;"#,
        r#"show create table a.b;"#,
        r#"show create table a.b format TabSeparatedWithNamesAndTypes;"#,
        r#"explain pipeline select a from b;"#,
        r#"explain pipeline select a from t1 ignore_result;"#,
        r#"describe a;"#,
        r#"describe a format TabSeparatedWithNamesAndTypes;"#,
        r#"create table a (c decimal(38, 0))"#,
        r#"create table a (c decimal(38))"#,
        r#"create table if not exists a.b (c integer not null default 1, b varchar);"#,
        r#"create table if not exists a.b (c integer default 1 not null, b varchar) as select * from t;"#,
        r#"create table if not exists a.b (c tuple(m integer, n string), d tuple(integer, string));"#,
        r#"create table if not exists a.b (a string, b string, c string as (concat(a, ' ', b)) stored );"#,
        r#"create table if not exists a.b (a int, b int, c int generated always as (a + b) virtual );"#,
        r#"create table a.b like c.d;"#,
        r#"create table t like t2 engine = memory;"#,
        r#"create table if not exists a.b (a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900');"#,
        r#"create table if not exists a.b (a int) 's3://testbucket/admin/data/'
             connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900')
             location_prefix = 'db';"#,
        r#"truncate table a;"#,
        r#"truncate table "a".b;"#,
        r#"drop table a;"#,
        r#"drop table if exists a."b";"#,
        r#"use "a";"#,
        r#"create catalog ctl type=hive connection=(url='<hive-meta-store>' thrift_protocol='binary');"#,
        r#"create database if not exists a;"#,
        r#"create database ctl.t engine = Default;"#,
        r#"create database t engine = Default;"#,
        r#"create database t FROM SHARE a.s;"#,
        r#"drop database ctl.t;"#,
        r#"drop database if exists t;"#,
        r#"create table c(a DateTime null, b DateTime(3));"#,
        r#"create view v as select number % 3 as a from numbers(1000);"#,
        r#"alter view v as select number % 3 as a from numbers(1000);"#,
        r#"drop view v;"#,
        r#"create view v1(c1) as select number % 3 as a from numbers(1000);"#,
        r#"alter view v1(c2) as select number % 3 as a from numbers(1000);"#,
        r#"rename table d.t to e.s;"#,
        r#"truncate table test;"#,
        r#"truncate table test_db.test;"#,
        r#"DROP table table1;"#,
        r#"DROP table IF EXISTS table1;"#,
        r#"CREATE TABLE t(c1 int null, c2 bigint null, c3 varchar null);"#,
        r#"CREATE TABLE t(c1 int not null, c2 bigint not null, c3 varchar not null);"#,
        r#"CREATE TABLE t(c1 varbinary);"#,
        r#"CREATE TABLE t(c1 int default 1);"#,
        r#"create table abc as (select * from xyz limit 10)"#,
        r#"ALTER USER u1 IDENTIFIED BY '123456';"#,
        r#"ALTER USER u1 WITH DEFAULT_ROLE = role1;"#,
        r#"ALTER USER u1 WITH DEFAULT_ROLE = role1, TENANTSETTING;"#,
        r#"ALTER USER u1 WITH SET NETWORK POLICY = 'policy1';"#,
        r#"ALTER USER u1 WITH UNSET NETWORK POLICY;"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH DEFAULT_ROLE='role123', TENANTSETTING"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH SET NETWORK POLICY='policy1'"#,
        r#"DROP database if exists db1;"#,
        r#"select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;"#,
        r#"select * from t4;"#,
        r#"select * from aa.bb;"#,
        r#"select * from a, b, c;"#,
        r#"select * from a, b, c order by "db"."a"."c1";"#,
        r#"select * from a join b on a.a = b.a;"#,
        r#"select * from a left outer join b on a.a = b.a;"#,
        r#"select * from a right outer join b on a.a = b.a;"#,
        r#"select * from a left semi join b on a.a = b.a;"#,
        r#"select * from a semi join b on a.a = b.a;"#,
        r#"select * from a left anti join b on a.a = b.a;"#,
        r#"select * from a anti join b on a.a = b.a;"#,
        r#"select * from a right semi join b on a.a = b.a;"#,
        r#"select * from a right anti join b on a.a = b.a;"#,
        r#"select * from a full outer join b on a.a = b.a;"#,
        r#"select * from a inner join b on a.a = b.a;"#,
        r#"select * from a left outer join b using(a);"#,
        r#"select * from a right outer join b using(a);"#,
        r#"select * from a full outer join b using(a);"#,
        r#"select * from a inner join b using(a);"#,
        r#"select * from a where a.a = any (select b.a from b);"#,
        r#"select * from a where a.a = all (select b.a from b);"#,
        r#"select * from a where a.a = some (select b.a from b);"#,
        r#"select * from a where a.a > (select b.a from b);"#,
        r#"select 1 from numbers(1) where ((1 = 1) or 1)"#,
        r#"select * from read_parquet('p1', 'p2', 'p3', prune_page => true, refresh_meta_cache => true);"#,
        r#"select * from @foo (pattern=>'[.]*parquet' file_format=>'tsv');"#,
        r#"select 'stringwith''quote'''"#,
        r#"select 'stringwith"doublequote'"#,
        r#"select '🦈'"#,
        r#"insert into t (c1, c2) values (1, 2), (3, 4);"#,
        r#"insert into t (c1, c2) values (1, 2);   "#,
        r#"insert into table t format json;"#,
        r#"insert into table t select * from t2;"#,
        r#"select parse_json('{"k1": [0, 1, 2]}').k1[0];"#,
        r#"CREATE STAGE ~"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c', aws_secret_key='4x5y6z') file_format=(type = CSV, compression = GZIP record_delimiter=',')"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage url='azblob://load/files/' connection=(account_name='1a2b3c' account_key='4x5y6z') file_format=(type = CSV compression = GZIP record_delimiter=',')"#,
        r#"DROP STAGE abc"#,
        r#"DROP STAGE ~"#,
        r#"list @stage_a;"#,
        r#"list @~;"#,
        r#"create user 'test-e' identified by 'password';"#,
        r#"drop user if exists 'test-j';"#,
        r#"alter user 'test-e' identified by 'new-password';"#,
        r#"create role test"#,
        r#"create role 'test'"#,
        r#"drop role if exists test"#,
        r#"drop role if exists 'test'"#,
        r#"OPTIMIZE TABLE t COMPACT SEGMENT LIMIT 10;"#,
        r#"OPTIMIZE TABLE t COMPACT LIMIT 10;"#,
        r#"OPTIMIZE TABLE t PURGE BEFORE (SNAPSHOT => '9828b23f74664ff3806f44bbc1925ea5') LIMIT 10;"#,
        r#"OPTIMIZE TABLE t PURGE BEFORE (TIMESTAMP => '2023-06-26 09:49:02.038483'::TIMESTAMP) LIMIT 10;"#,
        r#"ALTER TABLE t CLUSTER BY(c1);"#,
        r#"ALTER TABLE t DROP CLUSTER KEY;"#,
        r#"ALTER TABLE t RECLUSTER FINAL WHERE c1 > 0 LIMIT 10;"#,
        r#"ALTER TABLE t ADD COLUMN c int null;"#,
        r#"ALTER TABLE t ADD COLUMN a float default 1.1 COMMENT 'hello' FIRST;"#,
        r#"ALTER TABLE t ADD COLUMN b string default 'b' AFTER a;"#,
        r#"ALTER TABLE t RENAME COLUMN a TO b;"#,
        r#"ALTER TABLE t DROP COLUMN b;"#,
        r#"ALTER TABLE t MODIFY COLUMN b SET MASKING POLICY mask;"#,
        r#"ALTER TABLE t MODIFY COLUMN b UNSET MASKING POLICY;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int DEFAULT 1, COLUMN b float;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int NULL DEFAULT 1, COLUMN b float NOT NULL COMMENT 'column b';"#,
        r#"ALTER TABLE t MODIFY COLUMN a int;"#,
        r#"ALTER TABLE t MODIFY COLUMN a DROP STORED;"#,
        r#"ALTER TABLE t SET OPTIONS(SNAPSHOT_LOCATION='1/7/_ss/101fd790dbbe4238a31a8f2e2f856179_v4.mpk',block_per_segment = 500);"#,
        r#"ALTER DATABASE IF EXISTS ctl.c RENAME TO a;"#,
        r#"ALTER DATABASE c RENAME TO a;"#,
        r#"ALTER DATABASE ctl.c RENAME TO a;"#,
        r#"VACUUM TABLE t;"#,
        r#"VACUUM TABLE t RETAIN 4 HOURS DRY RUN;"#,
        r#"VACUUM TABLE t RETAIN 40 HOURS;"#,
        r#"VACUUM DROP TABLE RETAIN 20 HOURS;"#,
        r#"VACUUM DROP TABLE RETAIN 30 HOURS DRY RUN;"#,
        r#"VACUUM DROP TABLE FROM db RETAIN 40 HOURS;"#,
        r#"CREATE TABLE t (a INT COMMENT 'col comment') COMMENT='table comment';"#,
        r#"GRANT CREATE, CREATE USER ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON *.* TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO USER 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO ROLE role1;"#,
        r#"GRANT ALL ON *.* TO 'test-grant';"#,
        r#"GRANT ALL ON *.* TO ROLE role2;"#,
        r#"GRANT ALL PRIVILEGES ON * TO 'test-grant';"#,
        r#"GRANT ALL PRIVILEGES ON * TO ROLE role3;"#,
        r#"GRANT ROLE test TO 'test-user';"#,
        r#"GRANT ROLE test TO USER 'test-user';"#,
        r#"GRANT ROLE test TO ROLE `test-user`;"#,
        r#"GRANT SELECT ON db01.* TO 'test-grant';"#,
        r#"GRANT SELECT ON db01.* TO USER 'test-grant';"#,
        r#"GRANT SELECT ON db01.* TO ROLE role1"#,
        r#"GRANT SELECT ON db01.tb1 TO 'test-grant';"#,
        r#"GRANT SELECT ON db01.tb1 TO USER 'test-grant';"#,
        r#"GRANT SELECT ON db01.tb1 TO ROLE role1;"#,
        r#"GRANT SELECT ON tb1 TO ROLE role1;"#,
        r#"GRANT ALL ON tb1 TO 'u1';"#,
        r#"SHOW GRANTS;"#,
        r#"SHOW GRANTS FOR 'test-grant';"#,
        r#"SHOW GRANTS FOR USER 'test-grant';"#,
        r#"SHOW GRANTS FOR ROLE role1;"#,
        r#"SHOW GRANTS FOR ROLE 'role1';"#,
        r#"REVOKE SELECT, CREATE ON * FROM 'test-grant';"#,
        r#"REVOKE SELECT ON tb1 FROM ROLE role1;"#,
        r#"REVOKE SELECT ON tb1 FROM ROLE 'role1';"#,
        r#"drop role 'role1';"#,
        r#"GRANT ROLE test TO ROLE 'test-user';"#,
        r#"GRANT ROLE test TO ROLE `test-user`;"#,
        r#"SET ROLE `test-user`;"#,
        r#"SET ROLE 'test-user';"#,
        r#"SET ROLE ROLE1;"#,
        r#"REVOKE ALL ON tb1 FROM 'u1';"#,
        r#"COPY INTO mytable
                FROM '@~/mybucket/my data.csv'
                size_limit=10;"#,
        r#"COPY INTO mytable
                FROM @~/mybucket/data.csv
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;"#,
        r#"COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10
                max_files=10;"#,
        r#"COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10
                max_files=3000;"#,
        r#"COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                CONNECTION = (
                    ENDPOINT_URL = 'http://127.0.0.1:9900'
                )
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;"#,
        r#"COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                CONNECTION = (
                    ENDPOINT_URL = 'http://127.0.0.1:9900'
                )
                size_limit=10
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                );"#,
        r#"COPY INTO mytable
                FROM 'https://127.0.0.1:9900';"#,
        r#"COPY INTO mytable
                FROM 'https://127.0.0.1:';"#,
        r#"COPY INTO mytable
                FROM @my_stage
                FILE_FORMAT = (
                    type = CSV,
                    field_delimiter = ',',
                    record_delimiter = '\n',
                    skip_header = 1,
                    error_on_column_count_mismatch = FALSE
                )
                size_limit=10;"#,
        r#"COPY INTO 's3://mybucket/data.csv'
                FROM mytable
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )"#,
        r#"COPY INTO '@my_stage/my data'
                FROM mytable;"#,
        r#"COPY INTO @my_stage
                FROM mytable
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                );"#,
        r#"COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                CREDENTIALS = (
                    AWS_KEY_ID = 'access_key'
                    AWS_SECRET_KEY = 'secret_key'
                )
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;"#,
        r#"COPY INTO mytable
                FROM @external_stage/path/to/file.csv
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;"#,
        r#"COPY INTO mytable
                FROM @external_stage/path/to/dir/
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;"#,
        r#"COPY INTO mytable
                FROM @external_stage/path/to/file.csv
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                force=true;"#,
        r#"COPY INTO mytable
                FROM 'fs:///path/to/data.csv'
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10
                disable_variant_check=true;"#,
        r#"COPY INTO books FROM 's3://databend/books.csv'
                CONNECTION = (
                    ENDPOINT_URL = 'http://localhost:9000/',
                    ACCESS_KEY_ID = 'ROOTUSER',
                    SECRET_ACCESS_KEY = 'CHANGEME123',
                    region = 'us-west-2'
                )
                FILE_FORMAT = (type = CSV);"#,
        // We used to support COPY FROM a quoted at string
        // r#"COPY INTO mytable
        //         FROM '@external_stage/path/to/file.csv'
        //         FILE_FORMAT = (
        //             type = 'CSV'
        //             field_delimiter = ','
        //             record_delimiter = '\n'
        //             skip_header = 1
        //         )
        //         size_limit=10;"#,
        r#"CALL system$test(a)"#,
        r#"CALL system$test('a')"#,
        r#"show settings like 'enable%'"#,
        r#"PRESIGN @my_stage"#,
        r#"PRESIGN @my_stage/path/to/dir/"#,
        r#"PRESIGN @my_stage/path/to/file"#,
        r#"PRESIGN @my_stage/my\ file.csv"#,
        r#"PRESIGN @my_stage/\"file\".csv"#,
        r#"PRESIGN @my_stage/\'file\'.csv"#,
        r#"PRESIGN @my_stage/\\file\\.csv"#,
        r#"PRESIGN DOWNLOAD @my_stage/path/to/file"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE=7200"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE=7200 CONTENT_TYPE='application/octet-stream'"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file CONTENT_TYPE='application/octet-stream' EXPIRE=7200"#,
        r#"CREATE SHARE ENDPOINT IF NOT EXISTS t URL='http://127.0.0.1' TENANT=x ARGS=(jwks_key_file="https://eks.public/keys" ssl_cert="cert.pem") COMMENT='share endpoint comment';"#,
        r#"CREATE SHARE t COMMENT='share comment';"#,
        r#"CREATE SHARE IF NOT EXISTS t;"#,
        r#"DROP SHARE a;"#,
        r#"DROP SHARE IF EXISTS a;"#,
        r#"GRANT USAGE ON DATABASE db1 TO SHARE a;"#,
        r#"GRANT SELECT ON TABLE db1.tb1 TO SHARE a;"#,
        r#"REVOKE USAGE ON DATABASE db1 FROM SHARE a;"#,
        r#"REVOKE SELECT ON TABLE db1.tb1 FROM SHARE a;"#,
        r#"ALTER SHARE a ADD TENANTS = b,c;"#,
        r#"ALTER SHARE IF EXISTS a ADD TENANTS = b,c;"#,
        r#"ALTER SHARE IF EXISTS a REMOVE TENANTS = b,c;"#,
        r#"DESC SHARE b;"#,
        r#"DESCRIBE SHARE b;"#,
        r#"SHOW SHARES;"#,
        r#"SHOW GRANTS ON TABLE db1.tb1;"#,
        r#"SHOW GRANTS ON DATABASE db;"#,
        r#"SHOW GRANTS OF SHARE t;"#,
        r#"UPDATE db1.tb1 set a = a + 1, b = 2 WHERE c > 3;"#,
        r#"SET max_threads = 10;"#,
        r#"SET max_threads = 10*2;"#,
        r#"UNSET max_threads;"#,
        r#"UNSET (max_threads, sql_dialect);"#,
        r#"select $1 FROM '@my_stage/my data/'"#,
        r#"SELECT t.c1 FROM @stage1/dir/file
        ( file_format => 'PARQUET', FILES => ('file1', 'file2')) t;"#,
        r#"select table0.c1, table1.c2 from
            @stage1/dir/file ( FILE_FORMAT => 'parquet', FILES => ('file1', 'file2')) table0
            left join table1;"#,
        r#"SELECT c1 FROM 's3://test/bucket' (ENDPOINT_URL => 'xxx', PATTERN => '*.parquet') t;"#,
        r#"CREATE FILE FORMAT my_csv
            type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1;"#,
        r#"SHOW FILE FORMATS"#,
        r#"DROP FILE FORMAT my_csv"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS (a, b, c, d)"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS (a, b, (c, d))"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b), (c), (d, e))"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b), (), (d, e))"#,
        r#"SELECT * FROM t GROUP BY CUBE (a, b, c)"#,
        r#"SELECT * FROM t GROUP BY ROLLUP (a, b, c)"#,
        r#"CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() IN ('ANALYST') THEN VAL ELSE '*********'END comment = 'this is a masking policy'"#,
        r#"DESC MASKING POLICY email_mask"#,
        r#"DROP MASKING POLICY IF EXISTS email_mask"#,
        r#"CREATE VIRTUAL COLUMN (a['k1']['k2'], b[0][1]) FOR t"#,
        r#"ALTER VIRTUAL COLUMN (a['k1']['k2'], b[0][1]) FOR t"#,
        r#"DROP VIRTUAL COLUMN FOR t"#,
        r#"REFRESH VIRTUAL COLUMN FOR t"#,
        r#"CREATE NETWORK POLICY mypolicy ALLOWED_IP_LIST=('192.168.10.0/24') BLOCKED_IP_LIST=('192.168.10.99') COMMENT='test'"#,
        r#"ALTER NETWORK POLICY mypolicy SET ALLOWED_IP_LIST=('192.168.10.0/24','192.168.255.1') BLOCKED_IP_LIST=('192.168.1.99') COMMENT='test'"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 15 MINUTE SUSPEND_TASK_AFTER_NUM_FAILURES = 3 COMMENT = 'This is test task 1' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 6 * * *' 'America/Los_Angeles' COMMENT = 'serverless + cron' AS insert into t (c1, c2) values (1, 2), (3, 4)"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 12 * * *' AS VACUUM TABLE t"#,
        "--各环节转各环节转各环节转各环节转各\n  select 34343",
        "-- 96477300355	31379974136	3.074486292973661\nselect 34343",
        "-- xxxxx\n  select 34343;",
    ];

    for case in cases {
        let tokens = tokenize_sql(case).unwrap();
        let (stmt, fmt) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", case).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", stmt).unwrap();
        writeln!(file, "---------- AST ------------").unwrap();
        writeln!(file, "{:#?}", stmt).unwrap();
        writeln!(file, "\n").unwrap();
        if fmt.is_some() {
            writeln!(file, "---------- FORMAT ------------").unwrap();
            writeln!(file, "{:#?}", fmt).unwrap();
        }
    }
}

#[test]
fn test_statement_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement-error.txt").unwrap();

    let cases = &[
        r#"create table a.b (c integer not null 1, b float(10))"#,
        r#"create table a (c float(10))"#,
        r#"create table a (c varch)"#,
        r#"create table a (c tuple())"#,
        r#"create table a (c decimal)"#,
        r#"create table a (b tuple(c int, uint64));"#,
        r#"CREATE TABLE t(c1 NULLABLE(int) NOT NULL);"#,
        r#"drop table if a.b"#,
        r#"truncate table a.b.c.d"#,
        r#"truncate a"#,
        r#"drop a"#,
        r#"insert into t format"#,
        r#"show tables format"#,
        r#"alter database system x rename to db"#,
        r#"create user 'test-e' identified bi 'password';"#,
        r#"create user 'test-e'@'localhost' identified by 'password';"#,
        r#"drop usar if exists 'test-j';"#,
        r#"alter user 'test-e' identifies by 'new-password';"#,
        r#"create role 'test'@'%';"#,
        r#"drop role 'test'@'%';"#,
        r#"SHOW GRANT FOR ROLE 'role1';"#,
        r#"GRANT ROLE 'test' TO ROLE test-user;"#,
        r#"GRANT SELECT, ALL PRIVILEGES, CREATE ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON *.c TO 'test-grant';"#,
        r#"REVOKE SELECT, CREATE, ALL PRIVILEGES ON * FROM 'test-grant';"#,
        r#"REVOKE SELECT, CREATE ON * TO 'test-grant';"#,
        r#"COPY INTO mytable FROM 's3://bucket' CREDENTIAL = ();"#,
        r#"COPY INTO mytable FROM @mystage CREDENTIALS = ();"#,
        r#"CALL system$test"#,
        r#"CALL system$test(a"#,
        r#"show settings ilike 'enable%'"#,
        r#"PRESIGN INVALID @my_stage/path/to/file"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS a, b"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ()"#,
        r#"select * from aa.bb limit 10 order by bb;"#,
        r#"select * from aa.bb offset 10 order by bb;"#,
        r#"select * from aa.bb offset 10 limit 1;"#,
        r#"select * from aa.bb order by a order by b;"#,
        r#"select * from aa.bb offset 10 offset 20;"#,
        r#"select * from aa.bb limit 10 limit 20;"#,
        r#"select * from aa.bb limit 10,2 offset 2;"#,
        r#"select * from aa.bb limit 10,2,3;"#,
        r#"with a as (select 1) with b as (select 2) select * from aa.bb;"#,
        r#"copy into t1 from "" FILE"#,
        r#"copy into t1 from "" FILE_FORMAT"#,
        r#"copy into t1 from "" FILE_FORMAT = "#,
        r#"copy into t1 from "" FILE_FORMAT = ("#,
        r#"copy into t1 from "" FILE_FORMAT = (TYPE"#,
        r#"copy into t1 from "" FILE_FORMAT = (TYPE ="#,
        r#"copy into t1 from "" FILE_FORMAT = (TYPE ="#,
        r#"COPY INTO t1 FROM "" PATTERN = '.*[.]csv' FILE_FORMAT = (type = TSV field_delimiter = '\t' skip_headerx = 0);"#,
        r#"COPY INTO mytable
                FROM @my_stage
                FILE_FORMAT = (
                    type = CSV,
                    error_on_column_count_mismatch = 1
                )"#,
    ];

    for case in cases {
        let tokens = tokenize_sql(case).unwrap();
        let err = parse_sql(&tokens, Dialect::PostgreSQL).unwrap_err();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", case).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", err.message()).unwrap();
    }
}

#[test]
fn test_query() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("query.txt").unwrap();
    let cases = &[
        r#"select * exclude c1, b.* exclude (c2, c3, c4) from customer inner join orders on a = b limit 1"#,
        r#"select * from customer inner join orders"#,
        r#"select * from customer cross join orders"#,
        r#"select * from customer inner join orders on (a = b)"#,
        r#"select * from customer inner join orders on a = b limit 1"#,
        r#"select * from customer inner join orders on a = b limit 2 offset 3"#,
        r#"select * from customer natural full join orders"#,
        r#"select * from customer natural join orders left outer join detail using (id)"#,
        r#"with t2(tt) as (select a from t) select t2.tt from t2  where t2.tt > 1"#,
        r#"with t2(tt) as materialized (select a from t) select t2.tt from t2  where t2.tt > 1"#,
        r#"with t2 as (select a from t) select t2.a from t2  where t2.a > 1"#,
        r#"with t2(tt) as materialized (select a from t), t3 as materialized (select * from t), t4 as (select a from t where a > 1) select t2.tt, t3.a, t4.a from t2, t3, t4 where t2.tt > 1"#,
        r#"with recursive t2(tt) as (select a from t1 union select tt from t2) select t2.tt from t2"#,
        r#"with t(a,b) as (values(1,1),(2,null),(null,5)) select t.a, t.b from t"#,
        r#"select c_count cc, count(*) as custdist, sum(c_acctbal) as totacctbal
            from customer, orders ODS,
                (
                    select
                        c_custkey,
                        count(o_orderkey)
                    from
                        customer left outer join orders on
                            c_custkey = o_custkey
                            and o_comment not like '%:1%:2%'
                    group by
                        c_custkey
                ) as c_orders
            group by c_count
            order by custdist desc nulls first, c_count asc, totacctbal nulls last
            limit 10, totacctbal"#,
        r#"select * from t1 union select * from t2"#,
        r#"select * from t1 except select * from t2"#,
        r#"select * from t1 union select * from t2 union select * from t3"#,
        r#"select * from t1 union select * from t2 union all select * from t3"#,
        r#"select * from t1 union select * from t2 intersect select * from t3"#,
        r#"(select * from t1 union select * from t2) union select * from t3"#,
        r#"select * from t1 union (select * from t2 union select * from t3)"#,
        r#"SELECT * FROM ((SELECT *) EXCEPT (SELECT *)) foo"#,
        r#"SELECT * FROM (((SELECT *) EXCEPT (SELECT *))) foo"#,
        r#"SELECT * FROM (SELECT * FROM xyu ORDER BY x, y) AS xyu"#,
        r#"select * from monthly_sales pivot(sum(amount) for month in ('JAN', 'FEB', 'MAR', 'APR')) order by empid"#,
        r#"select * from monthly_sales_1 unpivot(sales for month in (jan, feb, mar, april)) order by empid"#,
        r#"select * from range(1, 2)"#,
        r#"select sum(a) over w from customer window w as (partition by a order by b)"#,
        r#"select a, sum(a) over w, sum(a) over w1, sum(a) over w2 from t1 window w as (partition by a), w2 as (w1 rows current row), w1 as (w order by a) order by a"#,
        r#"SELECT * FROM ((SELECT * FROM xyu ORDER BY x, y)) AS xyu"#,
        r#"SELECT * FROM (VALUES(1,1),(2,null),(null,5)) AS t(a,b)"#,
        r#"VALUES(1,'a'),(2,'b'),(null,'c') order by col0 limit 2"#,
    ];

    for case in cases {
        run_parser!(file, query, case);
    }
}

#[test]
fn test_query_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("query-error.txt").unwrap();
    let cases = &[
        r#"select * from customer join where a = b"#,
        r#"select * from join customer"#,
        r#"select * from customer natural inner join orders on a = b"#,
        r#"select * order a"#,
        r#"select * order"#,
        r#"select number + 5 as a, cast(number as float(255))"#,
        r#"select 1 1"#,
    ];

    for case in cases {
        run_parser!(file, query, case);
    }
}

#[test]
fn test_expr() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("expr.txt").unwrap();

    let cases = &[
        r#"a"#,
        r#"'I''m who I\'m.'"#,
        r#"'\776 \n \t \u0053 \xaa'"#,
        r#"char(0xD0, 0xBF, 0xD1)"#,
        r#"[42, 3.5, 4., .001, 5e2, 1.925e-3, .38e+7, 1.e-01, 0xfff, x'deedbeef']"#,
        r#"123456789012345678901234567890"#,
        r#"x'123456789012345678901234567890'"#,
        r#"1e100000000000000"#,
        r#".1"#,
        r#"-1"#,
        r#"(1)"#,
        r#"(1,)"#,
        r#"(1,2)"#,
        r#"(1,2,)"#,
        r#"[1]"#,
        r#"[1,]"#,
        r#"[[1]]"#,
        r#"[[1],[2]]"#,
        r#"[[[1,2,3],[4,5,6]],[[7,8,9]]][0][1][2]"#,
        r#"((1 = 1) or 1)"#,
        r#"typeof(1 + 2)"#,
        r#"- - + + - 1 + + - 2"#,
        r#"0XFF + 0xff + 0xa + x'ffff'"#,
        r#"1 - -(- - -1)"#,
        r#"1 + a * c.d"#,
        r#"number % 2"#,
        r#""t":k1.k2"#,
        r#""t":k1.k2.0"#,
        r#"t.0"#,
        r#"(NULL,).0"#,
        r#"col1 not between 1 and 2"#,
        r#"sum(col1)"#,
        r#""random"()"#,
        r#"random(distinct)"#,
        r#"covar_samp(number, number)"#,
        r#"CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS TUPLE(BIGINT UNSIGNED NULL, BOOLEAN))"#,
        r#"trim(leading 'abc' from 'def')"#,
        r#"extract(year from d)"#,
        r#"date_part(year, d)"#,
        r#"position('a' in str)"#,
        r#"substring(a from b for c)"#,
        r#"substring(a, b, c)"#,
        r#"col1::UInt8"#,
        r#"(arr[0]:a).b"#,
        r#"arr[4]["k"]"#,
        r#"a rlike '^11'"#,
        r#"G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)"#,
        r#"sum(CASE WHEN n2.n_name = 'GERMANY' THEN ol_amount ELSE 0 END) / CASE WHEN sum(ol_amount) = 0 THEN 1 ELSE sum(ol_amount) END"#,
        r#"p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
            AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'"#,
        r#"nullif(1, 1)"#,
        r#"nullif(a, b)"#,
        r#"coalesce(1, 2, 3)"#,
        r#"coalesce(a, b, c)"#,
        r#"ifnull(1, 1)"#,
        r#"ifnull(a, b)"#,
        r#"1 is distinct from 2"#,
        r#"a is distinct from b"#,
        r#"1 is not distinct from null"#,
        r#"{'k1':1,'k2':2}"#,
        // window expr
        r#"ROW_NUMBER() OVER (ORDER BY salary DESC)"#,
        r#"SUM(salary) OVER ()"#,
        r#"AVG(salary) OVER (PARTITION BY department)"#,
        r#"SUM(salary) OVER (PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"#,
        r#"AVG(salary) OVER (PARTITION BY department ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) "#,
        r#"COUNT() OVER (ORDER BY hire_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS UNBOUNDED PRECEDING)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS 3 PRECEDING)"#,
        r#"ARRAY_APPLY([1,2,3], x -> x + 1)"#,
        r#"ARRAY_FILTER(col, y -> y % 2 = 0)"#,
    ];

    for case in cases {
        run_parser!(file, expr, case);
    }
}

#[test]
fn test_expr_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("expr-error.txt").unwrap();

    let cases = &[
        r#"5 * (a and ) 1"#,
        r#"a + +"#,
        r#"CAST(col1 AS foo)"#,
        r#"1 a"#,
        r#"CAST(col1)"#,
        r#"G.E.B IS NOT NULL AND
            col1 NOT BETWEEN col2 AND
                AND 1 + col3 DIV sum(col4)"#,
    ];

    for case in cases {
        run_parser!(file, expr, case);
    }
}

#[test]
fn test_quote() {
    let cases = &[
        ("a", "a"),
        ("_", "_"),
        ("_abc", "_abc"),
        ("_abc12", "_abc12"),
        ("_12a", "_12a"),
        ("12a", "\"12a\""),
        ("12", "\"12\""),
        ("🍣", "\"🍣\""),
        ("価格", "\"価格\""),
        ("\t", "\"\t\""),
        ("complex \"string\"", "\"complex \"\"string\"\"\""),
        ("\"\"\"", "\"\"\"\"\"\"\"\""),
        ("'''", "\"'''\""),
        ("name\"with\"quote", "\"name\"\"with\"\"quote\""),
    ];
    for (input, want) in cases {
        let quoted = quote_ident(input, '"', false);
        assert_eq!(quoted, *want);
        let unquoted = unquote_ident(&quoted, '"');
        assert_eq!(unquoted, *input, "unquote({}) got {}", quoted, unquoted);
    }
}
