CREATE TABLE strings_oct_sample_u8 (value Uint8) engine=Memory;
CREATE TABLE strings_oct_sample_u16 (value Uint16) engine=Memory;
CREATE TABLE strings_oct_sample_u32 (value Uint32) engine=Memory;
CREATE TABLE strings_oct_sample_u64 (value Uint64) engine=Memory;
CREATE TABLE strings_oct_sample_i8 (value Int8) engine=Memory;
CREATE TABLE strings_oct_sample_i16 (value Int16) engine=Memory;
CREATE TABLE strings_oct_sample_i32 (value Int32) engine=Memory;
CREATE TABLE strings_oct_sample_i64 (value Int64) engine=Memory;

INSERT INTO strings_oct_sample_u8 VALUES (0), (255), (127), (NULL);
INSERT INTO strings_oct_sample_u16 VALUES (0), (65535), (32768), (NULL);
INSERT INTO strings_oct_sample_u32 VALUES (0), (4294967295), (2147483647), (NULL);
INSERT INTO strings_oct_sample_u64 VALUES (0), (18446744073709551615), (9223372036854775807), (NULL);
INSERT INTO strings_oct_sample_i8 VALUES (0), ('-128'), (127), (NULL);
INSERT INTO strings_oct_sample_i16 VALUES (0), ('-32768'), (32767), (NULL);
INSERT INTO strings_oct_sample_i32 VALUES (0), ('-2147483648'), (2147483647), (NULL);
INSERT INTO strings_oct_sample_i64 VALUES (0), ('-9223372036854775808'), (9223372036854775807), (NULL);

select oct(value) from strings_oct_sample_u8;
select oct(value) from strings_oct_sample_u16;
select oct(value) from strings_oct_sample_u32;
select oct(value) from strings_oct_sample_u64;
select oct(value) from strings_oct_sample_i8;
select oct(value) from strings_oct_sample_i16;
select oct(value) from strings_oct_sample_i32;
select oct(value) from strings_oct_sample_i64;

select oct(-128);
select oct(127);

DROP TABLE strings_oct_sample_u8;
DROP TABLE strings_oct_sample_u16;
DROP TABLE strings_oct_sample_u32;
DROP TABLE strings_oct_sample_u64;
DROP TABLE strings_oct_sample_i8;
DROP TABLE strings_oct_sample_i16;
DROP TABLE strings_oct_sample_i32;
DROP TABLE strings_oct_sample_i64;
