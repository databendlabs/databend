select '=======> array_array';

CREATE TABLE strings_repeat_sample_u8(s String, n Uint8) engine=Memory;
CREATE TABLE strings_repeat_sample_u16 (s String, n Uint16) engine=Memory;
CREATE TABLE strings_repeat_sample_u32 (s String, n Uint32) engine=Memory;
CREATE TABLE strings_repeat_sample_u64 (s String, n Uint64) engine=Memory;

INSERT INTO strings_repeat_sample_u8 VALUES ('abc', 3), ('abc', 0);
INSERT INTO strings_repeat_sample_u16 VALUES ('abc', 3), ('abc', 0);
INSERT INTO strings_repeat_sample_u32 VALUES ('abc', 3), ('abc', 0);
INSERT INTO strings_repeat_sample_u64 VALUES ('abc', 3), ('abc', 0);

select repeat(s, n) from strings_repeat_sample_u8;
select repeat(s, n) from strings_repeat_sample_u16;
select repeat(s, n) from strings_repeat_sample_u32;
select repeat(s, n) from strings_repeat_sample_u64;

drop table strings_repeat_sample_u8;
drop table strings_repeat_sample_u16;
drop table strings_repeat_sample_u32;
drop table strings_repeat_sample_u64;

select '=======> const_const';
select repeat('abc', 3);
select repeat('abc', 0);

select '=======> const_array';
CREATE TABLE strings_repeat_sample_2_u8(n Uint8) engine=Memory;
CREATE TABLE strings_repeat_sample_2_u16 (n Uint16) engine=Memory;
CREATE TABLE strings_repeat_sample_2_u32 (n Uint32) engine=Memory;
CREATE TABLE strings_repeat_sample_2_u64 (n Uint64) engine=Memory;

INSERT INTO strings_repeat_sample_2_u8 VALUES (3), (0);
INSERT INTO strings_repeat_sample_2_u16 VALUES (3), (0);
INSERT INTO strings_repeat_sample_2_u32 VALUES (3), (0);
INSERT INTO strings_repeat_sample_2_u64 VALUES (3), (0);

select repeat('abc', n) from strings_repeat_sample_2_u8;
select repeat('abc', n) from strings_repeat_sample_2_u16;
select repeat('abc', n) from strings_repeat_sample_2_u32;
select repeat('abc', n) from strings_repeat_sample_2_u64;


drop table strings_repeat_sample_2_u8;
drop table strings_repeat_sample_2_u16;
drop table strings_repeat_sample_2_u32;
drop table strings_repeat_sample_2_u64;

select '=======> array_const';

CREATE TABLE strings_repeat_sample_3(s String) engine=Memory;

INSERT INTO strings_repeat_sample_3 VALUES ('abc'), ('def');

select repeat(s, 3) from strings_repeat_sample_3;
select repeat(s, 0) from strings_repeat_sample_3;

drop table strings_repeat_sample_3;
