-- https://github.com/datafuselabs/databend/issues/492
SELECT number ,number-1 , number*100 , 1> 100 ,1 < 10 FROM numbers_mt (10) order by number;

-- between
select number from numbers_mt(10) where number not between 4 + 0.1  and 8 - 0.1 order by number;
select number from numbers_mt(10) where number between 4 + 0.1  and 8 - 0.1  order by number;

-- like
select * from system.databases where name like '%sys%';
select * from system.databases where name like '_ef_ul_';
select '%' like '\%';
select '%' like '\\%';

-- not like
select * from system.databases where name not like '%sys%' order by name;
select * from system.databases where name not like '_ef_ul_' order by name;
select '\%' not like '\%';

select * from numbers(10) where null = true;
select * from numbers(10) where null and true;

select '==compare_regexp==';
select * from system.databases where name regexp '^sys';
select * from system.databases where name regexp 'ef+.ul+.';
select 'foo' regexp 'FOO';
select * from system.databases where name rlike '^sys';
select * from system.databases where name rlike 'ef+.ul+.';
select 'foo' rlike 'FOO';

-- test case refer to: https://dev.mysql.com/doc/refman/8.0/en/regexp.html
select 'Michael!' regexp '.*';
select 'new*\n*line' regexp 'new\\*.\\*line';
select 'a' regexp '^[a-d]';
-- match the beginning of a string.
select 'fo\nfo' regexp '^fo$';
select 'fofo' regexp '^fo';
-- match the end of a string.
select 'fo\no' regexp '^fo\no$';
select 'fo\no' regexp '^fo$';
-- match any character
select 'fofo' regexp '^f.*$';
select 'fo\r\nfo' regexp '^f.*$';
select 'fo\r\nfo' regexp '(?m)^f.*$';
-- match any sequence of zero or more a characters.
select 'Ban' regexp '^Ba*n';
select 'Baaan' regexp '^Ba*n';
select 'Bn' regexp '^Ba*n';
-- match any sequence of one or more a characters.
select 'Ban' regexp '^Ba+n';
select 'Bn' regexp '^Ba+n';
-- match either zero or one a character.
select 'Bn' regexp '^Ba?n';
select 'Ban' regexp '^Ba?n';
select 'Baan' regexp '^Ba?n';
-- alternation; match either of the sequences de or abc.
select 'pi' regexp 'pi|apa';
select 'axe' regexp 'pi|apa';
select 'apa' regexp 'pi|apa';
select 'apa' regexp '^(pi|apa)$';
select 'pi' regexp '^(pi|apa)$';
select 'pix' regexp '^(pi|apa)$';
-- match zero or more instances of the sequence abc.
select 'pi' regexp '^(pi)*$';
select 'pip' regexp '^(pi)*$';
select 'pipi' regexp '^(pi)*$';
-- repetition
select 'abcde' regexp 'a[bcd]{2}e';
select 'abcde' regexp 'a[bcd]{3}e';
select 'abcde' regexp 'a[bcd]{1,10}e';
-- matches any character that is (or is not, if ^ is used) either a, b, c, d or X.
select 'aXbc' regexp '[a-dXYZ]';
select 'aXbc' regexp '^[a-dXYZ]$';
select 'aXbc' regexp '^[a-dXYZ]+$';
select 'aXbc' regexp '^[^a-dXYZ]+$';
select 'gheis' regexp '^[^a-dXYZ]+$';
select 'gheisa' regexp '^[^a-dXYZ]+$';
-- within a bracket expression (written using [ and ]), [:character_class:] represents a character class that matches all characters belonging to that class. 
select 'justalnums' regexp '[[:alnum:]]+';
select '!!' regexp '[[:alnum:]]+';

select '1+2' regexp '1+2';
select '1+2' regexp '1\\+2';

-- regular expression compatibility
select '🍣🍣b' regexp 'b';
select 'бжb' regexp 'b';
select 'µå周çб' regexp '周';
select '周周周周' regexp '.*';

select '==compare_not_regexp==';
select 'Michael!' not regexp '.*';
select 'new*\n*line' not regexp 'new\\*.\\*line';
select 'a' not regexp '^[a-d]';
-- match the beginning of a string.
select 'fo\nfo' not regexp '^fo$';
select 'fofo' not regexp '^fo';
-- match the end of a string.
select 'fo\no' not regexp '^fo\no$';
select 'fo\no' not regexp '^fo$';
-- match any character
select 'fofo' not regexp '^f.*$';
select 'fo\r\nfo' not regexp '^f.*$';
select 'fo\r\nfo' not regexp '(?m)^f.*$';
-- match any sequence of zero or more a characters.
select 'Ban' not regexp '^Ba*n';
select 'Baaan' not regexp '^Ba*n';
select 'Bn' not regexp '^Ba*n';
-- match any sequence of one or more a characters.
select 'Ban' not regexp '^Ba+n';
select 'Bn' not regexp '^Ba+n';
-- match either zero or one a character.
select 'Bn' not regexp '^Ba?n';
select 'Ban' not regexp '^Ba?n';
select 'Baan' not regexp '^Ba?n';
-- alternation; match either of the sequences de or abc.
select 'pi' not regexp 'pi|apa';
select 'axe' not regexp 'pi|apa';
select 'apa' not regexp 'pi|apa';
select 'apa' not regexp '^(pi|apa)$';
select 'pi' not regexp '^(pi|apa)$';
select 'pix' not regexp '^(pi|apa)$';
-- match zero or more instances of the sequence abc.
select 'pi' not regexp '^(pi)*$';
select 'pip' not regexp '^(pi)*$';
select 'pipi' not regexp '^(pi)*$';
-- repetition
select 'abcde' not regexp 'a[bcd]{2}e';
select 'abcde' not regexp 'a[bcd]{3}e';
select 'abcde' not regexp 'a[bcd]{1,10}e';
-- matches any character that is (or is not, if ^ is used) either a, b, c, d or X.
select 'aXbc' not regexp '[a-dXYZ]';
select 'aXbc' not regexp '^[a-dXYZ]$';
select 'aXbc' not regexp '^[a-dXYZ]+$';
select 'aXbc' not regexp '^[^a-dXYZ]+$';
select 'gheis' not regexp '^[^a-dXYZ]+$';
select 'gheisa' not regexp '^[^a-dXYZ]+$';
-- within a bracket expression (written using [ and ]), [:character_class:] represents a character class that matches all characters belonging to that class. 
select 'justalnums' not regexp '[[:alnum:]]+';
select '!!' not regexp '[[:alnum:]]+';

select '1+2' not regexp '1+2';
select '1+2' not regexp '1\\+2';

-- regular expression compatibility
select '🍣🍣b' not regexp 'b';
select 'бжb' not regexp 'b';
select 'µå周çб' not regexp '周';
select '周周周周' not regexp '.*';

select '==compare_number_string==';
-- using strict parse by default
select '333' = '333';
select toString(1) = '1';
select toString(111) = '111';
select toString(3 + 4) = '7';

-- TODO remove explicit cast
select '123 ab' = 123; -- {ErrorCode 1007}
select '123' = 123; -- {ErrorCode 1007}
select '7.4' = 7.4; -- {ErrorCode 1007}
select '7.4' > 7; -- {ErrorCode 1007}
select '777.4' < 778; -- {ErrorCode 1007}

select '==compare_datetime==';
-- compare with date/datetime strings
SELECT '2021-03-05' = toDate('2021-03-05');
SELECT '2021-03-05 01:01:01' = toDateTime('2021-03-05 01:01:01');
SELECT '2021-03-05 01:01:02' > toDateTime('2021-03-05 01:01:01');
SELECT '2021-03-06' > toDate('2021-03-05');
SELECT toDateTime('2021-03-05 00:00:00') = toDate('2021-03-05');
SELECT toDateTime('2021-03-05 00:00:01') > toDate('2021-03-05');
SELECT toDateTime('2021-03-04 00:00:01') < toDate('2021-03-05');
SELECT toDateTime(toDate('2021-03-05')) = toDate('2021-03-05');
