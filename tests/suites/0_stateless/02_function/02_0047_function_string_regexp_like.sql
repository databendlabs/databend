select * from system.databases where regexp_like(name, '^sys') ;
select * from system.databases where regexp_like(name, 'ef+.ul+.') ;
select regexp_like('foo', 'FOO');
select regexp_like('foo', null);
select regexp_like(null, null);
select regexp_like('foo', 'FOO', null);
select regexp_like('foo', 'FOO', 'c');
select regexp_like('foo', 'FOO', 'i');

-- test case refer to: https://dev.mysql.com/doc/refman/8.0/en/regexp.html
-- match the beginning of a string.
select regexp_like('fo\nfo', '^fo$');
select regexp_like('fofo', '^fo'); 
select regexp_like('fo\nfo', '^fo$', 'm');
-- match the end of a string.
select regexp_like('fo\no', '^fo\no$');
select regexp_like('fo\no', '^fo$');
select regexp_like('fo\no', '^fo$', 'm');
-- match any character
SELECT REGEXP_LIKE('fofo', '^f.*$');
SELECT REGEXP_LIKE('fo\r\nfo', '^f.*$');
SELECT REGEXP_LIKE('fo\r\nfo', '^f.*$', 'm');
SELECT REGEXP_LIKE('fo\r\nfo', '(?m)^f.*$');
SELECT REGEXP_LIKE('fo\r\nfo', '^f.*$', 'n');
-- match any sequence of zero or more a characters.
SELECT REGEXP_LIKE('Ban', '^Ba*n');
SELECT REGEXP_LIKE('Baaan', '^Ba*n');
SELECT REGEXP_LIKE('Bn', '^Ba*n');
-- match any sequence of one or more a characters.
SELECT REGEXP_LIKE('Ban', '^Ba+n'); 
SELECT REGEXP_LIKE('Bn', '^Ba+n');
-- match either zero or one a character.
SELECT REGEXP_LIKE('Bn', '^Ba?n');
SELECT REGEXP_LIKE('Ban', '^Ba?n');
SELECT REGEXP_LIKE('Baan', '^Ba?n');
-- alternation; match either of the sequences de or abc.
SELECT REGEXP_LIKE('pi', 'pi|apa');
SELECT REGEXP_LIKE('axe', 'pi|apa');
SELECT REGEXP_LIKE('apa', 'pi|apa');
SELECT REGEXP_LIKE('apa', '^(pi|apa)$');
SELECT REGEXP_LIKE('pi', '^(pi|apa)$');
SELECT REGEXP_LIKE('pix', '^(pi|apa)$');
-- match zero or more instances of the sequence abc.
SELECT REGEXP_LIKE('pi', '^(pi)*$'); 
SELECT REGEXP_LIKE('pip', '^(pi)*$');
SELECT REGEXP_LIKE('pipi', '^(pi)*$');
-- repetition
SELECT REGEXP_LIKE('abcde', 'a[bcd]{2}e');
SELECT REGEXP_LIKE('abcde', 'a[bcd]{3}e');
SELECT REGEXP_LIKE('abcde', 'a[bcd]{1,10}e');
-- matches any character that is (or is not, if ^ is used) either a, b, c, d or X.
SELECT REGEXP_LIKE('aXbc', '[a-dXYZ]');
SELECT REGEXP_LIKE('aXbc', '^[a-dXYZ]$');
SELECT REGEXP_LIKE('aXbc', '^[a-dXYZ]+$');
SELECT REGEXP_LIKE('aXbc', '^[^a-dXYZ]+$');
SELECT REGEXP_LIKE('gheis', '^[^a-dXYZ]+$');
SELECT REGEXP_LIKE('gheisa', '^[^a-dXYZ]+$');
-- within a bracket expression (written using [ and ]), [:character_class:] represents a character class that matches all characters belonging to that class. 
SELECT REGEXP_LIKE('justalnums', '[[:alnum:]]+');
SELECT REGEXP_LIKE('!!', '[[:alnum:]]+');

SELECT REGEXP_LIKE('1+2', '1+2'); 
SELECT REGEXP_LIKE('1+2', '1\\+2');
-- regular expression compatibility
select REGEXP_LIKE('🍣🍣b', 'b');
select regexp_like('бжb', 'b');
select regexp_like('µå周çб', '周');
select regexp_like('周周周周', '.*');
