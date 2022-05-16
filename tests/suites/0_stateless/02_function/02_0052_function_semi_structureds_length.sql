-- Array
select length(parse_json('[1, 2, [1, 2]]'));
select length(parse_json('[]'));

-- Array<T>
DROP TABLE IF EXISTS array_length_test;
CREATE TABLE array_length_test(number Array(UInt8));

INSERT INTO array_length_test VALUES([1,2]), ([3,4]);
SELECT LENGTH(number) FROM array_length_test ;

DROP TABLE array_length_test;