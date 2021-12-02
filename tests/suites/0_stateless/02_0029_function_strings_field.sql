SELECT FIELD(3, 77, 3) from numbers(5);
SELECT FIELD(3, 77, number+1) from numbers(5);
SELECT FIELD(number+1, 77, 3) from numbers(5);
SELECT FIELD(number+1, 77, 3) from numbers(5);
SELECT FIELD(number, 77, 4-number) from numbers(5);
