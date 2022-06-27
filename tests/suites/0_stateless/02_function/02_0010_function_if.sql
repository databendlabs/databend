select if(number>1, true, false) from numbers(3) order by number;
select if(number>1, number, 1) from numbers(3) order by number;
select if(number<1, 2, number) from numbers(3) order by number;
select if(number>0, 'Z+', 'zero') from numbers(3) order by number;
select if(number<1, true, null) from numbers(3) order by number;
select typeof(if(number % 3 = 0, to_uint32(1), to_int64(3))) from numbers(10) limit 1;
select typeof(if(number % 3 = 0, to_uint32(1), to_float32(3))) from numbers(10) limit 1;
SELECT if (number % 3 = 1, null, number) as a FROM numbers(7) order by number;

-- multi_if
SELECT 'multi-if';
select multi_if(number = 4, 3) from numbers(1); -- {ErrorCode 1028}
select multi_if(number = 4, 3, number = 5, null, number = 6, 'a', null) from numbers(10); -- {ErrorCode 1010}
select multi_if(number = 4, 3, number = 2, 4) from numbers(1); -- {ErrorCode 1028}
select count_if(a = '1'), count_if(a = '2'), count_if(a = '3'), count_if(a is null) from (
	SELECT multi_if (number % 4 = 1, '1', number % 4 = 2, '2', number % 4 = 3, '3', null) as a FROM numbers(100)
);

-- constant
SELECT 'constant';
select if(true, null, number), if(false, null, number) from numbers(1);
select if(true, number, null), if(false, number, null) from numbers(1);
