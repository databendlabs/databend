select if(number>1, true, false) from numbers(3) order by number;
select if(number>1, number, 1) from numbers(3) order by number;
select if(number<1, 2, number) from numbers(3) order by number;
select if(number>0, 'Z+', 'zero') from numbers(3) order by number;
select if(number<1, true, null) from numbers(3) order by number;
select toTypeName(if(number % 3 = 0, toUInt32(1), toInt64(3))) from numbers(10) limit 1;
select toTypeName(if(number % 3 = 0, toUInt32(1), toFloat32(3))) from numbers(10) limit 1;
