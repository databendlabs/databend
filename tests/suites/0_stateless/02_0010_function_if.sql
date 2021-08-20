select if(number>1, true, false) from numbers(3) order by number;
select if(number>1, number, 1) from numbers(3) order by number;
select if(number>1, number, number/2) from numbers(3) order by number;
select if(number<1, 2, number) from numbers(3) order by number;
select if(number, 'Z+', 'zero') from numbers(3) order by number;
select if(number<1, true, null) from numbers(3) order by number;
select if(1, number, number/2) from numbers(3) order by number;
