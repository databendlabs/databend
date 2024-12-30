select max(number) over (partition by number % 3 order by number), rank() over (partition by number % 3 order by number) from numbers(20000000) where number % 100 != 0 ignore_result;
