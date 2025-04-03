select number % 3 a, number % 4 b, number % 5 c, min(number), max(number), sum(number) from numbers(100000000) group by a,b,c;
