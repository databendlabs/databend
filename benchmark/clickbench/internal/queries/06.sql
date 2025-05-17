select number % 3 a, number % 4 b, number % 5 c, min(cast(number as Decimal(15,2))), max(cast(number as Decimal(15,2))), sum(cast(number as Decimal(45,2))) from numbers(100000000) group by a,b,c;
