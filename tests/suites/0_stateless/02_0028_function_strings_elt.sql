SELECT ELT(0, 'a', 'b', 'c');
SELECT ELT(1, 'a', 'b', 'c');
SELECT ELT(2, 'a', 'b', 'c');
SELECT ELT(3, 'a', 'b', 'c');
SELECT ELT(4, 'a', 'b', 'c');
SELECT ELT(number, 'a', 'b', number * 10) FROM numbers(5) ORDER BY number;
