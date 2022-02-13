SELECT ELT(0, 'a', 'b', 'c'); -- {ErrorCode 1102}
SELECT ELT(1, 'a', 'b', 'c');
SELECT ELT(2, 'a', 'b', 'c');
SELECT ELT(3, 'a', 'b', 'c');
SELECT ELT(4, 'a', 'b', 'c'); -- {ErrorCode 1102}
SELECT ELT(number+1, 'a', 'b', toString(number * 10)) FROM numbers(3) ORDER BY number;
