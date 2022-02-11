select unhex('616263');
select unhex('hello'); -- {ErrorCode 1054}
select unhex(hex('hello'));
select unhex(null);
