SELECT pi();
SELECT abs(-1);
SELECT abs(-10086);
SELECT abs('str'); -- {ErrorCode 7}
SELECT abs(NULL); -- {ErrorCode 7}
