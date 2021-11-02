SELECT pi();
SELECT abs(-1);
SELECT abs(-10086);
SELECT abs('-233.0');
SELECT abs('blah');
SELECT abs(TRUE); -- {ErrorCode 7}
SELECT abs(NULL); -- {ErrorCode 7}