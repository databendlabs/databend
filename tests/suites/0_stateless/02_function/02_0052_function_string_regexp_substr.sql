SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 2);
SELECT REGEXP_SUBSTR('abc def GHI', '[a-z]+', 1, 3, 'c');
SELECT REGEXP_SUBSTR('Customers - (NY)','\\([[:alnum:]\-]+\\)');
SELECT REGEXP_SUBSTR('周周周周', '.*', 2);
SELECT REGEXP_SUBSTR('🍣🍣b', 'b', 2);
SELECT REGEXP_SUBSTR('µå周çб周周', '周+', 3, 2);
SELECT REGEXP_SUBSTR('周 周周 周周周 周周周周', '周+', 2, 3);
SELECT REGEXP_SUBSTR(NULL, '');
SELECT REGEXP_SUBSTR('周 周周', '周+', 5);
