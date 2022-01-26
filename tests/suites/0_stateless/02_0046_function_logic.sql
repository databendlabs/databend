-- and, result: [line1, line8]
SELECT true and false;
SELECT true and true;
SELECT false and false;
SELECT 1 and 0;
SELECT 1 and 1;
SELECT 1 and null;
SELECT number from numbers(10) WHERE number > 5 AND number < 8 ORDER BY number;
-- or, result: [line9, line19]
SELECT true OR false;
SELECT true OR true;
SELECT false OR false;
SELECT 1 OR 0;
SELECT 1 OR 1;
SELECT 0 OR 0;
SELECT 1 OR null;
SELECT number from numbers(10) WHERE number > 7 OR number < 2 ORDER BY number;
-- not, result: [line20, line24]
SELECT not true;
SELECT not false;
SELECT not 1;
SELECT not 0;
SELECT not null;
