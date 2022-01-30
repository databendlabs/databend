CREATE FUNCTION cal AS (a,b,c,d,e) -> a + c * (e / b) - d;
CREATE FUNCTION notnull AS (p) -> not(isnull(p));
SELECT notnull(null);
SELECT notnull('null');
SELECT cal(1, 2, 3, 4, 6);
