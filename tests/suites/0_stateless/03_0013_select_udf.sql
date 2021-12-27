CREATE FUNCTION cal = '@0 + @2 * (@4 / @1) - @3';
CREATE FUNCTION notnull = 'not(isnull(@0))';
SELECT notnull(null);
SELECT notnull('null');
SELECT cal(1, 2, 3, 4, 6);
