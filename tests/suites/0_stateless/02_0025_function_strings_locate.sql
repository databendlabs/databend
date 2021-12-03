SELECT '===LOCATE===';
SELECT LOCATE('bar', 'foobarbar');
SELECT LOCATE('xbar', 'foobar');
SELECT LOCATE('bar', 'foobarbar', 5);
SELECT '===POSITION===';
SELECT POSITION('bar' IN 'foobarbar');
SELECT POSITION('xbar' IN 'foobar');
SELECT '===INSTR===';
SELECT INSTR('foobarbar', 'bar');
SELECT INSTR('foobar', 'xbar');

