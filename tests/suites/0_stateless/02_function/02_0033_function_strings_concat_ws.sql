SELECT CONCAT_WS(',', 'data', 'fuse', 'labs', '2021');
SELECT CONCAT_WS(',', 'data', NULL, 'bend');
SELECT CONCAT_WS(',', 'data', NULL, NULL, 'bend');
SELECT CONCAT_WS(NULL, 'data', 'fuse', 'labs');
SELECT CONCAT_WS(',', toString(number), 'data', toString(number+1)) from numbers(3) order by number;
SELECT CONCAT_WS(NULL, toString(number), 'data') from numbers(3);
SELECT CONCAT_WS(',', NULL); -- is emtpy, not NULL
