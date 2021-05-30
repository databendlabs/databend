select toTypeName(number) from numbers(100) limit 1;
select toTypeName(number + 1), toTypeName(number - 1),
       toTypeName(number / 1), toTypeName(number * 1) from numbers(100) limit 1;
select toTypeName('33'), toTypeName('44');


select '=== TEST_numeric_coercion';
select 'UInt64 OP UInt64', toTypeName(1 + 65536 * 65536), toTypeName(1-65536 * 65536), toTypeName(1 * 65536 * 65536), toTypeName(1/(65536 * 65536)) ;
