select toTypeName(number) from system.numbers(100) limit 1;
select toTypeName(number + 1), toTypeName(number - 1),
       toTypeName(number / 1), toTypeName(number * 1) from system.numbers(100) limit 1;

select database();


