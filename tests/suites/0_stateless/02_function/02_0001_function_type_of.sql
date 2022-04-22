select typeof(number) from numbers(100) limit 1;
select typeof(number + 1), typeof(number - 1),
       typeof(number / 1), typeof(number * 1) from numbers(100) limit 1;
select typeof('33'), typeof('44'), typeof(Null);


select '=== TEST_numeric_coercion';
select 'UInt8 OP UInt8',  typeof(1 + 2),             typeof(1-2),             typeof(1 * 2),             typeof(1/2) ;
select 'UInt8 OP UInt16', typeof(1 + 256),           typeof(1-256),           typeof(1 * 256),           typeof(1/256) ;
select 'UInt8 OP UInt32', typeof(1 + 65536),         typeof(1-65536),         typeof(1 * 65536),         typeof(1/65536) ;
select 'UInt8 OP UInt64', typeof(1 + 65536 * 65536), typeof(1-65536 * 65536), typeof(1 * 65536 * 65536), typeof(1/(65536 * 65536)) ;


select 'UInt16 OP UInt8',  typeof(256 + 2),             typeof(256 - 2),              typeof(256 * 2),              typeof(256 / 2) ;
select 'UInt16 OP UInt16', typeof(256 + 256),           typeof(256 - 256),            typeof(256 * 256),            typeof(256 / 256) ;
select 'UInt16 OP UInt32', typeof(256 + 65536),         typeof(256 - 65536),          typeof(256 * 65536),          typeof(256 / 65536) ;
select 'UInt16 OP UInt64', typeof(256 + 65536 * 65536), typeof(256 - 65536 * 65536),  typeof(256 * 65536 * 65536),  typeof(256 / (65536 * 65536)) ;


select 'UInt32 OP UInt8',  typeof(65536 + 2),             typeof(65536 - 2),              typeof(65536 * 2),              typeof(65536 / 2) ;
select 'UInt32 OP UInt16', typeof(65536 + 256),           typeof(65536 - 256),            typeof(65536 * 256),            typeof(65536 / 256) ;
select 'UInt32 OP UInt32', typeof(65536 + 65536),         typeof(65536 - 65536),          typeof(65536 * 65536),          typeof(65536 / 65536) ;
select 'UInt32 OP UInt64', typeof(65536 + 65536 * 65536), typeof(65536 - 65536 * 65536),  typeof(65536 * 65536 * 65536),  typeof(65536 / (65536 * 65536)) ;



select 'UInt64 OP UInt8',  typeof(65536 * 65536 + 2),             typeof(65536 * 65536 - 2),              typeof(65536 * 65536 * 2),              typeof(65536 * 65536 / 2) ;
select 'UInt64 OP UInt16', typeof(65536 * 65536 + 256),           typeof(65536 * 65536 - 256),            typeof(65536 * 65536 * 256),            typeof(65536 * 65536 / 256) ;
select 'UInt64 OP UInt32', typeof(65536 * 65536 + 65536),         typeof(65536 * 65536 - 65536),          typeof(65536 * 65536 * 65536),          typeof(65536 * 65536 / 65536) ;
select 'UInt64 OP UInt64', typeof(65536 * 65536 + 65536 * 65536), typeof(65536 * 65536 - 65536 * 65536),  typeof(65536 * 65536 * 65536),  typeof(65536 * 65536 / (65536 * 65536)) ;

select '=== TEST_datetimes';

select typeof(now()) = 'DateTime';
select typeof(today()) = 'Date';
