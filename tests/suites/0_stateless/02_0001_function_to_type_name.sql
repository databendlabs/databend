select toTypeName(number) from numbers(100) limit 1;
select toTypeName(number + 1), toTypeName(number - 1),
       toTypeName(number / 1), toTypeName(number * 1) from numbers(100) limit 1;
select toTypeName('33'), toTypeName('44');


select '=== TEST_numeric_coercion';
select 'UInt8 OP UInt8',  toTypeName(1 + 2),             toTypeName(1-2),             toTypeName(1 * 2),             toTypeName(1/2) ;
select 'UInt8 OP UInt16', toTypeName(1 + 256),           toTypeName(1-256),           toTypeName(1 * 256),           toTypeName(1/256) ;
select 'UInt8 OP UInt32', toTypeName(1 + 65536),         toTypeName(1-65536),         toTypeName(1 * 65536),         toTypeName(1/65536) ;
select 'UInt8 OP UInt64', toTypeName(1 + 65536 * 65536), toTypeName(1-65536 * 65536), toTypeName(1 * 65536 * 65536), toTypeName(1/(65536 * 65536)) ;


select 'UInt16 OP UInt8',  toTypeName(256 + 2),             toTypeName(256 - 2),              toTypeName(256 * 2),              toTypeName(256 / 2) ;
select 'UInt16 OP UInt16', toTypeName(256 + 256),           toTypeName(256 - 256),            toTypeName(256 * 256),            toTypeName(256 / 256) ;
select 'UInt16 OP UInt32', toTypeName(256 + 65536),         toTypeName(256 - 65536),          toTypeName(256 * 65536),          toTypeName(256 / 65536) ;
select 'UInt16 OP UInt64', toTypeName(256 + 65536 * 65536), toTypeName(256 - 65536 * 65536),  toTypeName(256 * 65536 * 65536),  toTypeName(256 / (65536 * 65536)) ;


select 'UInt32 OP UInt8',  toTypeName(65536 + 2),             toTypeName(65536 - 2),              toTypeName(65536 * 2),              toTypeName(65536 / 2) ;
select 'UInt32 OP UInt16', toTypeName(65536 + 256),           toTypeName(65536 - 256),            toTypeName(65536 * 256),            toTypeName(65536 / 256) ;
select 'UInt32 OP UInt32', toTypeName(65536 + 65536),         toTypeName(65536 - 65536),          toTypeName(65536 * 65536),          toTypeName(65536 / 65536) ;
select 'UInt32 OP UInt64', toTypeName(65536 + 65536 * 65536), toTypeName(65536 - 65536 * 65536),  toTypeName(65536 * 65536 * 65536),  toTypeName(65536 / (65536 * 65536)) ;



select 'UInt64 OP UInt8',  toTypeName(65536 * 65536 + 2),             toTypeName(65536 * 65536 - 2),              toTypeName(65536 * 65536 * 2),              toTypeName(65536 * 65536 / 2) ;
select 'UInt64 OP UInt16', toTypeName(65536 * 65536 + 256),           toTypeName(65536 * 65536 - 256),            toTypeName(65536 * 65536 * 256),            toTypeName(65536 * 65536 / 256) ;
select 'UInt64 OP UInt32', toTypeName(65536 * 65536 + 65536),         toTypeName(65536 * 65536 - 65536),          toTypeName(65536 * 65536 * 65536),          toTypeName(65536 * 65536 / 65536) ;
select 'UInt64 OP UInt64', toTypeName(65536 * 65536 + 65536 * 65536), toTypeName(65536 * 65536 - 65536 * 65536),  toTypeName(65536 * 65536 * 65536),  toTypeName(65536 * 65536 / (65536 * 65536)) ;

select '=== TEST_datetimes';

select toTypeName(now()) = 'DateTime32';
select toTypeName(today()) = 'Date16';
