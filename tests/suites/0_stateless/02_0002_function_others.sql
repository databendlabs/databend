select count() from numbers(100) where ignore(number + 1);
select count() from numbers(100) where not ignore(toString(number + 3), 1, 4343, 4343, 'a');
