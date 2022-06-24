SELECT LENGTH(gen_random_uuid()) = 36;
SELECT IGNORE(gen_random_uuid());
SELECT gen_zero_uuid();
SELECT is_empty_uuid(gen_random_uuid());
SELECT is_empty_uuid(gen_zero_uuid());
SELECT is_empty_uuid('5');
SELECT is_empty_uuid(null);
SELECT is_not_empty_uuid(gen_random_uuid());
SELECT is_not_empty_uuid(gen_zero_uuid());
SELECT is_not_empty_uuid('5');
SELECT is_not_empty_uuid(null);

SELECT UNIQ(gen_random_uuid())  = 10, COUNT_IF(LENGTH(gen_random_uuid()) = 36)  = 10 from numbers(10);
