
## Stateless Tests Spec

### Test Name

xx_yyyy_[test_name]
* xx is category
* yyyy is the sequence no under the category

### Test Category

* 00 -- for Dummy tests, [example](00_0000_dummy_select_1.sql)
* 01 -- for System tables tests, [example](01_0000_system_numbers.sql)
* 02 -- for Function tests, [example](02_0000_function_arithmetic.sql)
* 03 -- for Select tests, [example](03_0000_select_aliases.sql)
* 04 -- for Explain tests, [example](04_0000_explain.sql)
* 05 -- for DDL tests, [example](05_0000_ddl_create_tables.sql)
* 06 -- for Show tests, [example](06_0000_show_queries.sql)
* 07 -- for Use tests, [example](07_0000_use_database.sql)
* 08 -- for Optimizer tests, [example](08_0000_optimizer.sql)
* 09 -- for Remote engine tests, [example](09_0000_remote_create_table.sql)
* 10 -- for Describe tests, [example](10_0000_describe_table.sql)

Note: If your test is not in the above category, please add it.



