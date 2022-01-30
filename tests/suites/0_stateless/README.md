
## Stateless Tests Spec

### Test directory structure (like this)

* ├── 0_stateless  
* │  ├── 00_dummy
* │  │  ├── 00_0000_dummy_select_1\.result
* │  │  ├── 00_0000_dummy_select_1.sql
* |  |  | ..
* │  ├── 01_system
* │  │  ├── 01_0000_system_numbers.result
* |  |  | ..
* |   ... 

### Test Name

xx_yyyy_[test_name]
* xx is category
* yyyy is the sequence no under the category

### Test Category

* 00_dummy -- for Dummy tests, [example](00_0000_dummy_select_1.sql)
* 01_system -- for System tables tests, [example](01_0000_system_numbers.sql)
* 02_function -- for Function tests, [example](02_0000_function_arithmetic.sql)
* 03_select -- for Select tests, [example](03_0000_select_aliases.sql)
* 04_explain -- for Explain tests, [example](04_0000_explain.sql)
* 05_ddl -- for DDL tests, [example](05_0000_ddl_create_tables.sql)
* 06_show -- for Show tests, [example](06_0000_show_queries.sql)
* 07_use -- for Use tests, [example](07_0000_use_database.sql)
* 08_optimizer -- for Optimizer tests, [example](08_0000_optimizer.sql)
* 09+_others -- for Other tests, [example](09_0000_remote_create_table.sql)

Note: If your test is not in the above category, please add it.



