
## Stateless Tests Spec

### Test directory structure (like this)

```
00_dummy/
├── 00_0000_dummy_select_1.result
├── 00_0000_dummy_select_1.sql
├── 00_0000_dummy_select_sh.sh
├── 00_0001_select_with_stackoverflow.result
├── 00_0001_select_with_stackoverflow.sql
├── 00_0002_dummy_select_py.py
├── 00_0002_dummy_select_py.result
└── 00_0002_dummy_select_sh.result
```

### Test Name

xx_yyyy_[test_name]
* xx is category no
* yyyy is the sequence no under the category

### Test Category

* 00_dummy -- for Dummy tests
* 01_system -- for System tables tests
* 02_function -- for Function tests
* 03_dml -- for `SELECT`, `INSERT`, `UPDATE`, `DELETE` tests
* 04_explain -- for `EXPLAIN` tests
* 05_ddl -- for DDL tests
* 06_show -- for `SHOW` statement tests
* 07_use -- for `USE` statement tests
* 08_optimizer -- for Optimizer tests
* 09_fuse_engine -- for `FUSE` storage engine tests
* 10_stage -- for `STAGE` tests
* 20_others -- for other tests

Note: If your test is not in the above category, please add it.



