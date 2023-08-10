## Databend UDF Server Tests

```sh
pip install pyarrow
# start UDF server
python3 udf.py
```

```sql
mysql> create function str_cmp as (string, string) -> int32 address='http://localhost:8815';
Query OK, 0 rows affected (0.03 sec)

mysql> select str_cmp('abc', 'abc'), str_cmp('abc', 'abd'), str_cmp('abc', 'aaa');
+-----------------------+-----------------------+-----------------------+
| str_cmp('abc', 'abc') | str_cmp('abc', 'abd') | str_cmp('abc', 'aaa') |
+-----------------------+-----------------------+-----------------------+
|                     0 |                    -1 |                     1 |
+-----------------------+-----------------------+-----------------------+
1 row in set (0.36 sec)
```