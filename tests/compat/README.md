# Test compatibility between databend-query and databend-meta

## What it does
- Find out the min compatible version of databend-query for the latest databend-meta, and test compatibility between them.
- Find out the min compatible version of databend-meta for the latest databend-query, and test compatibility between them.

To ensure the compatibility, we run several sql-logic tests on the old query plus latest meta and old meta plus latest query.