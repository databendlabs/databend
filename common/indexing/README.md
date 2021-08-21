# Indexing

The `common/indexing` mod used to create index and apply index for partition(or file) pruning.
In order to get the top performance, we should arrange for queries to prune large numbers of unnecessary partitions as we can.

# Design

There are serial index in Datafuse, and they are combined as a funnel chain when pruning.

For example, we have a table as:
```
CREATE TABLE t1
(
    name VARCHAR,
    age INT,
    address VARCHAR,
    PRIMARY KEY(name,age)
)
PARTITION BY SUBSTRING(name FROM 0 FOR 2)

INSERT INTO t1 VALUES('jack', 24, 'shanghai'),('bohu', 18, 'beijing'),('bohu', 24, 'ca'),('bob', 21, 'moscow');
SELECT * FROM t1 WHERE name = 'bohu' AND age < 24;
```

## Partition Index

Partition index is the first level to check.

This determines which partitions need to be searched.

```
partitions/
    - bo
    - ja
```

For the query, we first to apply the `PARTITION BY` expression for the constant of the name in filter expression `name = 'bohu' and age < 24`: 
`SUBSTRING('bohu', FROM 0 FOR 2)` is 'bo', then we only need read data/index from 'bo' directory.


Note: filter will be more complex than this, and here is just a simple case.

`partition index` has not yet implemented.

## MinMax Index

Partition index is the second level to check.

This determines which files need to be searched.

For the min/max index, we have:
```
bo/
    - file1.name.minmax
        {
          "col":"name",
          "min":"bob",
          "max":"bohu",
          "version":"V1"
        }
        
    - file1.age.minmax
       {
          "col":"age",
          "min":18,
          "max":24,
          "version":"V1"
        }
```

For the expression `WHERE name = 'bohu' AND age < 24`, we should to check if the data hits this file by:
```rust
    pub fn apply_index(idx_map: HashMap<String, MinMaxIndex>, expr: &Expression) -> Result<bool>
```

If the result is `false` this file is pruned, otherwise we need it.

## Sparse Index

Partition index is the third level to check.

This determines which pages of the file need to be searched.
