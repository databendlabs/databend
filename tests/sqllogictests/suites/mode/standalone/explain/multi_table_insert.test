statement ok
CREATE OR REPLACE TABLE orders_placed (order_id INT, customer_id INT, order_amount FLOAT, order_date DATE);

statement ok
CREATE OR REPLACE TABLE processing_updates (order_id INT, update_note VARCHAR);

statement ok
INSERT INTO orders_placed (order_id, customer_id, order_amount, order_date)
VALUES    (101, 1, 250.00, '2023-01-01'),
          (102, 2, 450.00, '2023-01-02'),
          (103, 1, 1250.00, '2023-01-03'),
          (104, 3, 750.00, '2023-01-04'),
          (105, 2, 50.00, '2023-01-05');

query T
EXPLAIN INSERT FIRST
WHEN order_amount > 1000 THEN INTO processing_updates VALUES (order_id, 'PriorityHandling')
WHEN order_amount > 500 THEN INTO processing_updates VALUES (order_id, 'ExpressHandling')
WHEN order_amount > 100 THEN INTO processing_updates VALUES (order_id, 'StandardHandling')
ELSE INTO processing_updates VALUES (order_id, 'ReviewNeeded')
SELECT    order_id,
          order_amount
FROM      orders_placed;
----
Commit
└── WriteData
    └── EvalScalar
        ├── branch 0: orders_placed.order_id (#0), 'PriorityHandling'
        ├── branch 1: orders_placed.order_id (#0), 'ExpressHandling'
        ├── branch 2: orders_placed.order_id (#0), 'StandardHandling'
        ├── branch 3: orders_placed.order_id (#0), 'ReviewNeeded'
        └── Filter
            ├── branch 0: is_true(orders_placed.order_amount (#2) > CAST(1000 AS Float32 NULL))
            ├── branch 1: is_true((NOT orders_placed.order_amount (#2) > CAST(1000 AS Float32 NULL) AND orders_placed.order_amount (#2) > CAST(500 AS Float32 NULL)))
            ├── branch 2: is_true(((NOT orders_placed.order_amount (#2) > CAST(1000 AS Float32 NULL) AND NOT orders_placed.order_amount (#2) > CAST(500 AS Float32 NULL)) AND orders_placed.order_amount (#2) > CAST(100 AS Float32 NULL)))
            ├── branch 3: is_true(((NOT orders_placed.order_amount (#2) > CAST(1000 AS Float32 NULL) AND NOT orders_placed.order_amount (#2) > CAST(500 AS Float32 NULL)) AND NOT orders_placed.order_amount (#2) > CAST(100 AS Float32 NULL)))
            └── Duplicate
                ├── Duplicate data to 4 branch
                └── TableScan
                    ├── table: default.default.orders_placed
                    ├── output columns: [order_id (#0), order_amount (#2)]
                    ├── read rows: 5
                    ├── read size: < 1 KiB
                    ├── partitions total: 1
                    ├── partitions scanned: 1
                    ├── pruning stats: [segments: <range pruning: 1 to 1>, blocks: <range pruning: 1 to 1>]
                    ├── push downs: [filters: [], limit: NONE]
                    └── estimated rows: 5.00
