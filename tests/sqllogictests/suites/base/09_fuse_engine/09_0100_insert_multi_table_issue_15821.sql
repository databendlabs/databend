
statement ok
create or replace database issue_15821;

statement ok
use issue_15821;

statement ok
CREATE or replace TABLE tim_hortons_transactions (
    transaction_id INT,
    customer_id INT,
    items VARIANT
);


statement ok
INSERT INTO tim_hortons_transactions (transaction_id, customer_id, items)
VALUES
    (101, 1, parse_json('[{"item":"coffee", "price":2.50}, {"item":"donut", "price":1.20}]')),
    (102, 2, parse_json('[{"item":"bagel", "price":1.80}, {"item":"muffin", "price":2.00}]')),
    (103, 3, parse_json('[{"item":"timbit_assortment", "price":5.00}]'));




statement ok
create or replace table target1 ( id int, customer int, purchased_item string, price float, dummy int);

statement ok
create or replace table target2 ( id int, customer int, purchased_item string, price float, dummy int);

query TT
INSERT
first
    WHEN
            (lower(purchased_item) = 'coffee' OR lower(purchased_item) = 'donut') and purchased_item_len > 0
        THEN
            INTO target1
    WHEN
            (lower(purchased_item) = 'bagel' OR lower(purchased_item) = 'muffin') and purchased_item_len > 0
        THEN
            INTO target2
SELECT transaction_id, customer_id, purchased_item, price, purchased_item_len
FROM (
         SELECT
             t.transaction_id,
             t.customer_id,
             f.value:item::STRING AS purchased_item,
                 length(f.value:item::STRING) AS purchased_item_len,
             f.value:price::FLOAT AS price
         FROM
             tim_hortons_transactions t,
             LATERAL FLATTEN(input => t.items) f
     ) data;
----
2 2


query TTTTT
select * from target1 order by purchased_item;
----
101 1 coffee 6.0 3
101 1 donut 5.0 1

query TTTTT
select * from target2 order by purchased_item;
----
102 2 bagel 5.0 2
102 2 muffin 6.0 2
