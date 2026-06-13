CREATE OR REPLACE TABLE inventory
(
    inv_date_sk               integer                               ,
    inv_item_sk               integer                               ,
    inv_warehouse_sk          integer                               ,
    inv_quantity_on_hand      integer                           null       
);
