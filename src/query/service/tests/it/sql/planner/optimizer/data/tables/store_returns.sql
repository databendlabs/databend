CREATE OR REPLACE TABLE store_returns
(
    sr_returned_date_sk       integer                           null,
    sr_return_time_sk         integer                           null,
    sr_item_sk                integer                               ,
    sr_customer_sk            integer                           null,
    sr_cdemo_sk               integer                           null,
    sr_hdemo_sk               integer                           null,
    sr_addr_sk                integer                           null,
    sr_store_sk               integer                           null,
    sr_reason_sk              integer                           null,
    sr_ticket_number          integer                               ,
    sr_return_quantity        integer                           null,
    sr_return_amt             decimal(7,2)                      null,
    sr_return_tax             decimal(7,2)                      null,
    sr_return_amt_inc_tax     decimal(7,2)                      null,
    sr_fee                    decimal(7,2)                      null,
    sr_return_ship_cost       decimal(7,2)                      null,
    sr_refunded_cash          decimal(7,2)                      null,
    sr_reversed_charge        decimal(7,2)                      null,
    sr_store_credit           decimal(7,2)                      null,
    sr_net_loss               decimal(7,2)                      null       
);
