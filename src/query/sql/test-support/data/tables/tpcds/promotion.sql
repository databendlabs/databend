CREATE OR REPLACE TABLE promotion
(
    p_promo_sk                integer                               ,
    p_promo_id                char(16)                              ,
    p_start_date_sk           integer                           null,
    p_end_date_sk             integer                           null,
    p_item_sk                 integer                           null,
    p_cost                    decimal(15,2)                     null,
    p_response_target         integer                           null,
    p_promo_name              char(50)                          null,
    p_channel_dmail           char(1)                           null,
    p_channel_email           char(1)                           null,
    p_channel_catalog         char(1)                           null,
    p_channel_tv              char(1)                           null,
    p_channel_radio           char(1)                           null,
    p_channel_press           char(1)                           null,
    p_channel_event           char(1)                           null,
    p_channel_demo            char(1)                           null,
    p_channel_details         varchar(100)                      null,
    p_purpose                 char(15)                          null,
    p_discount_active         char(1)                           null     
);
