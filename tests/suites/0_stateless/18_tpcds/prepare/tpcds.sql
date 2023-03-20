create table customer_address
(
    ca_address_sk             integer                               ,
    ca_address_id             char(16)                              ,
    ca_street_number          char(10)                      not null,
    ca_street_name            varchar(60)                   not null,
    ca_street_type            char(15)                      not null,
    ca_suite_number           char(10)                      not null,
    ca_city                   varchar(60)                   not null,
    ca_county                 varchar(30)                   not null,
    ca_state                  char(2)                       not null,
    ca_zip                    char(10)                      not null,
    ca_country                varchar(20)                   not null,
    ca_gmt_offset             decimal(5,2)                  not null,
    ca_location_type          char(20)                      not null     
);

create table customer_demographics
(
    cd_demo_sk                integer                               ,
    cd_gender                 char(1)                       not null,
    cd_marital_status         char(1)                       not null,
    cd_education_status       char(20)                      not null,
    cd_purchase_estimate      integer                       not null,
    cd_credit_rating          char(10)                      not null,
    cd_dep_count              integer                       not null,
    cd_dep_employed_count     integer                       not null,
    cd_dep_college_count      integer                       not null
);

create table date_dim
(
    d_date_sk                 integer                               ,
    d_date_id                 char(16)                              ,
    d_date                    date                          not null,
    d_month_seq               integer                       not null,
    d_week_seq                integer                       not null,
    d_quarter_seq             integer                       not null,
    d_year                    integer                       not null,
    d_dow                     integer                       not null,
    d_moy                     integer                       not null,
    d_dom                     integer                       not null,
    d_qoy                     integer                       not null,
    d_fy_year                 integer                       not null,
    d_fy_quarter_seq          integer                       not null,
    d_fy_week_seq             integer                       not null,
    d_day_name                char(9)                       not null,
    d_quarter_name            char(6)                       not null,
    d_holiday                 char(1)                       not null,
    d_weekend                 char(1)                       not null,
    d_following_holiday       char(1)                       not null,
    d_first_dom               integer                       not null,
    d_last_dom                integer                       not null,
    d_same_day_ly             integer                       not null,
    d_same_day_lq             integer                       not null,
    d_current_day             char(1)                       not null,
    d_current_week            char(1)                       not null,
    d_current_month           char(1)                       not null,
    d_current_quarter         char(1)                       not null,
    d_current_year            char(1)                       not null  
);

create table warehouse
(
    w_warehouse_sk            integer                               ,
    w_warehouse_id            char(16)                              ,
    w_warehouse_name          varchar(20)                   not null,
    w_warehouse_sq_ft         integer                       not null,
    w_street_number           char(10)                      not null,
    w_street_name             varchar(60)                   not null,
    w_street_type             char(15)                      not null,
    w_suite_number            char(10)                      not null,
    w_city                    varchar(60)                   not null,
    w_county                  varchar(30)                   not null,
    w_state                   char(2)                       not null,
    w_zip                     char(10)                      not null,
    w_country                 varchar(20)                   not null,
    w_gmt_offset              decimal(5,2)                  not null       
);

create table ship_mode
(
    sm_ship_mode_sk           integer                               ,
    sm_ship_mode_id           char(16)                              ,
    sm_type                   char(30)                      not null,
    sm_code                   char(10)                      not null,
    sm_carrier                char(20)                      not null,
    sm_contract               char(20)                      not null 
);

create table time_dim
(
    t_time_sk                 integer                               ,
    t_time_id                 char(16)                              ,
    t_time                    integer                       not null,
    t_hour                    integer                       not null,
    t_minute                  integer                       not null,
    t_second                  integer                       not null,
    t_am_pm                   char(2)                       not null,
    t_shift                   char(20)                      not null,
    t_sub_shift               char(20)                      not null,
    t_meal_time               char(20)                      not null
);

create table reason
(
    r_reason_sk               integer                               ,
    r_reason_id               char(16)                              ,
    r_reason_desc             char(100)                     not null    
);

create table income_band
(
    ib_income_band_sk         integer                               ,
    ib_lower_bound            integer                       not null,
    ib_upper_bound            integer                       not null   
);

create table item
(
    i_item_sk                 integer                               ,
    i_item_id                 char(16)                              ,
    i_rec_start_date          date                          not null,
    i_rec_end_date            date                          not null,
    i_item_desc               varchar(200)                  not null,
    i_current_price           decimal(7,2)                  not null,
    i_wholesale_cost          decimal(7,2)                  not null,
    i_brand_id                integer                       not null,
    i_brand                   char(50)                      not null,
    i_class_id                integer                       not null,
    i_class                   char(50)                      not null,
    i_category_id             integer                       not null,
    i_category                char(50)                      not null,
    i_manufact_id             integer                       not null,
    i_manufact                char(50)                      not null,
    i_size                    char(20)                      not null,
    i_formulation             char(20)                      not null,
    i_color                   char(20)                      not null,
    i_units                   char(10)                      not null,
    i_container               char(10)                      not null,
    i_manager_id              integer                       not null,
    i_product_name            char(50)                      not null
);

create table store
(
    s_store_sk                integer                               ,
    s_store_id                char(16)                              ,
    s_rec_start_date          date                          not null,
    s_rec_end_date            date                          not null,
    s_closed_date_sk          integer                       not null,
    s_store_name              varchar(50)                   not null,
    s_number_employees        integer                       not null,
    s_floor_space             integer                       not null,
    s_hours                   char(20)                      not null,
    s_manager                 varchar(40)                   not null,
    s_market_id               integer                       not null,
    s_geography_class         varchar(100)                  not null,
    s_market_desc             varchar(100)                  not null,
    s_market_manager          varchar(40)                   not null,
    s_division_id             integer                       not null,
    s_division_name           varchar(50)                   not null,
    s_company_id              integer                       not null,
    s_company_name            varchar(50)                   not null,
    s_street_number           varchar(10)                   not null,
    s_street_name             varchar(60)                   not null,
    s_street_type             char(15)                      not null,
    s_suite_number            char(10)                      not null,
    s_city                    varchar(60)                   not null,
    s_county                  varchar(30)                   not null,
    s_state                   char(2)                       not null,
    s_zip                     char(10)                      not null,
    s_country                 varchar(20)                   not null,
    s_gmt_offset              decimal(5,2)                  not null,
    s_tax_precentage          decimal(5,2)                  not null
);

create table call_center
(
    cc_call_center_sk         integer                               ,
    cc_call_center_id         char(16)                              ,
    cc_rec_start_date         date                          not null,
    cc_rec_end_date           date                          not null,
    cc_closed_date_sk         integer                       not null,
    cc_open_date_sk           integer                       not null,
    cc_name                   varchar(50)                   not null,
    cc_class                  varchar(50)                   not null,
    cc_employees              integer                       not null,
    cc_sq_ft                  integer                       not null,
    cc_hours                  char(20)                      not null,
    cc_manager                varchar(40)                   not null,
    cc_mkt_id                 integer                       not null,
    cc_mkt_class              char(50)                      not null,
    cc_mkt_desc               varchar(100)                  not null,
    cc_market_manager         varchar(40)                   not null,
    cc_division               integer                       not null,
    cc_division_name          varchar(50)                   not null,
    cc_company                integer                       not null,
    cc_company_name           char(50)                      not null,
    cc_street_number          char(10)                      not null,
    cc_street_name            varchar(60)                   not null,
    cc_street_type            char(15)                      not null,
    cc_suite_number           char(10)                      not null,
    cc_city                   varchar(60)                   not null,
    cc_county                 varchar(30)                   not null,
    cc_state                  char(2)                       not null,
    cc_zip                    char(10)                      not null,
    cc_country                varchar(20)                   not null,
    cc_gmt_offset             decimal(5,2)                  not null,
    cc_tax_percentage         decimal(5,2)                  not null   
);

create table customer
(
    c_customer_sk             integer                               ,
    c_customer_id             char(16)                              ,
    c_current_cdemo_sk        integer                       not null,
    c_current_hdemo_sk        integer                       not null,
    c_current_addr_sk         integer                       not null,
    c_first_shipto_date_sk    integer                       not null,
    c_first_sales_date_sk     integer                       not null,
    c_salutation              char(10)                      not null,
    c_first_name              char(20)                      not null,
    c_last_name               char(30)                      not null,
    c_preferred_cust_flag     char(1)                       not null,
    c_birth_day               integer                       not null,
    c_birth_month             integer                       not null,
    c_birth_year              integer                       not null,
    c_birth_country           varchar(20)                   not null,
    c_login                   char(13)                      not null,
    c_email_address           char(50)                      not null,
    c_last_review_date_sk     integer                       not null   
);

create table web_site
(
    web_site_sk               integer                               ,
    web_site_id               char(16)                              ,
    web_rec_start_date        date                          not null,
    web_rec_end_date          date                          not null,
    web_name                  varchar(50)                   not null,
    web_open_date_sk          integer                       not null,
    web_close_date_sk         integer                       not null,
    web_class                 varchar(50)                   not null,
    web_manager               varchar(40)                   not null,
    web_mkt_id                integer                       not null,
    web_mkt_class             varchar(50)                   not null,
    web_mkt_desc              varchar(100)                  not null,
    web_market_manager        varchar(40)                   not null,
    web_company_id            integer                       not null,
    web_company_name          char(50)                      not null,
    web_street_number         char(10)                      not null,
    web_street_name           varchar(60)                   not null,
    web_street_type           char(15)                      not null,
    web_suite_number          char(10)                      not null,
    web_city                  varchar(60)                   not null,
    web_county                varchar(30)                   not null,
    web_state                 char(2)                       not null,
    web_zip                   char(10)                      not null,
    web_country               varchar(20)                   not null,
    web_gmt_offset            decimal(5,2)                  not null,
    web_tax_percentage        decimal(5,2)                  not null 
);

create table store_returns
(
    sr_returned_date_sk       integer                       not null,
    sr_return_time_sk         integer                       not null,
    sr_item_sk                integer                               ,
    sr_customer_sk            integer                       not null,
    sr_cdemo_sk               integer                       not null,
    sr_hdemo_sk               integer                       not null,
    sr_addr_sk                integer                       not null,
    sr_store_sk               integer                       not null,
    sr_reason_sk              integer                       not null,
    sr_ticket_number          integer                               ,
    sr_return_quantity        integer                       not null,
    sr_return_amt             decimal(7,2)                  not null,
    sr_return_tax             decimal(7,2)                  not null,
    sr_return_amt_inc_tax     decimal(7,2)                  not null,
    sr_fee                    decimal(7,2)                  not null,
    sr_return_ship_cost       decimal(7,2)                  not null,
    sr_refunded_cash          decimal(7,2)                  not null,
    sr_reversed_charge        decimal(7,2)                  not null,
    sr_store_credit           decimal(7,2)                  not null,
    sr_net_loss               decimal(7,2)                  not null       
);

create table household_demographics
(
    hd_demo_sk                integer                               ,
    hd_income_band_sk         integer                       not null,
    hd_buy_potential          char(15)                      not null,
    hd_dep_count              integer                       not null,
    hd_vehicle_count          integer                       not null    
);

create table web_page
(
    wp_web_page_sk            integer                               ,
    wp_web_page_id            char(16)                              ,
    wp_rec_start_date         date                          not null,
    wp_rec_end_date           date                          not null,
    wp_creation_date_sk       integer                       not null,
    wp_access_date_sk         integer                       not null,
    wp_autogen_flag           char(1)                       not null,
    wp_customer_sk            integer                       not null,
    wp_url                    varchar(100)                  not null,
    wp_type                   char(50)                      not null,
    wp_char_count             integer                       not null,
    wp_link_count             integer                       not null,
    wp_image_count            integer                       not null,
    wp_max_ad_count           integer                       not null
);

create table promotion
(
    p_promo_sk                integer                               ,
    p_promo_id                char(16)                              ,
    p_start_date_sk           integer                       not null,
    p_end_date_sk             integer                       not null,
    p_item_sk                 integer                       not null,
    p_cost                    decimal(15,2)                 not null,
    p_response_target         integer                       not null,
    p_promo_name              char(50)                      not null,
    p_channel_dmail           char(1)                       not null,
    p_channel_email           char(1)                       not null,
    p_channel_catalog         char(1)                       not null,
    p_channel_tv              char(1)                       not null,
    p_channel_radio           char(1)                       not null,
    p_channel_press           char(1)                       not null,
    p_channel_event           char(1)                       not null,
    p_channel_demo            char(1)                       not null,
    p_channel_details         varchar(100)                  not null,
    p_purpose                 char(15)                      not null,
    p_discount_active         char(1)                       not null     
);

create table catalog_page
(
    cp_catalog_page_sk        integer                               ,
    cp_catalog_page_id        char(16)                              ,
    cp_start_date_sk          integer                       not null,
    cp_end_date_sk            integer                       not null,
    cp_department             varchar(50)                   not null,
    cp_catalog_number         integer                       not null,
    cp_catalog_page_number    integer                       not null,
    cp_description            varchar(100)                  not null,
    cp_type                   varchar(100)                  not null      
);

create table inventory
(
    inv_date_sk               integer                               ,
    inv_item_sk               integer                               ,
    inv_warehouse_sk          integer                               ,
    inv_quantity_on_hand      integer                       not null       
);

create table catalog_returns
(
    cr_returned_date_sk       integer                       not null,
    cr_returned_time_sk       integer                       not null,
    cr_item_sk                integer                               ,
    cr_refunded_customer_sk   integer                       not null,
    cr_refunded_cdemo_sk      integer                       not null,
    cr_refunded_hdemo_sk      integer                       not null,
    cr_refunded_addr_sk       integer                       not null,
    cr_returning_customer_sk  integer                       not null,
    cr_returning_cdemo_sk     integer                       not null,
    cr_returning_hdemo_sk     integer                       not null,
    cr_returning_addr_sk      integer                       not null,
    cr_call_center_sk         integer                       not null,
    cr_catalog_page_sk        integer                       not null,
    cr_ship_mode_sk           integer                       not null,
    cr_warehouse_sk           integer                       not null,
    cr_reason_sk              integer                       not null,
    cr_order_number           integer                               ,
    cr_return_quantity        integer                       not null,
    cr_return_amount          decimal(7,2)                  not null,
    cr_return_tax             decimal(7,2)                  not null,
    cr_return_amt_inc_tax     decimal(7,2)                  not null,
    cr_fee                    decimal(7,2)                  not null,
    cr_return_ship_cost       decimal(7,2)                  not null,
    cr_refunded_cash          decimal(7,2)                  not null,
    cr_reversed_charge        decimal(7,2)                  not null,
    cr_store_credit           decimal(7,2)                  not null,
    cr_net_loss               decimal(7,2)                  not null       
);

create table web_returns
(
    wr_returned_date_sk       integer                       not null,
    wr_returned_time_sk       integer                       not null,
    wr_item_sk                integer                               ,
    wr_refunded_customer_sk   integer                       not null,
    wr_refunded_cdemo_sk      integer                       not null,
    wr_refunded_hdemo_sk      integer                       not null,
    wr_refunded_addr_sk       integer                       not null,
    wr_returning_customer_sk  integer                       not null,
    wr_returning_cdemo_sk     integer                       not null,
    wr_returning_hdemo_sk     integer                       not null,
    wr_returning_addr_sk      integer                       not null,
    wr_web_page_sk            integer                       not null,
    wr_reason_sk              integer                       not null,
    wr_order_number           integer                               ,
    wr_return_quantity        integer                       not null,
    wr_return_amt             decimal(7,2)                  not null,
    wr_return_tax             decimal(7,2)                  not null,
    wr_return_amt_inc_tax     decimal(7,2)                  not null,
    wr_fee                    decimal(7,2)                  not null,
    wr_return_ship_cost       decimal(7,2)                  not null,
    wr_refunded_cash          decimal(7,2)                  not null,
    wr_reversed_charge        decimal(7,2)                  not null,
    wr_account_credit         decimal(7,2)                  not null,
    wr_net_loss               decimal(7,2)                  not null      
);

create table web_sales
(
    ws_sold_date_sk           integer                       not null,
    ws_sold_time_sk           integer                       not null,
    ws_ship_date_sk           integer                       not null,
    ws_item_sk                integer                               ,
    ws_bill_customer_sk       integer                       not null,
    ws_bill_cdemo_sk          integer                       not null,
    ws_bill_hdemo_sk          integer                       not null,
    ws_bill_addr_sk           integer                       not null,
    ws_ship_customer_sk       integer                       not null,
    ws_ship_cdemo_sk          integer                       not null,
    ws_ship_hdemo_sk          integer                       not null,
    ws_ship_addr_sk           integer                       not null,
    ws_web_page_sk            integer                       not null,
    ws_web_site_sk            integer                       not null,
    ws_ship_mode_sk           integer                       not null,
    ws_warehouse_sk           integer                       not null,
    ws_promo_sk               integer                       not null,
    ws_order_number           integer                               ,
    ws_quantity               integer                       not null,
    ws_wholesale_cost         decimal(7,2)                  not null,
    ws_list_price             decimal(7,2)                  not null,
    ws_sales_price            decimal(7,2)                  not null,
    ws_ext_discount_amt       decimal(7,2)                  not null,
    ws_ext_sales_price        decimal(7,2)                  not null,
    ws_ext_wholesale_cost     decimal(7,2)                  not null,
    ws_ext_list_price         decimal(7,2)                  not null,
    ws_ext_tax                decimal(7,2)                  not null,
    ws_coupon_amt             decimal(7,2)                  not null,
    ws_ext_ship_cost          decimal(7,2)                  not null,
    ws_net_paid               decimal(7,2)                  not null,
    ws_net_paid_inc_tax       decimal(7,2)                  not null,
    ws_net_paid_inc_ship      decimal(7,2)                  not null,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  not null,
    ws_net_profit             decimal(7,2)                  not null       
);

create table catalog_sales
(
    cs_sold_date_sk           integer                       not null,
    cs_sold_time_sk           integer                       not null,
    cs_ship_date_sk           integer                       not null,
    cs_bill_customer_sk       integer                       not null,
    cs_bill_cdemo_sk          integer                       not null,
    cs_bill_hdemo_sk          integer                       not null,
    cs_bill_addr_sk           integer                       not null,
    cs_ship_customer_sk       integer                       not null,
    cs_ship_cdemo_sk          integer                       not null,
    cs_ship_hdemo_sk          integer                       not null,
    cs_ship_addr_sk           integer                       not null,
    cs_call_center_sk         integer                       not null,
    cs_catalog_page_sk        integer                       not null,
    cs_ship_mode_sk           integer                       not null,
    cs_warehouse_sk           integer                       not null,
    cs_item_sk                integer                               ,
    cs_promo_sk               integer                       not null,
    cs_order_number           integer                               ,
    cs_quantity               integer                       not null,
    cs_wholesale_cost         decimal(7,2)                  not null,
    cs_list_price             decimal(7,2)                  not null,
    cs_sales_price            decimal(7,2)                  not null,
    cs_ext_discount_amt       decimal(7,2)                  not null,
    cs_ext_sales_price        decimal(7,2)                  not null,
    cs_ext_wholesale_cost     decimal(7,2)                  not null,
    cs_ext_list_price         decimal(7,2)                  not null,
    cs_ext_tax                decimal(7,2)                  not null,
    cs_coupon_amt             decimal(7,2)                  not null,
    cs_ext_ship_cost          decimal(7,2)                  not null,
    cs_net_paid               decimal(7,2)                  not null,
    cs_net_paid_inc_tax       decimal(7,2)                  not null,
    cs_net_paid_inc_ship      decimal(7,2)                  not null,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  not null,
    cs_net_profit             decimal(7,2)                  not null
);

create table store_sales
(
    ss_sold_date_sk           integer                       not null,
    ss_sold_time_sk           integer                       not null,
    ss_item_sk                integer                               ,
    ss_customer_sk            integer                       not null,
    ss_cdemo_sk               integer                       not null,
    ss_hdemo_sk               integer                       not null,
    ss_addr_sk                integer                       not null,
    ss_store_sk               integer                       not null,
    ss_promo_sk               integer                       not null,
    ss_ticket_number          integer                               ,
    ss_quantity               integer                       not null,
    ss_wholesale_cost         decimal(7,2)                  not null,
    ss_list_price             decimal(7,2)                  not null,
    ss_sales_price            decimal(7,2)                  not null,
    ss_ext_discount_amt       decimal(7,2)                  not null,
    ss_ext_sales_price        decimal(7,2)                  not null,
    ss_ext_wholesale_cost     decimal(7,2)                  not null,
    ss_ext_list_price         decimal(7,2)                  not null,
    ss_ext_tax                decimal(7,2)                  not null,
    ss_coupon_amt             decimal(7,2)                  not null,
    ss_net_paid               decimal(7,2)                  not null,
    ss_net_paid_inc_tax       decimal(7,2)                  not null,
    ss_net_profit             decimal(7,2)                  not null
);

