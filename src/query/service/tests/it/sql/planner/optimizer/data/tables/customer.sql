CREATE OR REPLACE TABLE customer
(
    c_customer_sk             integer                               ,
    c_customer_id             char(16)                              ,
    c_current_cdemo_sk        integer                           null,
    c_current_hdemo_sk        integer                           null,
    c_current_addr_sk         integer                           null,
    c_first_shipto_date_sk    integer                           null,
    c_first_sales_date_sk     integer                           null,
    c_salutation              char(10)                          null,
    c_first_name              char(20)                          null,
    c_last_name               char(30)                          null,
    c_preferred_cust_flag     char(1)                           null,
    c_birth_day               integer                           null,
    c_birth_month             integer                           null,
    c_birth_year              integer                           null,
    c_birth_country           varchar(20)                       null,
    c_login                   char(13)                          null,
    c_email_address           char(50)                          null,
    c_last_review_date_sk     integer                           null   
);
