CREATE OR REPLACE TABLE store
(
    s_store_sk                integer                               ,
    s_store_id                char(16)                              ,
    s_rec_start_date          date                              null,
    s_rec_end_date            date                              null,
    s_closed_date_sk          integer                           null,
    s_store_name              varchar(50)                       null,
    s_number_employees        integer                           null,
    s_floor_space             integer                           null,
    s_hours                   char(20)                          null,
    s_manager                 varchar(40)                       null,
    s_market_id               integer                           null,
    s_geography_class         varchar(100)                      null,
    s_market_desc             varchar(100)                      null,
    s_market_manager          varchar(40)                       null,
    s_division_id             integer                           null,
    s_division_name           varchar(50)                       null,
    s_company_id              integer                           null,
    s_company_name            varchar(50)                       null,
    s_street_number           varchar(10)                       null,
    s_street_name             varchar(60)                       null,
    s_street_type             char(15)                          null,
    s_suite_number            char(10)                          null,
    s_city                    varchar(60)                       null,
    s_county                  varchar(30)                       null,
    s_state                   char(2)                           null,
    s_zip                     char(10)                          null,
    s_country                 varchar(20)                       null,
    s_gmt_offset              decimal(5,2)                      null,
    s_tax_precentage          decimal(5,2)                      null
);
