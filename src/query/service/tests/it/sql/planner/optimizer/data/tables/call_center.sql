CREATE OR REPLACE TABLE call_center
(
    cc_call_center_sk         integer                               ,
    cc_call_center_id         char(16)                              ,
    cc_rec_start_date         date                              null,
    cc_rec_end_date           date                              null,
    cc_closed_date_sk         integer                           null,
    cc_open_date_sk           integer                           null,
    cc_name                   varchar(50)                       null,
    cc_class                  varchar(50)                       null,
    cc_employees              integer                           null,
    cc_sq_ft                  integer                           null,
    cc_hours                  char(20)                          null,
    cc_manager                varchar(40)                       null,
    cc_mkt_id                 integer                           null,
    cc_mkt_class              char(50)                          null,
    cc_mkt_desc               varchar(100)                      null,
    cc_market_manager         varchar(40)                       null,
    cc_division               integer                           null,
    cc_division_name          varchar(50)                       null,
    cc_company                integer                           null,
    cc_company_name           char(50)                          null,
    cc_street_number          char(10)                          null,
    cc_street_name            varchar(60)                       null,
    cc_street_type            char(15)                          null,
    cc_suite_number           char(10)                          null,
    cc_city                   varchar(60)                       null,
    cc_county                 varchar(30)                       null,
    cc_state                  char(2)                           null,
    cc_zip                    char(10)                          null,
    cc_country                varchar(20)                       null,
    cc_gmt_offset             decimal(5,2)                      null,
    cc_tax_percentage         decimal(5,2)                      null   
);
