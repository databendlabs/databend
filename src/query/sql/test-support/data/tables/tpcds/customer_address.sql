CREATE OR REPLACE TABLE customer_address
(
    ca_address_sk             integer                               ,
    ca_address_id             char(16)                              ,
    ca_street_number          char(10)                          null,
    ca_street_name            varchar(60)                       null,
    ca_street_type            char(15)                          null,
    ca_suite_number           char(10)                          null,
    ca_city                   varchar(60)                       null,
    ca_county                 varchar(30)                       null,
    ca_state                  char(2)                           null,
    ca_zip                    char(10)                          null,
    ca_country                varchar(20)                       null,
    ca_gmt_offset             decimal(5,2)                      null,
    ca_location_type          char(20)                          null     
);
