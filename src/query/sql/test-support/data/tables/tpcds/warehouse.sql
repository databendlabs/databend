CREATE OR REPLACE TABLE warehouse
(
    w_warehouse_sk            integer                               ,
    w_warehouse_id            char(16)                              ,
    w_warehouse_name          varchar(20)                       null,
    w_warehouse_sq_ft         integer                           null,
    w_street_number           char(10)                          null,
    w_street_name             varchar(60)                       null,
    w_street_type             char(15)                          null,
    w_suite_number            char(10)                          null,
    w_city                    varchar(60)                       null,
    w_county                  varchar(30)                       null,
    w_state                   char(2)                           null,
    w_zip                     char(10)                          null,
    w_country                 varchar(20)                       null,
    w_gmt_offset              decimal(5,2)                      null       
);
