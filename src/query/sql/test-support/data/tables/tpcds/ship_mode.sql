CREATE OR REPLACE TABLE ship_mode
(
    sm_ship_mode_sk           integer                               ,
    sm_ship_mode_id           char(16)                              ,
    sm_type                   char(30)                          null,
    sm_code                   char(10)                          null,
    sm_carrier                char(20)                          null,
    sm_contract               char(20)                          null 
);
