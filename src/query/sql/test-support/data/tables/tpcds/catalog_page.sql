CREATE OR REPLACE TABLE catalog_page
(
    cp_catalog_page_sk        integer                               ,
    cp_catalog_page_id        char(16)                              ,
    cp_start_date_sk          integer                           null,
    cp_end_date_sk            integer                           null,
    cp_department             varchar(50)                       null,
    cp_catalog_number         integer                           null,
    cp_catalog_page_number    integer                           null,
    cp_description            varchar(100)                      null,
    cp_type                   varchar(100)                      null      
);
