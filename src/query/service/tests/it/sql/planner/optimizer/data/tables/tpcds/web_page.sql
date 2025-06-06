CREATE OR REPLACE TABLE web_page
(
    wp_web_page_sk            integer                               ,
    wp_web_page_id            char(16)                              ,
    wp_rec_start_date         date                              null,
    wp_rec_end_date           date                              null,
    wp_creation_date_sk       integer                           null,
    wp_access_date_sk         integer                           null,
    wp_autogen_flag           char(1)                           null,
    wp_customer_sk            integer                           null,
    wp_url                    varchar(100)                      null,
    wp_type                   char(50)                          null,
    wp_char_count             integer                           null,
    wp_link_count             integer                           null,
    wp_image_count            integer                           null,
    wp_max_ad_count           integer                           null
);
