CREATE OR REPLACE TABLE customer_demographics
(
    cd_demo_sk                integer                               ,
    cd_gender                 char(1)                           null,
    cd_marital_status         char(1)                           null,
    cd_education_status       char(20)                          null,
    cd_purchase_estimate      integer                           null,
    cd_credit_rating          char(10)                          null,
    cd_dep_count              integer                           null,
    cd_dep_employed_count     integer                           null,
    cd_dep_college_count      integer                           null
);
