CREATE OR REPLACE TABLE household_demographics
(
    hd_demo_sk                integer                               ,
    hd_income_band_sk         integer                           null,
    hd_buy_potential          char(15)                          null,
    hd_dep_count              integer                           null,
    hd_vehicle_count          integer                           null    
);
