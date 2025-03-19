CREATE OR REPLACE TABLE time_dim
(
    t_time_sk                 integer                               ,
    t_time_id                 char(16)                              ,
    t_time                    integer                           null,
    t_hour                    integer                           null,
    t_minute                  integer                           null,
    t_second                  integer                           null,
    t_am_pm                   char(2)                           null,
    t_shift                   char(20)                          null,
    t_sub_shift               char(20)                          null,
    t_meal_time               char(20)                          null
);
