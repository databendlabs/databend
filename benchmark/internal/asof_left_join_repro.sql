-- Reproduce a slow ASOF LEFT JOIN pattern similar to:
--   player(user_id, created_at) ASOF LEFT JOIN deposit(user_id, updated_at < created_at)
--
-- Run with:
--   bendsql -f benchmark/internal/asof_left_join_repro.sql
--
-- Notes:
-- 1. This script uses synthetic data, but keeps the same logical shape as the
--    production query: equality on user_id + inequality on event time.
-- 2. The final two benchmark queries are wrapped in aggregations so we measure
--    execution cost instead of client-side result rendering cost.
-- 3. If the gap is not obvious enough on your machine, increase the row counts
--    in the two numbers(...) inserts.

drop database if exists bench_asof_join;
create database bench_asof_join;
use bench_asof_join;

drop table if exists fact_player_repro;
drop table if exists fact_deposit_repro;

create table fact_player_repro (
    room_id bigint,
    user_id bigint,
    created_at timestamp
);

create table fact_deposit_repro (
    id bigint,
    user_id bigint,
    updated_at timestamp,
    created_at timestamp,
    amount decimal(18, 2),
    transaction_status string,
    balance decimal(18, 2)
);

-- Data shape:
-- - 20,000 users
-- - 96 deposits per user  => 1,920,000 deposit rows
-- - 24 game events/user   =>   480,000 player rows
--
-- Deposits are denser than game events so each player row has multiple valid
-- historical matches on the same user_id.

insert into fact_deposit_repro
select
    number as id,
    number % 20000 as user_id,
    add_seconds(
        '2026-03-20 00:00:00'::timestamp,
        intdiv(number, 20000) * 900 + (number % 7)
    ) as updated_at,
    add_seconds(
        '2026-03-20 00:00:00'::timestamp,
        intdiv(number, 20000) * 900 + (number % 7) - 30
    ) as created_at,
    ((number % 10000) * 0.01 + 1)::decimal(18, 2) as amount,
    if(number % 9 = 0, 'failed', 'success') as transaction_status,
    ((number % 200000) * 0.01 + 100)::decimal(18, 2) as balance
from numbers(1920000);

insert into fact_player_repro
select
    number as room_id,
    number % 20000 as user_id,
    add_seconds(
        '2026-03-20 00:00:00'::timestamp,
        7200 + intdiv(number, 20000) * 1800 + (number % 13)
    ) as created_at
from numbers(480000);

-- Sanity check.
select 'deposit_rows' as metric, count() as value from fact_deposit_repro
union all
select 'player_rows' as metric, count() as value from fact_player_repro
order by metric;

-- Optional warm-up.
select count() from fact_player_repro;
select count() from fact_deposit_repro;

-- Original ASOF LEFT JOIN shape.
with deposit_filtered as (
    select *
    from fact_deposit_repro
    where updated_at >= '2026-03-20'
)
select
    count() as row_cnt,
    sum(if(deposit_id is null, 0, 1)) as matched_rows,
    sum(coalesce(amount, 0)) as matched_amount
from (
    select
        p.room_id,
        p.user_id,
        p.created_at as game_created_at,
        d.id as deposit_id,
        d.created_at as deposit_created_at,
        d.amount,
        d.transaction_status,
        d.balance
    from fact_player_repro p
    asof left join deposit_filtered d
        on p.user_id = d.user_id
       and d.updated_at < p.created_at
    where p.created_at >= '2026-03-20'
) t;

-- ROW_NUMBER rewrite for the same logical requirement.
with deposit_filtered as (
    select *
    from fact_deposit_repro
    where updated_at >= '2026-03-20'
)
select
    count() as row_cnt,
    sum(if(deposit_id is null, 0, 1)) as matched_rows,
    sum(coalesce(amount, 0)) as matched_amount
from (
    select
        p.room_id,
        p.user_id,
        p.created_at as game_created_at,
        d.id as deposit_id,
        d.created_at as deposit_created_at,
        d.amount,
        d.transaction_status,
        d.balance
    from fact_player_repro p
    left join deposit_filtered d
        on p.user_id = d.user_id
       and d.updated_at < p.created_at
    where p.created_at >= '2026-03-20'
    qualify row_number() over (
        partition by p.room_id, p.user_id, p.created_at
        order by d.updated_at desc nulls last, d.id desc nulls last
    ) = 1
) t;

-- Optional: inspect plans if you want to compare operator choices.
explain
with deposit_filtered as (
    select *
    from fact_deposit_repro
    where updated_at >= '2026-03-20'
)
select
    p.room_id,
    p.user_id,
    p.created_at as game_created_at,
    d.id as deposit_id,
    d.created_at as deposit_created_at,
    d.amount,
    d.transaction_status,
    d.balance
from fact_player_repro p
asof left join deposit_filtered d
    on p.user_id = d.user_id
   and d.updated_at < p.created_at
where p.created_at >= '2026-03-20';

explain
with deposit_filtered as (
    select *
    from fact_deposit_repro
    where updated_at >= '2026-03-20'
)
select
    p.room_id,
    p.user_id,
    p.created_at as game_created_at,
    d.id as deposit_id,
    d.created_at as deposit_created_at,
    d.amount,
    d.transaction_status,
    d.balance
from fact_player_repro p
left join deposit_filtered d
    on p.user_id = d.user_id
   and d.updated_at < p.created_at
where p.created_at >= '2026-03-20'
qualify row_number() over (
    partition by p.room_id, p.user_id, p.created_at
    order by d.updated_at desc nulls last, d.id desc nulls last
) = 1;
