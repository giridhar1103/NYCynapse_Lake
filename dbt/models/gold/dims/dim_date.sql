{{ config(materialized='table') }}
with days as (
    select cast(d as date) as date
    from generate_series(date '2024-01-01', date '2027-12-31', interval 1 day) t(d)
)
select
    days.date,
    cast(year(days.date) as smallint)                as year,
    cast(quarter(days.date) as tinyint)              as quarter,
    cast(month(days.date) as tinyint)                as month,
    monthname(days.date)                             as month_name,
    cast(date_trunc('month', days.date) as date)     as month_start,
    cast(day(days.date) as tinyint)                  as day_of_month,
    cast(isodow(days.date) as tinyint)               as day_of_week,
    dayname(days.date)                               as day_name,
    cast(date_trunc('week', days.date) as date)      as week_start,
    isodow(days.date) in (6, 7)                      as is_weekend,
    h.holiday_date is not null                       as is_holiday,
    h.holiday_name,
    isodow(days.date) between 1 and 5 and h.holiday_date is null as is_workday
from days
left join {{ ref('us_federal_holidays') }} h on h.holiday_date = days.date
