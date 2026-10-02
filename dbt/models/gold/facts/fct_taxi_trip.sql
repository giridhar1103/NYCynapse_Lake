{% set payment %}
    case payment_type when 0 then 'flex fare' when 1 then 'credit card' when 2 then 'cash'
        when 3 then 'no charge' when 4 then 'dispute' when 5 then 'unknown'
        when 6 then 'voided' end
{% endset %}
{% set rate %}
    case rate_code when 1 then 'standard' when 2 then 'JFK flat fare' when 3 then 'Newark'
        when 4 then 'Nassau or Westchester' when 5 then 'negotiated' when 6 then 'group ride'
        else 'unknown' end
{% endset %}
{% for service in ['yellow', 'green'] %}
select
    '{{ service }}'                                 as taxi_type,
    file_month,
    {{ ny_parts('pickup_at', 'pickup') }},
    pickup_at,
    dropoff_at,
    pickup_zone_id,
    dropoff_zone_id,
    passenger_count,
    trip_miles,
    round(epoch(dropoff_at - pickup_at) / 60.0, 2)  as trip_minutes,
    {{ payment }}                                   as payment_type,
    {{ rate }}                                      as rate_type,
    {% if service == 'green' %}trip_type = 1{% else %}null{% endif %} as is_street_hail,
    fare_usd,
    extra_usd,
    mta_tax_usd,
    tip_usd,
    tolls_usd,
    improvement_surcharge_usd,
    congestion_surcharge_usd,
    {% if service == 'yellow' %}airport_fee_usd{% else %}null{% endif %} as airport_fee_usd,
    cbd_congestion_fee_usd,
    total_usd,
    dropoff_at > pickup_at and trip_miles >= 0 and total_usd >= 0 and trip_miles < 200
                                                    as is_plausible
from {{ source('silver', 'tlc_' ~ service ~ '_trips') }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
