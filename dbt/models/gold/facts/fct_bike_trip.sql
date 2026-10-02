select
    ride_id,
    file_month,
    {{ ny_parts('started_at', 'start') }},
    started_at,
    ended_at,
    round(epoch(ended_at - started_at) / 60.0, 2)   as duration_minutes,
    bike_type,
    rider_type,
    start_station_id                                as start_station_number,
    start_station_name,
    end_station_id                                  as end_station_number,
    end_station_name,
    start_station_id is not null and start_station_id = end_station_id as is_round_trip,
    start_boro_code,
    start_nta_code,
    start_taxi_zone_id,
    end_boro_code,
    end_nta_code,
    end_taxi_zone_id,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    case when start_latitude is not null and end_latitude is not null
         then round(ST_Distance_Sphere(ST_Point(start_latitude, start_longitude),
                                       ST_Point(end_latitude, end_longitude)) / 1609.344, 3)
    end                                             as straight_line_miles
from {{ source('silver', 'bike_trips') }}
