select
    split_part(source, ':', 1)                      as source,
    nullif(split_part(source, ':', 2), '')          as feed,
    gap_start,
    gap_end,
    round(epoch(gap_end - gap_start) / 60.0, 1)     as gap_minutes,
    reason
from {{ source('ops', 'feed_gaps') }}
