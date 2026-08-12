{{ config(materialized='view') }}

-- Agrega o consumo de CPU de todas as tasks de cada query.

select
    cluster_id,
    query_id,
    count(*)                                  as task_count,
    sum(split_cpu_time_ms)                    as total_cpu_time_ms,
    sum(split_cpu_time_ms) / 1000.0           as total_cpu_time_s,
    sum(split_cpu_time_ms) / 1000.0 / 3600.0  as vcpu_hours
from {{ ref('stg_trino__tasks') }}
group by 1, 2
