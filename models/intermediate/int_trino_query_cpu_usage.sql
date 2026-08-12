{{ config(materialized='view') }}

-- Agrega o consumo de CPU de todas as tasks de cada query.

select
    cluster_id,
    query_id,
    count(*)                                             as task_count,
    sum(split_cpu_time_ms)                                as total_cpu_time_ms,
    -- cast p/ double antes de dividir: bigint / decimal literal (1000.0)
    -- produz DECIMAL(_,1) em Trino, que trunca p/ 0.0 qualquer valor
    -- abaixo de ~6min de CPU (a maioria das queries reais).
    cast(sum(split_cpu_time_ms) as double) / 1000.0          as total_cpu_time_s,
    cast(sum(split_cpu_time_ms) as double) / 1000.0 / 3600.0 as vcpu_hours
from {{ ref('stg_trino__tasks') }}
group by 1, 2
