{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id']
  )
}}

with queries as (

    select * from {{ ref('stg_trino__queries') }}
    {% if is_incremental() %}
    -- mesmo lookback de stg_trino__queries.sql -- ver dbt_project.yml
    -- (var query_min_expire_age_minutes) para o porquê
    where ended_at > (
        select coalesce(max(ended_at), timestamp '1970-01-01') from {{ this }}
    ) - interval '{{ var("query_min_expire_age_minutes") }}' minute
    {% endif %}

),

cpu_usage as (

    select * from {{ ref('int_trino_query_cpu_usage') }}

),

rate as (

    select * from {{ ref('int_cluster_vcpu_rate') }}

),

concurrent_usage as (

    select * from {{ ref('int_trino_query_concurrent_vcpu_usage') }}

)

select
    q.cluster_id,
    q.query_id,
    q.user_name,
    q.source,
    q.catalog,
    q.schema_name,
    q.query_type,
    q.state,
    q.created,
    q.started,
    q.ended_at,
    -- dia de execução com base em `started`; queries que atravessam a
    -- meia-noite ficam atribuídas ao dia em que começaram, não terminaram
    date(q.started)                                     as query_date,
    date_diff('millisecond', q.started, q.ended_at)
        / 1000.0 / 3600.0                                as duration_hours,
    q.query,
    coalesce(c.task_count, 0)                          as task_count,
    coalesce(c.vcpu_hours, 0)                          as vcpu_hours,
    r.vcpu_hour_rate_usd,
    r.cluster_total_vcpus,
    coalesce(c.vcpu_hours, 0) * r.vcpu_hour_rate_usd   as estimated_cost_usd,
    -- % da capacidade do cluster: taxa média de vCPU da própria query
    -- (vcpu_hours / duração) sobre o total de vCPUs do cluster
    case
        when date_diff('millisecond', q.started, q.ended_at) > 0
            then (coalesce(c.vcpu_hours, 0)
                    / (date_diff('millisecond', q.started, q.ended_at) / 1000.0 / 3600.0))
                 / nullif(r.cluster_total_vcpus, 0) * 100
    end                                                  as pct_cluster_capacity_avg,
    -- % da capacidade do cluster: soma das taxas médias de todas as
    -- queries sobrepostas no tempo (ver int_trino_query_concurrent_vcpu_usage)
    coalesce(cu.concurrent_cluster_vcpu_usage, 0)
        / nullif(r.cluster_total_vcpus, 0) * 100          as pct_cluster_capacity_concurrent
from queries q
left join cpu_usage c on c.query_id = q.query_id and c.cluster_id = q.cluster_id
left join concurrent_usage cu on cu.query_id = q.query_id and cu.cluster_id = q.cluster_id
join rate r on r.cluster_id = q.cluster_id
