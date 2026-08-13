{{ config(materialized='view') }}

-- Visão mensal de crescimento de processamento por cluster: mesma lógica
-- de fct_trino_cluster_growth_daily, mas agregada por mês (date_trunc),
-- com variação percentual mês a mês (month-over-month). Ver esse outro
-- model para as notas sobre materialização como view e sobre a
-- truncagem decimal ao dividir bigint/bigint em Trino.

with monthly_cost as (

    select
        cluster_id,
        date_trunc('month', query_date) as query_month,
        count(*)                                          as total_queries,
        sum(case when state = 'FAILED' then 1 else 0 end) as failed_queries,
        sum(vcpu_hours)                                   as total_vcpu_hours,
        sum(estimated_cost_usd)                           as total_cost_usd
    from {{ ref('fct_trino_query_cost') }}
    group by 1, 2

),

monthly_io as (

    select
        cluster_id,
        date_trunc('month', query_date) as query_month,
        sum(input_bytes) as total_input_bytes
    from {{ ref('fct_trino_workload') }}
    group by 1, 2

),

monthly as (

    select
        c.cluster_id,
        c.query_month,
        c.total_queries,
        c.failed_queries,
        c.total_vcpu_hours,
        c.total_cost_usd,
        coalesce(io.total_input_bytes, 0) as total_input_bytes
    from monthly_cost c
    left join monthly_io io
        on io.cluster_id = c.cluster_id and io.query_month = c.query_month

),

with_lag as (

    select
        *,
        lag(total_queries) over w     as queries_prev_month,
        lag(total_vcpu_hours) over w  as vcpu_hours_prev_month,
        lag(total_cost_usd) over w    as cost_usd_prev_month,
        lag(total_input_bytes) over w as input_bytes_prev_month
    from monthly
    window w as (partition by cluster_id order by query_month)

)

select
    cluster_id,
    query_month,
    total_queries,
    failed_queries,
    total_vcpu_hours,
    total_cost_usd,
    total_input_bytes,
    -- custo por TB processado no mês -- métrica de eficiência, não de
    -- volume. nullif() no denominador: meses sem nenhum byte lido não
    -- devem estourar DIVISION_BY_ZERO.
    total_cost_usd
        / nullif(cast(total_input_bytes as double) / 1e12, 0)    as cost_usd_per_tb,
    queries_prev_month,
    vcpu_hours_prev_month,
    cost_usd_prev_month,
    input_bytes_prev_month,
    cast(total_queries - queries_prev_month as double)
        / nullif(cast(queries_prev_month as double), 0) * 100      as queries_mom_pct,
    (total_vcpu_hours - vcpu_hours_prev_month)
        / nullif(vcpu_hours_prev_month, 0) * 100                    as vcpu_hours_mom_pct,
    (total_cost_usd - cost_usd_prev_month)
        / nullif(cost_usd_prev_month, 0) * 100                      as cost_usd_mom_pct,
    cast(total_input_bytes - input_bytes_prev_month as double)
        / nullif(cast(input_bytes_prev_month as double), 0) * 100  as input_bytes_mom_pct
from with_lag
order by cluster_id, query_month
