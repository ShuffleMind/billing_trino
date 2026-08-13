{{ config(materialized='view') }}

-- Visão diária de crescimento de processamento por cluster: volume de
-- queries, vCPU-horas, custo estimado e bytes lidos, com variação
-- percentual em relação ao dia anterior (day-over-day).
--
-- Materializada como view porque a variação (LAG) depende de todo o
-- histórico do cluster -- incompatível com o merge incremental dos fatos
-- de origem, mesmo raciocínio de fct_trino_query_daily_rank.
--
-- Limitação: LAG() compara com a linha anterior do resultado, não com o
-- dia de calendário anterior -- se o cluster ficar um dia inteiro sem
-- queries, o "dia anterior" da comparação pula esse buraco e vira o
-- último dia que teve atividade. Para calendário contínuo sem buracos,
-- seria necessário um date spine (generate_series) com LEFT JOIN.

with daily_cost as (

    select
        cluster_id,
        query_date,
        count(*)                                          as total_queries,
        sum(case when state = 'FAILED' then 1 else 0 end) as failed_queries,
        sum(vcpu_hours)                                   as total_vcpu_hours,
        sum(estimated_cost_usd)                           as total_cost_usd
    from {{ ref('fct_trino_query_cost') }}
    group by 1, 2

),

daily_io as (

    select
        cluster_id,
        query_date,
        sum(input_bytes) as total_input_bytes
    from {{ ref('fct_trino_workload') }}
    group by 1, 2

),

daily as (

    select
        c.cluster_id,
        c.query_date,
        c.total_queries,
        c.failed_queries,
        c.total_vcpu_hours,
        c.total_cost_usd,
        coalesce(io.total_input_bytes, 0) as total_input_bytes
    from daily_cost c
    left join daily_io io
        on io.cluster_id = c.cluster_id and io.query_date = c.query_date

),

with_lag as (

    select
        *,
        lag(total_queries) over w     as queries_prev_day,
        lag(total_vcpu_hours) over w  as vcpu_hours_prev_day,
        lag(total_cost_usd) over w    as cost_usd_prev_day,
        lag(total_input_bytes) over w as input_bytes_prev_day
    from daily
    window w as (partition by cluster_id order by query_date)

)

select
    cluster_id,
    query_date,
    total_queries,
    failed_queries,
    total_vcpu_hours,
    total_cost_usd,
    total_input_bytes,
    -- custo por TB processado no dia -- métrica de eficiência, não de
    -- volume. nullif() no denominador: dias sem nenhum byte lido (só
    -- DDL/comandos administrativos) não devem estourar DIVISION_BY_ZERO.
    total_cost_usd
        / nullif(cast(total_input_bytes as double) / 1e12, 0)    as cost_usd_per_tb,
    queries_prev_day,
    vcpu_hours_prev_day,
    cost_usd_prev_day,
    input_bytes_prev_day,
    -- cast p/ double antes de dividir: bigint / bigint produz DECIMAL de
    -- escala insuficiente em Trino e trunca variações pequenas p/ 0 --
    -- mesmo problema documentado em fct_trino_query_cost.sql. nullif() no
    -- denominador em vez de CASE WHEN: o motor vetorizado do Trino avalia
    -- a divisão pra todas as linhas do lote antes de aplicar a condição.
    cast(total_queries - queries_prev_day as double)
        / nullif(cast(queries_prev_day as double), 0) * 100      as queries_dod_pct,
    (total_vcpu_hours - vcpu_hours_prev_day)
        / nullif(vcpu_hours_prev_day, 0) * 100                    as vcpu_hours_dod_pct,
    (total_cost_usd - cost_usd_prev_day)
        / nullif(cost_usd_prev_day, 0) * 100                      as cost_usd_dod_pct,
    cast(total_input_bytes - input_bytes_prev_day as double)
        / nullif(cast(input_bytes_prev_day as double), 0) * 100  as input_bytes_dod_pct
from with_lag
order by cluster_id, query_date
