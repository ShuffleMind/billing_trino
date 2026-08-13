{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id']
  )
}}

-- Fato por query com as métricas de workload (tendência, uso por
-- usuário/erro, relações elapsed/scheduled vs input) equivalentes ao que
-- o Presto/Trino Workload Analyzer (github.com/varadaio/presto-workload-analyzer)
-- calcula a partir da QueryInfo completa (API /v1/query/{id}) -- aqui
-- tudo vem só de system.runtime.queries/tasks, sem precisar da API REST.
--
-- NÃO cobre o que exige o plano de execução por operador (que
-- system.runtime.* não expõe): breakdown por tipo de operador, bytes
-- lidos por tabela/scan, seletividade de filtro, ou lado/tipo de join.
--
-- Grão: 1 linha por (query_id, cluster_id), igual a fct_trino_query_cost.

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

io_usage as (

    select * from {{ ref('int_trino_query_io_usage') }}

)

select
    q.cluster_id,
    q.query_id,
    q.user_name,
    q.source,
    q.state,
    q.resource_group_id,
    q.error_type,
    q.error_code,
    q.created,
    q.started,
    q.ended_at,
    -- dia de execução com base em `started`; mesmo critério de
    -- fct_trino_query_cost.sql
    date(q.started)                                     as query_date,
    -- cast p/ double antes de dividir -- ver comentário em
    -- fct_trino_query_cost.sql sobre truncamento decimal em Trino
    cast(date_diff('millisecond', q.created, q.ended_at) as double)
        / 1000.0                                         as elapsed_seconds,
    cast(q.queued_time_ms as double) / 1000.0            as queued_seconds,
    q.query,
    coalesce(c.vcpu_hours, 0)                           as vcpu_hours,
    coalesce(io.scheduled_cluster_hours, 0)             as scheduled_cluster_hours,
    coalesce(io.input_bytes, 0)                         as input_bytes,
    coalesce(io.input_rows, 0)                          as input_rows,
    coalesce(io.output_bytes, 0)                        as output_bytes,
    coalesce(io.output_rows, 0)                         as output_rows
from queries q
left join cpu_usage c on c.query_id = q.query_id and c.cluster_id = q.cluster_id
left join io_usage io on io.query_id = q.query_id and io.cluster_id = q.cluster_id
