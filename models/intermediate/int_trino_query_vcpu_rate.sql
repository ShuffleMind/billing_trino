{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id'],
    file_format='iceberg',
    partitioned_by=['query_date']
  )
}}

-- Taxa média de consumo de vCPU por query (vcpu_hours / duração).
-- Materializada como tabela Iceberg particionada por query_date para que
-- int_trino_query_concurrent_vcpu_usage restrinja o self-join à partição
-- do dia sendo processado (poda de partição) em vez de escanear o
-- histórico inteiro. Assume taxa constante ao longo da execução -- não
-- captura picos/vales reais de CPU dentro da query.

select
    q.cluster_id,
    q.query_id,
    q.started,
    q.ended_at,
    date(q.started)                                                     as query_date,
    date_diff('millisecond', q.started, q.ended_at) / 1000.0 / 3600.0    as duration_hours,
    c.vcpu_hours,
    c.vcpu_hours
        / nullif(date_diff('millisecond', q.started, q.ended_at) / 1000.0 / 3600.0, 0)
        as avg_vcpu_rate
from {{ ref('stg_trino__queries') }} q
join {{ ref('int_trino_query_cpu_usage') }} c
    on c.query_id = q.query_id and c.cluster_id = q.cluster_id

{% if is_incremental() %}
-- mesmo lookback de stg_trino__queries.sql: stg_trino__queries pode
-- re-mergear (via merge, não append) registros dentro da janela de
-- retenção que ficaram visíveis fora de ordem; sem esse mesmo lookback
-- aqui, essa correção não se propagaria para esta tabela.
where q.ended_at > (
    select coalesce(max(ended_at), timestamp '1970-01-01') from {{ this }}
) - interval '{{ var("query_min_expire_age_minutes") }}' minute
{% endif %}
