{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id'],
    file_format='parquet',
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
    -- cast p/ double antes de dividir: bigint / decimal literal (1000.0)
    -- produz DECIMAL(22,1) em Trino, que trunca p/ 0.0 qualquer duração
    -- abaixo de ~3min (a maioria das queries reais) -- ver nullif() abaixo,
    -- que também depende dessa mesma correção pra não achar "denominador
    -- zero" onde na verdade só faltava precisão.
    cast(date_diff('millisecond', q.started, q.ended_at) as double) / 1000.0 / 3600.0
        as duration_hours,
    c.vcpu_hours,
    -- nullif() no denominador (em vez de CASE WHEN > 0) porque o motor
    -- vetorizado do Trino avalia a expressão da divisão para todas as
    -- linhas do batch antes de aplicar a condição do CASE -- um CASE WHEN
    -- não protege contra DIVISION_BY_ZERO em tempo de execução aqui.
    c.vcpu_hours
        / nullif(cast(date_diff('millisecond', q.started, q.ended_at) as double) / 1000.0 / 3600.0, 0)
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
