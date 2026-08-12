{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id'],
    file_format='parquet',
    partitioned_by=['query_date']
  )
}}

-- Aproximação de concorrência real: para cada query, soma a taxa média
-- de vCPU (avg_vcpu_rate) de todas as queries do MESMO cluster cujo
-- intervalo [started, ended_at) se sobrepõe ao dela -- inclui a própria
-- query. O self-join é restrito à(s) partição(ões) de query_date sendo
-- processada(s): int_trino_query_vcpu_rate é particionada por essa
-- mesma coluna, então filtrar os dois lados do join por query_date
-- permite poda de partição em vez de scan O(n²) sobre todo o histórico.
--
-- Trade-off: queries que atravessam a virada do dia não são comparadas
-- com queries do dia anterior/seguinte mesmo que os intervalos se
-- sobreponham de fato -- assumimos que esse caso de borda é raro o
-- suficiente para não justificar o custo de escanear partições vizinhas.

with target_partitions as (

    select distinct cluster_id, query_date
    from {{ ref('int_trino_query_vcpu_rate') }}
    {% if is_incremental() %}
    where query_date >= (select coalesce(max(query_date), date '1970-01-01') from {{ this }})
    {% endif %}

)

select
    a.query_id,
    a.cluster_id,
    a.query_date,
    sum(b.avg_vcpu_rate) as concurrent_cluster_vcpu_usage
from {{ ref('int_trino_query_vcpu_rate') }} a
join target_partitions t
    on t.cluster_id = a.cluster_id
    and t.query_date = a.query_date
join {{ ref('int_trino_query_vcpu_rate') }} b
    on b.cluster_id = a.cluster_id
    and b.query_date = a.query_date
    and b.started < a.ended_at
    and b.ended_at > a.started
group by a.query_id, a.cluster_id, a.query_date
