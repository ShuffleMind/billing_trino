{{ config(materialized='view') }}

-- Sempre recalculada por completo (view): ranking e % do total diário
-- dependem de todas as queries do dia inteiro, o que não é compatível
-- com o merge incremental de fct_trino_query_cost -- cada rodada
-- incremental só enxerga o lote novo, não o dia todo, então esses
-- totais não podem ser colunas materializadas ali.
--
-- Particionado por (cluster_id, query_date): "dia" e "total diário" são
-- por cluster -- clusters diferentes não competem pela mesma capacidade,
-- então não fazem sentido no mesmo ranking/percentual.

select
    *,
    sum(vcpu_hours) over (partition by cluster_id, query_date)          as daily_total_vcpu_hours,
    sum(estimated_cost_usd) over (partition by cluster_id, query_date)  as daily_total_cost_usd,
    vcpu_hours
        / nullif(sum(vcpu_hours) over (partition by cluster_id, query_date), 0) * 100
        as pct_vcpu_hours_of_daily_total,
    estimated_cost_usd
        / nullif(sum(estimated_cost_usd) over (partition by cluster_id, query_date), 0) * 100
        as pct_cost_of_daily_total,
    row_number() over (partition by cluster_id, query_date order by estimated_cost_usd desc) as daily_cost_rank,
    row_number() over (partition by cluster_id, query_date order by vcpu_hours desc)         as daily_vcpu_rank
from {{ ref('fct_trino_query_cost') }}
