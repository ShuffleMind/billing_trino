{{ config(materialized='view') }}

-- Taxa $/VCPU-hora derivada do shape do cluster (não de preço de mercado):
-- rate = custo/hora total do cluster ÷ total de vCPUs do cluster.
-- Assume shape homogêneo por node_role. Se o cluster faz autoscaling ou
-- muda de instance_type, versione o seed com faixas de valid_from/valid_to
-- e filtre pelo período vigente em vez de sobrescrever a linha.

select
    cluster_id,
    sum(vcpus * node_count)                                   as cluster_total_vcpus,
    sum(hourly_price_usd * node_count)                        as cluster_hourly_cost_usd,
    sum(hourly_price_usd * node_count)
        / nullif(sum(vcpus * node_count), 0)                  as vcpu_hour_rate_usd
from {{ ref('cluster_shape') }}
group by cluster_id
