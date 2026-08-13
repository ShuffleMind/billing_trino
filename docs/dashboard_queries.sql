-- Queries executadas ao vivo pelo dashboard Trino FinOps a cada carregamento
-- da página (endpoint GET /api/finops em scripts/finops_server.py -- o
-- servidor em si não está neste repo, ver README para onde rodá-lo).
--
-- Todas rodam contra o catalog/schema apontado por TRINO_CATALOG/TRINO_SCHEMA
-- (default: iceberg.billing), sobre os marts gerados por este pipeline dbt:
-- fct_trino_query_cost, fct_trino_workload e fct_trino_cluster_growth_daily.
-- Nenhuma tem cache -- refletem o estado do cluster no momento do request.


-- ============================================================
-- 1) Série diária (KPIs "custo", "custo por TB" e "GB processados",
--    com as respectivas variações day-over-day)
-- Painel: cards de KPI no topo do dashboard
-- ============================================================
select
    query_date,
    total_cost_usd,
    total_vcpu_hours,
    total_input_bytes,
    cost_usd_per_tb
from fct_trino_cluster_growth_daily
order by query_date;


-- ============================================================
-- 2) Totais acumulados (queries, falhas, vCPU-horas, custo)
-- Painel: cards de KPI da aba "Consumo" / "Confiabilidade"
-- ============================================================
select
    count(*) as queries,
    sum(case when state = 'FAILED' then 1 else 0 end) as failed,
    sum(vcpu_hours) as vcpu_hours,
    sum(estimated_cost_usd) as cost_usd
from fct_trino_query_cost;


-- ============================================================
-- 2b) Total de bytes lidos (soma o "GB processados" dos totais acima)
-- ============================================================
select sum(input_bytes) as bytes
from fct_trino_workload;


-- ============================================================
-- 3) Custo por fonte (source do cliente Trino: trino-cli, dbt-trino, ...)
-- Painel: donut "Custo por fonte"
-- ============================================================
select
    source,
    count(*) as queries,
    sum(estimated_cost_usd) as cost
from fct_trino_query_cost
group by source
order by cost desc;


-- ============================================================
-- 4) Bytes processados por dataset (heurística por texto da query)
-- Painel: "Bytes processados por dataset"
-- ============================================================
select
    case
        when strpos(lower(query), 'nyc_taxi') > 0 then 'NYC Taxi (Parquet real)'
        when strpos(lower(query), 'orders') > 0 then 'orders (Iceberg)'
        else 'outros'
    end as dataset,
    sum(input_bytes) as bytes
from fct_trino_workload
group by 1
order by bytes desc;


-- ============================================================
-- 5) Queries com falha, agrupadas por error_code
-- Painel: tabela da aba "Confiabilidade"
-- ============================================================
select
    error_code,
    count(*) as n
from fct_trino_workload
where state = 'FAILED' and error_code is not null
group by error_code
order by n desc;


-- ============================================================
-- 6) Top N queries por custo estimado (N = TOP_QUERIES_LIMIT, default 12)
-- Painel: tabela "Queries recentes por custo" / "por vCPU-horas"
-- ============================================================
select
    query_id,
    query,
    source,
    state,
    vcpu_hours,
    estimated_cost_usd,
    duration_hours
from fct_trino_query_cost
order by estimated_cost_usd desc
limit 12;
