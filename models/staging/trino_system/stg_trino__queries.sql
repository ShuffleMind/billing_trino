{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id']
  )
}}

-- Camada de tradução: isola o resto do projeto de mudanças de schema entre
-- versões do Trino no system.runtime.queries. Confirmado via
-- `DESCRIBE system.runtime.queries;` contra um Trino 462 real: a tabela
-- NÃO tem colunas catalog/schema/query_type (só existiam em versões mais
-- antigas ou nunca existiram) -- por isso não são selecionadas aqui.
-- Reconfirme ao trocar de versão do Trino antes de rodar em produção.
--
-- created/started/end vêm como timestamp(3) with time zone em
-- system.runtime.queries, mas Iceberg só aceita timestamp(6).
-- Rodando via `trino` CLI o Trino faz esse widening implicitamente no
-- CREATE TABLE AS SELECT, mas o dbt-trino (via cliente Python) envolve a
-- query numa camada extra que quebra esse comportamento implícito e
-- falha com "Timestamp precision (3) not supported for Iceberg" -- por
-- isso o cast explícito abaixo, em vez de depender de inferência do
-- engine.

select
    cast('{{ var("cluster_id") }}' as varchar) as cluster_id,
    query_id,
    state,
    "user"            as user_name,
    source,
    resource_group_id,
    query,
    cast(created as timestamp(6) with time zone)        as created,
    cast(started as timestamp(6) with time zone)        as started,
    cast("end" as timestamp(6) with time zone)          as ended_at,
    queued_time_ms,
    error_type,
    error_code
from {{ source('trino_system', 'queries') }}
-- só captura queries em estado terminal: enquanto a query roda, a soma de
-- cpu_time das tasks ainda está incompleta
where state in ('FINISHED', 'FAILED')

{% if is_incremental() %}
    -- relê a janela de retenção inteira (query.min-expire-age) a cada
    -- execução, não só o que é estritamente novo desde o último
    -- max(ended_at): uma linha pode ficar visível em
    -- system.runtime.queries um pouco depois do timestamp "end" que ela
    -- carrega. Isso faz execuções reprocessarem registros já capturados
    -- -- por isso a estratégia é merge, não append.
    and "end" > (
        select coalesce(max(ended_at), timestamp '1970-01-01') from {{ this }}
    ) - interval '{{ var("query_min_expire_age_minutes") }}' minute
{% endif %}
