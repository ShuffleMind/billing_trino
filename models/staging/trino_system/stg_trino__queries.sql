{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'cluster_id']
  )
}}

-- Camada de tradução: isola o resto do projeto de mudanças de schema entre
-- versões do Trino no system.runtime.queries. Confirme os nomes de coluna
-- rodando `DESCRIBE system.runtime.queries;` na sua versão (462) antes de
-- rodar em produção -- alguns nomes variam entre releases.

select
    cast('{{ var("cluster_id") }}' as varchar) as cluster_id,
    query_id,
    state,
    "user"            as user_name,
    source,
    catalog,
    "schema"          as schema_name,
    query_type,
    resource_group_id,
    query,
    created,
    started,
    last_heartbeat,
    "end"             as ended_at,
    queued_time_ms,
    analysis_time_ms,
    planning_time_ms,
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
