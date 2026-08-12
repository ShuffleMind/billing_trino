{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'task_id', 'cluster_id']
  )
}}

-- Mesma lógica de tradução de schema aplicada a system.runtime.tasks.
-- split_cpu_time_ms = soma do tempo de CPU consumido pelos splits da task.
-- Confirmado via `DESCRIBE system.runtime.tasks;` contra um Trino 462
-- real: não existe coluna elapsed_time_ms (removida daqui). raw_input_bytes/
-- raw_input_rows continuam presentes na 462. Reconfirme ao trocar de
-- versão do Trino antes de rodar em produção.

select
    cast('{{ var("cluster_id") }}' as varchar) as cluster_id,
    query_id,
    task_id,
    stage_id,
    state,
    split_cpu_time_ms,
    created,
    "end" as ended_at
from {{ source('trino_system', 'tasks') }}
where state = 'FINISHED'

{% if is_incremental() %}
    -- mesmo raciocínio de lookback de stg_trino__queries.sql -- relê a
    -- janela de retenção inteira a cada execução; merge evita duplicar
    -- as linhas já capturadas em execuções anteriores.
    and "end" > (
        select coalesce(max(ended_at), timestamp '1970-01-01') from {{ this }}
    ) - interval '{{ var("query_min_expire_age_minutes") }}' minute
{% endif %}
