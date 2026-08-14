{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['query_id', 'task_id', 'cluster_id']
  )
}}

-- Mesma lógica de tradução de schema aplicada a system.runtime.tasks.
-- split_cpu_time_ms = soma do tempo de CPU consumido pelos splits da task.
-- split_scheduled_time_ms = soma do tempo de CPU + espera na fila de
-- agendamento (proxy de utilização de cluster, não só CPU pura).
-- raw_input_bytes/rows e output_bytes/rows = volume de dados por task,
-- usados para métricas de I/O por query (ver int_trino_query_io_usage).
-- Confirmado via `DESCRIBE system.runtime.tasks;` contra um Trino 462
-- real: não existe coluna elapsed_time_ms (removida daqui). Reconfirme ao
-- trocar de versão do Trino antes de rodar em produção.
--
-- cast explícito de created/end p/ timestamp(6) with time zone: mesmo
-- motivo de stg_trino__queries.sql -- Iceberg só aceita timestamp(6) e o
-- widening implícito do timestamp(3) da fonte não sobrevive à forma como
-- o dbt-trino executa o CTAS.

select
    cast('{{ var("cluster_id") }}' as varchar) as cluster_id,
    query_id,
    task_id,
    split_cpu_time_ms,
    split_scheduled_time_ms,
    raw_input_bytes,
    raw_input_rows,
    output_bytes,
    output_rows,
    cast(created as timestamp(6) with time zone) as created,
    cast("end" as timestamp(6) with time zone)   as ended_at
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
