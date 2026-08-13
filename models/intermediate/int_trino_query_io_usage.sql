{{ config(materialized='view') }}

-- Agrega volume de dados e tempo agendado (scheduled time) de todas as
-- tasks de cada query. Separado de int_trino_query_cpu_usage porque tem
-- outra responsabilidade (I/O e utilização de cluster, não custo em
-- vCPU) -- alimenta métricas no estilo "workload analyzer" (tendência de
-- input por data/hora, uso por usuário, elapsed vs input) que hoje o
-- pipeline não calcula.
--
-- split_scheduled_time_ms inclui tempo de CPU + espera na fila de
-- agendamento -- é uma medida de utilização de cluster diferente de
-- vcpu_hours (que é só CPU pura).
--
-- output_bytes/rows aqui são a soma de TODAS as tasks (todos os
-- estágios, inclusive intermediários) -- não é o mesmo valor que
-- QueryStats.outputDataSize da API do Trino (que é só o resultado final
-- da query). É um proxy do volume total de dados movimentado, não do
-- tamanho exato do resultado.

select
    cluster_id,
    query_id,
    cast(sum(split_scheduled_time_ms) as double) / 1000.0 / 3600.0 as scheduled_cluster_hours,
    sum(raw_input_bytes)                                            as input_bytes,
    sum(raw_input_rows)                                             as input_rows,
    sum(output_bytes)                                               as output_bytes,
    sum(output_rows)                                                as output_rows
from {{ ref('stg_trino__tasks') }}
group by 1, 2
