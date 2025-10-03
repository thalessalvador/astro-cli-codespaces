{{ config(materialized='table') }}

with base as (
  select distinct
    instante
  from {{ ref('stg_usina_disp') }}
  where instante is not null
)
select
  {{ dbt_utils.generate_surrogate_key([
    "to_char(instante, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3')"
  ]) }} as id_dim_tempo,
  instante,
  extract(year  from instante) as ano,
  extract(month from instante) as mes,
  extract(day   from instante) as dia,
  extract(hour  from instante) as hora
from base
