{{ config(materialized='table') }}

with base as (
  select distinct
    trim(nom_subsistema)                    as nom_subsistema,
    trim(coalesce(nom_estado,'DESCONHECIDO')) as nom_estado
  from {{ ref('stg_usina_disp') }}
  where nom_subsistema is not null
)
select
  {{ dbt_utils.generate_surrogate_key([
    "upper(nom_subsistema)",
    "upper(nom_estado)"
  ]) }} as id_dim_localidade,
  nom_subsistema,
  nom_estado
from base
