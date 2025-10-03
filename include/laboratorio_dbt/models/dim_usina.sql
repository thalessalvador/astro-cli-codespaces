{{ config(materialized='table') }}

with base as (
  select distinct
    trim(nom_usina)                   as nom_usina,
    trim(coalesce(nom_tipocombustivel,'')) as nom_tipocombustivel,
    trim(coalesce(ceg,''))            as ceg
  from {{ ref('stg_usina_disp') }}
  where nom_usina is not null
)
select
  {{ dbt_utils.generate_surrogate_key([
    "upper(nom_usina)",
    "upper(coalesce(nom_tipocombustivel,''))",
    "upper(coalesce(ceg,''))"
  ]) }} as id_dim_usina,
  nom_usina,
  nom_tipocombustivel,
  nullif(ceg,'') as ceg
from base
