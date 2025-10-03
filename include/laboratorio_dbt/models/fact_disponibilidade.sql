{{ config(materialized='table') }}

with base as (
  select
    trim(nom_usina)                          as nom_usina,
    trim(coalesce(nom_tipocombustivel,''))   as nom_tipocombustivel,
    trim(coalesce(ceg,''))                   as ceg,
    trim(nom_subsistema)                     as nom_subsistema,
    trim(coalesce(nom_estado,'DESCONHECIDO')) as nom_estado,  
    instante,
    cast(date_trunc('day', instante) as date) as dia,
    pot_instalada_mw,
    disp_operacional_mw,
    disp_sincronizada_mw
  from {{ ref('stg_usina_disp') }}
  where instante is not null
),
joined as (
  select
    u.id_dim_usina           as id_dim_usina,
    l.id_dim_localidade      as id_dim_localidade,
    t.id_dim_tempo           as id_dim_tempo,
    b.dia                    as dia,
    b.instante               as instante,
    b.pot_instalada_mw       as pot_instalada_mw,
    b.disp_operacional_mw    as disp_operacional_mw,
    b.disp_sincronizada_mw   as disp_sincronizada_mw
  from base b
  left join {{ ref('dim_usina') }} u
    on  upper(b.nom_usina)           = upper(u.nom_usina)
    and upper(b.nom_tipocombustivel) = upper(coalesce(u.nom_tipocombustivel,''))
    and upper(b.ceg)                 = upper(coalesce(u.ceg,''))
  left join {{ ref('dim_localidade') }} l
    on  upper(b.nom_subsistema) = upper(l.nom_subsistema)
    and upper(b.nom_estado)     = upper(l.nom_estado)
  left join {{ ref('dim_tempo') }} t
    on  b.instante = t.instante
)
select
  id_dim_usina,
  id_dim_localidade,
  id_dim_tempo,
  dia,
  instante,
  pot_instalada_mw,
  disp_operacional_mw,
  disp_sincronizada_mw
from joined
