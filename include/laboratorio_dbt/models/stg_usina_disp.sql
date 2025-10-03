{{ config(materialized='view') }}

with fonte as (

  -- JULHO/2025
  select
    id_subsistema,
    nom_subsistema,
    nom_estado,
    nom_usina,
    nom_tipocombustivel,
    ceg,
    try_to_timestamp_ntz(din_instante)                           as instante,
    try_to_decimal(to_varchar(val_potenciainstalada), 38, 6)     as pot_instalada_mw,
    try_to_decimal(to_varchar(val_dispoperacional),   38, 6)     as disp_operacional_mw,
    try_to_decimal(to_varchar(val_dispsincronizada),  38, 6)     as disp_sincronizada_mw
  from {{ source('staging','disponibilidade_usina_2025_07') }}

  union all

  -- AGOSTO/2025
  select
    id_subsistema,
    nom_subsistema,
    nom_estado,
    nom_usina,
    nom_tipocombustivel,
    ceg,
    try_to_timestamp_ntz(din_instante),
    try_to_decimal(to_varchar(val_potenciainstalada), 38, 6),
    try_to_decimal(to_varchar(val_dispoperacional),   38, 6),
    try_to_decimal(to_varchar(val_dispsincronizada),  38, 6)
  from {{ source('staging','disponibilidade_usina_2025_08') }}

  union all

  -- SETEMBRO/2025
  select
    id_subsistema,
    nom_subsistema,
    nom_estado,
    nom_usina,
    nom_tipocombustivel,
    ceg,
    try_to_timestamp_ntz(din_instante),
    try_to_decimal(to_varchar(val_potenciainstalada), 38, 6),
    try_to_decimal(to_varchar(val_dispoperacional),   38, 6),
    try_to_decimal(to_varchar(val_dispsincronizada),  38, 6)
  from {{ source('staging','disponibilidade_usina_2025_09') }}
)

select
  id_subsistema,
  nom_subsistema,
  nom_estado,
  nom_usina,
  nom_tipocombustivel,
  ceg,
  instante,
  pot_instalada_mw,
  disp_operacional_mw,
  disp_sincronizada_mw
from fonte
