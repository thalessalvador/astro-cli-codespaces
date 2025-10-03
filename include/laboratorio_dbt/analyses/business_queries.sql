-- ============================================================
-- PERGUNTA 1
-- Qual a disponibilidade operacional total por estado em um determinado mês?
-- (MÊS ESCOLHIDO: agosto/2025)
-- ============================================================
select
  l.nom_estado,
  sum(f.disp_operacional_mw) as disp_operacional_total_mw
from CORE.fact_disponibilidade f
join CORE.dim_localidade l
  on f.id_dim_localidade = l.id_dim_localidade
where f.instante >= date_trunc('month', to_date('2025-08-01'))
  and f.instante <  dateadd(month, 1, date_trunc('month', to_date('2025-08-01')))
group by l.nom_estado
order by disp_operacional_total_mw desc;

-- ============================================================
-- PERGUNTA 2
-- Qual usina teve a maior média de potência instalada no último trimestre?
-- (DATA DE REFERÊNCIA: 2025-09-12)
-- último trimestre = intervalo [start_of_prev_quarter, start_of_this_quarter)
-- ============================================================
select
  u.nom_usina,
  avg(f.pot_instalada_mw) as media_pot_instalada_mw
from CORE.fact_disponibilidade f
join CORE.dim_usina u
  on f.id_dim_usina = u.id_dim_usina
where f.instante >= dateadd(quarter, -1, date_trunc('quarter', to_date('2025-09-12')))
  and f.instante <  date_trunc('quarter', to_date('2025-09-12'))
group by u.nom_usina
order by media_pot_instalada_mw desc
limit 1;

-- ============================================================
-- PERGUNTA 3
-- Qual a média de disponibilidade sincronizada por hora do dia
-- para usinas do tipo "Hidráulica"?
-- (PERÍODO: últimos 30 dias a partir de 2025-09-12)
-- ============================================================
select
  extract(hour from f.instante) as hora_do_dia,
  avg(f.disp_sincronizada_mw)   as media_disp_sincronizada_mw
from CORE.fact_disponibilidade f
join CORE.dim_usina u
  on f.id_dim_usina = u.id_dim_usina
where u.nom_tipocombustivel ilike 'Hidráulica%'
  and f.instante >= dateadd(day, -30, to_date('2025-09-12'))
  and f.instante <  to_date('2025-09-12')
group by extract(hour from f.instante)
order by hora_do_dia;
