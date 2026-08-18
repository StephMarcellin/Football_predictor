{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_goals — grain (match_id, team_id) — cible team_goals (buts marqués).
-- Réutilise les features match-équipe de mart_1n2 (mêmes signaux), cible = gf.
-- Le modèle prédit 0/1/2/3+ → dérive O/U total, BTTS, clean sheet.
-- ══════════════════════════════════════════════════════════════════════════════

select
    mm.* exclude (result_1n2),
    b.gf as team_goals
from {{ ref('mart_1n2') }} mm
join {{ ref('backbone') }} b using (match_id, team_id)