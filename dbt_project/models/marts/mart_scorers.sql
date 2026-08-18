{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_scorers — grain (match_id, team_id, player_id) — cible scored (a marqué ≥1).
-- Population : joueur_match (finition, focalisé attaque). Enrichi du profil de
-- création (joueur_saison). Label dérivé des events (is_goal, hors csc).
-- ══════════════════════════════════════════════════════════════════════════════

with goals as (
    select match_id, player_id, 1 as scored
    from {{ ref('int_whoscored_events') }}
    where is_goal and not is_own_goal
    group by 1, 2
)

select
    jm.*,
    js.off_chances_created_per90_lag,
    js.off_key_passes_per90_lag,
    js.off_xgchain_per90_lag,
    js.off_xgbuildup_per90_lag,
    js.scorer_xgot_overperformance_lag,
    js.n_apps_lag,
    js.profile_confidence_flag,
    coalesce(g.scored, 0) as scored
from {{ ref('joueur_match') }} jm
left join {{ ref('joueur_saison') }} js using (match_id, team_id, player_id)
left join goals g on g.match_id = jm.match_id and g.player_id = jm.player_id