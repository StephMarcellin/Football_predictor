{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_assists — grain (match_id, team_id, player_id) — cible assisted (≥1 passe déc.).
-- Population : joueur_saison (tous les joueurs profilés → capte les passeurs de
-- tout poste). Label : player_match_stats.n_assists ≥ 1.
-- ══════════════════════════════════════════════════════════════════════════════

select
    js.match_id,
    js.team_id,
    js.player_id,
    js.season,
    case when b.venue = 'Home' then true else false end as is_home,
    b.opponent_id,
    js.scorer_xg_per90_lag,
    js.scorer_shots_per90_lag,
    js.off_chances_created_per90_lag,
    js.off_key_passes_per90_lag,
    js.off_xgchain_per90_lag,
    js.off_xgbuildup_per90_lag,
    js.n_apps_lag,
    js.profile_confidence_flag,
    case when pms.n_assists >= 1 then 1 else 0 end as assisted
from {{ ref('joueur_saison') }} js
left join {{ ref('backbone') }} b using (match_id, team_id)
left join {{ ref('player_match_stats') }} pms using (match_id, team_id, player_id)