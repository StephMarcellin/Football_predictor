{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_scorers — grain (match_id, team_id, player_id) — cible scored (a marqué ≥1).
-- Population : joueur_match (finition). Enrichi de :
--   • profil de création + exposition (joueur_saison)
--   • contexte offensif de l'équipe + défensif de l'adversaire
--     (equipe_match, fenêtres rolling 3/5/10, anti-leakage)
--   • priors de qualité de saison PRÉCÉDENTE (equipe_match, _lag)
-- Label dérivé des events (is_goal, hors csc). Pure sélection (aucun calcul).
-- ══════════════════════════════════════════════════════════════════════════════

with goals as (
    select match_id, player_id, 1 as scored
    from {{ ref('int_whoscored_events') }}
    where is_goal and not is_own_goal
    group by 1, 2
)

select
    jm.*,
    em.season,   -- requis par le split temporel (retiré des features par prepare_x)

    -- ── Profil de création + exposition (joueur_saison) ──────────────────────
    js.off_chances_created_per90_lag,
    js.off_key_passes_per90_lag,
    js.off_xgchain_per90_lag,
    js.off_xgbuildup_per90_lag,
    js.scorer_xgot_overperformance_lag,
    js.n_apps_lag,
    js.minutes_lag,
    js.profile_confidence_flag,

    -- ── Contexte offensif de l'ÉQUIPE (equipe_match, rolling anti-leakage) ────
    em.avg_np_xg_rolling_3,  em.avg_np_xg_rolling_5,  em.avg_np_xg_rolling_10,
    em.failed_to_score_rate_rolling_3, em.failed_to_score_rate_rolling_5, em.failed_to_score_rate_rolling_10,
    em.win_rate_rolling_3,   em.win_rate_rolling_5,   em.win_rate_rolling_10,

    -- ── Contexte défensif de l'ADVERSAIRE (equipe_match sur opponent_id) ──────
    opp.avg_np_xg_conceded_rolling_3  as opp_avg_np_xg_conceded_rolling_3,
    opp.avg_np_xg_conceded_rolling_5  as opp_avg_np_xg_conceded_rolling_5,
    opp.avg_np_xg_conceded_rolling_10 as opp_avg_np_xg_conceded_rolling_10,
    opp.clean_sheet_rate_rolling_3  as opp_clean_sheet_rate_rolling_3,
    opp.clean_sheet_rate_rolling_5  as opp_clean_sheet_rate_rolling_5,
    opp.clean_sheet_rate_rolling_10 as opp_clean_sheet_rate_rolling_10,

    -- ── Priors de qualité de saison PRÉCÉDENTE (equipe_match, _lag) ───────────
    em.season_xg_per_shot_for_lag,
    em.season_xg_per_shot_against_lag,
    opp.season_xg_per_shot_for_lag     as opp_season_xg_per_shot_for_lag,
    opp.season_xg_per_shot_against_lag as opp_season_xg_per_shot_against_lag,

    coalesce(g.scored, 0) as scored
from {{ ref('joueur_match') }} jm
left join {{ ref('joueur_saison') }} js  using (match_id, team_id, player_id)
left join {{ ref('equipe_match') }}  em  on em.match_id  = jm.match_id and em.team_id  = jm.team_id
left join {{ ref('equipe_match') }}  opp on opp.match_id = jm.match_id and opp.team_id = jm.opponent_id
left join goals g on g.match_id = jm.match_id and g.player_id = jm.player_id
