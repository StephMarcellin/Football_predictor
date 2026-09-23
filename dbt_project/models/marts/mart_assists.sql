{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_assists — grain (match_id, team_id, player_id) — cible assisted (≥1 passe déc.).
-- Population : joueur_saison (tous les joueurs profilés → passeurs de tout poste).
-- Enrichi de :
--   • exposition + tireur de coups de pied arrêtés (joueur_saison)
--   • contexte offensif de l'équipe + défensif de l'adversaire
--     (equipe_match, fenêtres rolling 3/5/10, anti-leakage)
--   • priors de qualité de saison PRÉCÉDENTE (equipe_match, _lag)
-- Label : player_match_stats.n_assists ≥ 1. Pure sélection (aucun calcul).
-- ══════════════════════════════════════════════════════════════════════════════

select
    js.match_id,
    js.team_id,
    js.player_id,
    js.season,
    case when b.venue = 'Home' then true else false end as is_home,
    b.opponent_id,

    -- ── Profil du passeur + exposition (joueur_saison) ───────────────────────
    js.scorer_xg_per90_lag,
    js.scorer_shots_per90_lag,
    js.off_chances_created_per90_lag,
    js.off_key_passes_per90_lag,
    js.off_xgchain_per90_lag,
    js.off_xgbuildup_per90_lag,
    js.n_apps_lag,
    js.minutes_lag,
    js.scorer_freekick_taker_lag,
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

    case when pms.n_assists >= 1 then 1 else 0 end as assisted
from {{ ref('joueur_saison') }} js
left join {{ ref('backbone') }}           b   using (match_id, team_id)
left join {{ ref('player_match_stats') }} pms using (match_id, team_id, player_id)
left join {{ ref('equipe_match') }}       em  on em.match_id  = js.match_id and em.team_id  = js.team_id
left join {{ ref('equipe_match') }}       opp on opp.match_id = js.match_id and opp.team_id = b.opponent_id
