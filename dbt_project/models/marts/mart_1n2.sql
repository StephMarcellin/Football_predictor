{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_1n2 — grain (match_id, team_id) — cible result_1n2 (V/N/D).
-- 100 % CTE. Le profil d'équipe (forme/style + qualité du onze) est construit
-- UNE fois dans team_profile, puis réutilisé pour soi et pour l'adversaire.
-- (Couche zonale imputée + gardien : étape B.)
-- ══════════════════════════════════════════════════════════════════════════════

with

base as (
    select
        match_id, team_id, opponent_id, date, season, result_1n2,

        -- ── Cotes / probabilités (déjà pivotées par venue dans backbone) ──────
        -- Passage quasi-direct : faits ponctuels du match, pas d'agrégat glissant.
        -- Ouverture
        odds_pinnacle_team, odds_pinnacle_draw, odds_pinnacle_opp,
        odds_avg_team, odds_avg_draw, odds_avg_opp,
        pinnacle_prob_team, pinnacle_prob_draw, pinnacle_prob_opp,
        market_prob_team, market_prob_draw, market_prob_opp,
        (pinnacle_prob_team - pinnacle_prob_opp) as pinnacle_edge,
        -- Clôture (les plus "sharp")
        pinnacle_prob_close_team, pinnacle_prob_close_draw, pinnacle_prob_close_opp,
        market_prob_close_team,   market_prob_close_draw,   market_prob_close_opp,
        (pinnacle_prob_close_team - pinnacle_prob_close_opp) as pinnacle_close_edge,
        -- Drift ouverture→clôture (mouvement de ligne)
        pinnacle_drift_team, pinnacle_drift_draw, pinnacle_drift_opp,
        -- Over/Under 2.5 (profil de buts attendu par le marché)
        pinnacle_prob_over25,       pinnacle_prob_under25,
        pinnacle_prob_close_over25, pinnacle_prob_close_under25

    from {{ ref('backbone') }}
    where match_id is not null and team_id is not null
),

-- Profil d'équipe : forme/style (equipe_match) + qualité du onze (equipe_lineup_match).
team_profile as (
    select em.*, lu.* exclude (match_id, team_id)
    from {{ ref('equipe_match') }} em
    left join {{ ref('equipe_lineup_match') }} lu using (match_id, team_id)
),

-- Le même profil, toutes colonnes préfixées opp_ (pour l'adversaire).
team_profile_opp as (
    select columns('.*') as "opp_\0"
    from team_profile
),

-- Directionnel soi↔adversaire : H2H + pressing vs relance. Grain (match, team).
directional as (
    select h2h.*, conf.* exclude (match_id, team_id, opponent_id)
    from {{ ref('equipe_adversaire_match') }} h2h
    left join {{ ref('equipe_confrontation_match') }} conf using (match_id, team_id)
)



select
    base.*,
    tp.*  exclude (match_id, team_id, opponent_id, date, season, league_source, venue),
    tpo.* exclude (opp_match_id, opp_team_id, opp_opponent_id, opp_date, opp_season,
                   opp_league_source, opp_venue, opp_is_home, opp_comp_category),
    d.*   exclude (match_id, team_id, opponent_id, date, season, league_source),
    zcz.* exclude (match_id, team_id),
    kg.*  exclude (match_id, team_id)
from base
left join team_profile     tp  using (match_id, team_id)
left join team_profile_opp tpo on tpo.opp_match_id = base.match_id and tpo.opp_team_id = base.opponent_id
left join directional      d   using (match_id, team_id)
left join {{ ref('equipe_confrontation_zone') }} zcz using (match_id, team_id)
left join {{ ref('equipe_gardien_match') }}      kg  using (match_id, team_id)