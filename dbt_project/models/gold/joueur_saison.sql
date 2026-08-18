{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='joueur_saison'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.joueur_saison — grain (match_id, team_id, player_id) — snapshot as-of-date
-- Familles CDC : 4 (source des agrégats du onze), 5/6 NON-zonales, 8 (carton),
-- 10 (Buteurs).
--   PASSE 1 : player_match_stats (volumes per-90, ratios).
--   PASSE 2 : player_xg_chain (xgChain/xgBuildup), part des tirs de l'équipe,
--             rôles tireur penalty / coup franc.
--   RESTE À FAIRE : scorer_xgot_overperformance (feature 70) — nécessite de
--   relier xgot_predictions aux tirs (int_shot_placement) pour retrouver le
--   tireur ; traité dans une passe dédiée.
--
-- Fenêtre as-of-date : 38 dernières apparitions strictement antérieures
-- (ROWS 38 PRECEDING AND 1 PRECEDING, match courant exclu → anti-leakage).
-- Volumes en PER-90 (Σ stat / Σ minutes × 90) ; ratios exacts.
-- ══════════════════════════════════════════════════════════════════════════════

WITH

-- 1) Base joueur-match (dédoublonnée sur le scrape le plus récent).
base AS (
    SELECT
        match_id, team_id, player_id, date, season, league_source,
        minutes_played,
        xg_contribution, n_shots, n_chances_created, n_key_passes,
        n_aerial_won, n_aerial_duels, n_tackles, n_interceptions,
        n_errors_lead_to_shot, n_errors_lead_to_goal,
        n_yellow_cards, n_second_yellows
    FROM {{ ref('player_match_stats') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY match_id, team_id, player_id ORDER BY scraped_at DESC
    ) = 1
),

-- 2) Tirs de l'équipe par match (dénominateur de la part de tirs).
team_shots AS (
    SELECT match_id, team_id, SUM(n_shots) AS team_shots_match
    FROM base GROUP BY match_id, team_id
),

-- 3) xgChain / xgBuildup du joueur par match (Σ sur ses chaînes distinctes).
xgc AS (
    SELECT match_id, chain_team_id AS team_id, player_id,
        SUM(xgchain)   AS xgchain_match,
        SUM(xgbuildup) AS xgbuildup_match
    FROM (
        SELECT DISTINCT match_id, chain_id, chain_team_id, player_id, xgchain, xgbuildup
        FROM {{ ref('player_xg_chain') }}
    )
    GROUP BY match_id, chain_team_id, player_id
),

-- 4) Penaltys tirés par le joueur dans le match (rôle de tireur).
pen AS (
    SELECT match_id, attacking_team_id AS team_id, taker_player_id AS player_id,
        COUNT(*) AS n_pens_taken
    FROM {{ ref('int_penalties') }}
    WHERE taker_player_id IS NOT NULL
    GROUP BY match_id, attacking_team_id, taker_player_id
),

-- 5) Coups francs tirés par le joueur dans le match (rôle de tireur).
fk AS (
    SELECT match_id, chain_team_id AS team_id, freekick_taker_id AS player_id,
        COUNT(*) AS n_fk_taken
    FROM {{ ref('freekick_profiles') }}
    WHERE freekick_taker_id IS NOT NULL
    GROUP BY match_id, chain_team_id, freekick_taker_id
),

-- 6a) Menace concédée par le joueur dans le match (feature 48, non-zonale).
threat_conceded_match AS (
    SELECT match_id, team_id, player_id, SUM(threat_conceded) AS threat_conceded_match
    FROM {{ ref('threat_conceded') }}
    WHERE match_id IS NOT NULL
    GROUP BY match_id, team_id, player_id
),

-- 6b) xGOT : buts vs xGOT du joueur dans le match (feature 70). xgot_predictions
--     est produit par le pipeline Python (table externe, PAS un modèle dbt) → on
--     la référence en direct ; elle doit exister au moment du run dbt. On récupère
--     le tireur via int_shot_placement (jointure sur match_id + row_num, 100%).
xgot_match AS (
    SELECT sp.match_id, sp.team_id, sp.player_id,
        SUM(CASE WHEN xg.is_goal THEN 1 ELSE 0 END) AS n_goals_ot,
        SUM(xg.xgot)                                AS sum_xgot
    FROM machine_learning.xgot_predictions xg
    JOIN {{ ref('int_shot_placement') }} sp
        ON sp.match_id = xg.match_id AND sp.row_num = xg.row_num
    WHERE sp.match_id IS NOT NULL
    GROUP BY sp.match_id, sp.team_id, sp.player_id
),

-- 7) Enrichissement au grain joueur-match (LEFT JOIN → 0 si absent).
enriched AS (
    SELECT
        b.*,
        ts.team_shots_match,
        COALESCE(x.xgchain_match, 0)   AS xgchain_match,
        COALESCE(x.xgbuildup_match, 0) AS xgbuildup_match,
        COALESCE(p.n_pens_taken, 0)    AS n_pens_taken,
        COALESCE(f.n_fk_taken, 0)      AS n_fk_taken,
        COALESCE(tc.threat_conceded_match, 0) AS threat_conceded_match,
        COALESCE(xo.n_goals_ot, 0)     AS n_goals_ot,
        COALESCE(xo.sum_xgot, 0)       AS sum_xgot
    FROM base b
    LEFT JOIN team_shots ts USING (match_id, team_id)
    LEFT JOIN xgc x         USING (match_id, team_id, player_id)
    LEFT JOIN pen p         USING (match_id, team_id, player_id)
    LEFT JOIN fk  f         USING (match_id, team_id, player_id)
    LEFT JOIN threat_conceded_match tc USING (match_id, team_id, player_id)
    LEFT JOIN xgot_match xo USING (match_id, team_id, player_id)
),

-- 7) Profil as-of-date sur les 38 dernières apparitions antérieures.
profil AS (
    SELECT
        match_id, team_id, player_id, date, season, league_source,

        {% set w %}PARTITION BY player_id ORDER BY date, match_id ROWS BETWEEN 38 PRECEDING AND 1 PRECEDING{% endset %}
        {% set mins %}NULLIF(SUM(minutes_played) OVER ({{ w }}), 0){% endset %}

        -- Confiance / volume
        COUNT(*)             OVER ({{ w }}) AS n_apps_lag,
        SUM(minutes_played)  OVER ({{ w }}) AS minutes_lag,

        -- ══ PASSE 1 ══
        -- Famille 10 / 5 (offensif) — per-90
        SUM(xg_contribution) OVER ({{ w }}) / {{ mins }} * 90 AS scorer_xg_per90_lag,
        SUM(n_shots)         OVER ({{ w }}) / {{ mins }} * 90 AS scorer_shots_per90_lag,
        SUM(n_chances_created) OVER ({{ w }}) / {{ mins }} * 90 AS off_chances_created_per90_lag,
        SUM(n_key_passes)    OVER ({{ w }}) / {{ mins }} * 90 AS off_key_passes_per90_lag,
        SUM(xg_contribution) OVER ({{ w }}) / NULLIF(SUM(n_shots) OVER ({{ w }}), 0) AS off_xg_per_shot_lag,
        -- Famille 6 (défensif non-zonal)
        SUM(n_aerial_won)    OVER ({{ w }}) / NULLIF(SUM(n_aerial_duels) OVER ({{ w }}), 0) AS def_aerial_win_rate_lag,
        SUM(n_tackles + n_interceptions)            OVER ({{ w }}) / {{ mins }} * 90 AS def_actions_per90_lag,
        SUM(n_errors_lead_to_shot + n_errors_lead_to_goal) OVER ({{ w }}) / {{ mins }} * 90 AS def_errors_per90_lag,
        -- Famille 8 (discipline)
        SUM((n_yellow_cards + n_second_yellows)::DOUBLE) OVER ({{ w }}) / {{ mins }} * 90 AS player_card_propensity_lag,

        -- ══ PASSE 2 ══
        -- Famille 5 — implication dans la construction (per-90)
        SUM(xgchain_match)   OVER ({{ w }}) / {{ mins }} * 90 AS off_xgchain_per90_lag,
        SUM(xgbuildup_match) OVER ({{ w }}) / {{ mins }} * 90 AS off_xgbuildup_per90_lag,
        -- Famille 10 — part des tirs de l'équipe (ratio), rôles CPA (compteurs)
        SUM(n_shots)     OVER ({{ w }}) / NULLIF(SUM(team_shots_match) OVER ({{ w }}), 0) AS scorer_team_shot_share_lag,
        SUM(n_pens_taken) OVER ({{ w }}) AS scorer_penalty_taker_lag,
        SUM(n_fk_taken)   OVER ({{ w }}) AS scorer_freekick_taker_lag,
        -- Famille 6 — menace concédée par 90 (feature 48, non-zonale)
        SUM(threat_conceded_match) OVER ({{ w }}) / {{ mins }} * 90 AS def_threat_conceded_per90_lag,
        -- Famille 10 — surperformance à la finition = Σ buts − Σ xGOT sur la fenêtre
        -- (positif = finisseur clinique). NULL si aucun tir cadré (division protégée).
        (SUM(n_goals_ot) OVER ({{ w }}) - SUM(sum_xgot) OVER ({{ w }}))
            / NULLIF(SUM(sum_xgot) OVER ({{ w }}), 0) AS scorer_xgot_overperformance_lag

    FROM enriched
)

SELECT *,
    -- [Famille 11, feature 79] Fiabilité du profil selon le nb d'apparitions dans
    -- la fenêtre (seuils par défaut, à calibrer). 'none' → cible d'imputation.
    CASE WHEN n_apps_lag >= 20 THEN 'high'
         WHEN n_apps_lag >= 5  THEN 'medium'
         WHEN n_apps_lag >= 1  THEN 'low'
         ELSE 'none' END AS profile_confidence_flag
FROM profil

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
