{{
    config(
        materialized='table',
        schema='gold',
        alias='equipe_lineup_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.equipe_lineup_match — grain (match_id, team_id)
-- Famille CDC 4 : qualité BRUTE des 11 titulaires, indépendante des matchups.
-- Répond à « cette compo est-elle forte dans l'absolu » et fait tourner le
-- simulateur de lineup. Table séparée d'equipe_match car dépendante de la compo
-- (connue ~1 h avant le coup d'envoi), là où equipe_match est connu des jours avant.
--
-- Méthode : XI de départ (int_whoscored_lineup, start_minute=0) → on agrège les
-- profils SEASON-LAG des joueurs (joueur_saison, déjà as-of-date, donc sans fuite).
--
-- Reporté : lineup_avg_age (source int_whoscored_player_match.age salie — min 0,
-- médiane gonflée, cf. troubleshooting) ; lineup_avg_rating_lag (nécessite un
-- season-lag du rating) ; n_key_starters_missing (nécessite un seuil « titulaire
-- habituel » — hyperparamètre à calibrer).
-- ══════════════════════════════════════════════════════════════════════════════

WITH xi AS (
    SELECT DISTINCT match_id, team_id, player_id
    FROM {{ ref('int_whoscored_lineup') }}
    WHERE start_minute = 0 AND match_id IS NOT NULL
),

xi_prof AS (
    SELECT
        x.match_id, x.team_id,
        js.scorer_xg_per90_lag,
        js.scorer_shots_per90_lag,
        js.def_actions_per90_lag,
        js.def_aerial_win_rate_lag
    FROM xi x
    LEFT JOIN {{ ref('joueur_saison') }} js
        ON  js.match_id  = x.match_id
        AND js.team_id   = x.team_id
        AND js.player_id = x.player_id
)

SELECT
    match_id,
    team_id,
    SUM(scorer_xg_per90_lag)        AS lineup_sum_xg_per90_lag,
    AVG(scorer_shots_per90_lag)     AS lineup_avg_shots_per90_lag,
    AVG(def_actions_per90_lag)      AS lineup_avg_def_actions_per90_lag,
    AVG(def_aerial_win_rate_lag)    AS lineup_avg_aerial_win_rate_lag,
    COUNT(scorer_xg_per90_lag)      AS n_starters_profiled
FROM xi_prof
GROUP BY match_id, team_id
