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

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_lineup lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_lineup AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_formation_seq                                            AS "formation_seq",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        int_period                                                   AS "period",
        int_start_minute                                             AS "start_minute",
        int_end_minute                                               AS "end_minute",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        int_slot                                                     AS "slot",
        dec_grid_vertical                                            AS "grid_vertical",
        dec_grid_horizontal                                          AS "grid_horizontal",
        bool_is_captain                                              AS "is_captain"
    FROM {{ ref('int_whoscored_lineup') }}
),

-- joueur_saison lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_joueur_saison AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dt_date                                                      AS "date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        int_n_apps_lag                                               AS "n_apps_lag",
        CAST(int_minutes_lag AS HUGEINT)                             AS "minutes_lag",
        dec_scorer_xg_per90_lag                                      AS "scorer_xg_per90_lag",
        dec_scorer_shots_per90_lag                                   AS "scorer_shots_per90_lag",
        dec_off_chances_created_per90_lag                            AS "off_chances_created_per90_lag",
        dec_off_key_passes_per90_lag                                 AS "off_key_passes_per90_lag",
        dec_off_xg_per_shot_lag                                      AS "off_xg_per_shot_lag",
        dec_def_aerial_win_rate_lag                                  AS "def_aerial_win_rate_lag",
        dec_def_actions_per90_lag                                    AS "def_actions_per90_lag",
        dec_def_errors_per90_lag                                     AS "def_errors_per90_lag",
        dec_player_card_propensity_lag                               AS "player_card_propensity_lag",
        dec_off_xgchain_per90_lag                                    AS "off_xgchain_per90_lag",
        dec_off_xgbuildup_per90_lag                                  AS "off_xgbuildup_per90_lag",
        dec_scorer_team_shot_share_lag                               AS "scorer_team_shot_share_lag",
        CAST(int_scorer_penalty_taker_lag AS HUGEINT)                AS "scorer_penalty_taker_lag",
        CAST(int_scorer_freekick_taker_lag AS HUGEINT)               AS "scorer_freekick_taker_lag",
        dec_def_threat_conceded_per90_lag                            AS "def_threat_conceded_per90_lag",
        dec_scorer_xgot_overperformance_lag                          AS "scorer_xgot_overperformance_lag",
        str_profile_confidence_flag                                  AS "profile_confidence_flag"
    FROM {{ ref('joueur_saison') }}
),

mdl_body AS (
WITH xi AS (
    SELECT DISTINCT match_id, team_id, player_id
    FROM in_int_whoscored_lineup
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
    LEFT JOIN in_joueur_saison js
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
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "lineup_sum_xg_per90_lag"                                    AS dec_lineup_sum_xg_per90_lag,
        "lineup_avg_shots_per90_lag"                                 AS dec_lineup_avg_shots_per90_lag,
        "lineup_avg_def_actions_per90_lag"                           AS dec_lineup_avg_def_actions_per90_lag,
        "lineup_avg_aerial_win_rate_lag"                             AS dec_lineup_avg_aerial_win_rate_lag,
        "n_starters_profiled"                                        AS int_n_starters_profiled
    FROM mdl_body
)

SELECT * FROM mdl_out
