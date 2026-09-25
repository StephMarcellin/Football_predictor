{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='equipe_adversaire_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.equipe_adversaire_match — grain (match_id, team_id, opponent_id)
-- Famille CDC : 1 (confrontation directe H2H). Classe temporelle : H2H-CUTOFF.
--
-- h2h_history porte déjà les cumuls ANTÉRIEURS au match (vérif H3 : le match
-- courant n'est jamais compté). On se contente d'en DÉRIVER les taux.
--
-- Garde-fous issus de la vérif H3 :
--   • 14,8 % des lignes ont h2h_played NULL = première confrontation.
--   • Un TAUX sur zéro confrontation n'existe pas → on le laisse NULL
--     (« inconnu »), on ne le force PAS à 0 (0 % ≠ inconnu). L'imputation
--     famille 11 / la sélection de features gèrent le NULL en aval.
--   • Un COMPTE de confrontations, lui, vaut bien 0 quand il n'y en a pas
--     → COALESCE(..., 0) sur les compteurs uniquement.
-- ══════════════════════════════════════════════════════════════════════════════

-- Refonte nommage : h2h_history est lu sous ses nouveaux noms et remappé vers les
-- anciens (CTE h2h_in) — les sorties de ce modèle ne changent pas (types compris :
-- team_id / opponent_id restent INTEGER).

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- h2h_history lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_h2h_history AS (
    SELECT
        str_match_id                                                 AS "str_match_id",
        str_team_id                                                  AS "str_team_id",
        str_opponent_id                                              AS "str_opponent_id",
        dt_date                                                      AS "dt_date",
        str_season                                                   AS "str_season",
        str_league_source                                            AS "str_league_source",
        str_venue                                                    AS "str_venue",
        int_gf                                                       AS "int_gf",
        int_ga                                                       AS "int_ga",
        str_result_1n2                                               AS "str_result_1n2",
        dec_np_xg                                                    AS "dec_np_xg",
        dec_np_xg_conceded                                           AS "dec_np_xg_conceded",
        CAST(int_h2h_played AS HUGEINT)                              AS "int_h2h_played",
        CAST(int_h2h_wins AS HUGEINT)                                AS "int_h2h_wins",
        CAST(int_h2h_draws AS HUGEINT)                               AS "int_h2h_draws",
        CAST(int_h2h_losses AS HUGEINT)                              AS "int_h2h_losses",
        CAST(int_h2h_home_played AS HUGEINT)                         AS "int_h2h_home_played",
        CAST(int_h2h_home_wins AS HUGEINT)                           AS "int_h2h_home_wins",
        CAST(int_h2h_home_draws AS HUGEINT)                          AS "int_h2h_home_draws",
        CAST(int_h2h_home_losses AS HUGEINT)                         AS "int_h2h_home_losses",
        CAST(int_h2h_away_played AS HUGEINT)                         AS "int_h2h_away_played",
        CAST(int_h2h_away_wins AS HUGEINT)                           AS "int_h2h_away_wins",
        CAST(int_h2h_away_draws AS HUGEINT)                          AS "int_h2h_away_draws",
        CAST(int_h2h_away_losses AS HUGEINT)                         AS "int_h2h_away_losses",
        CAST(int_h2h_played_10 AS HUGEINT)                           AS "int_h2h_played_10",
        CAST(int_h2h_wins_10 AS HUGEINT)                             AS "int_h2h_wins_10",
        CAST(int_h2h_draws_10 AS HUGEINT)                            AS "int_h2h_draws_10",
        CAST(int_h2h_losses_10 AS HUGEINT)                           AS "int_h2h_losses_10",
        dec_h2h_avg_gf_10                                            AS "dec_h2h_avg_gf_10",
        dec_h2h_avg_ga_10                                            AS "dec_h2h_avg_ga_10",
        dec_h2h_avg_xg_diff_10                                       AS "dec_h2h_avg_xg_diff_10"
    FROM {{ ref('h2h_history') }}
),

mdl_body AS (
WITH h2h_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        CAST(str_opponent_id AS BIGINT)   AS opponent_id,
        dt_date                            AS date,
        str_season                         AS season,
        str_league_source                  AS league_source,
        int_h2h_played                     AS h2h_played,
        int_h2h_home_played                AS h2h_home_played,
        int_h2h_wins                       AS h2h_wins,
        int_h2h_draws                      AS h2h_draws,
        int_h2h_losses                     AS h2h_losses,
        int_h2h_home_wins                  AS h2h_home_wins,
        dec_h2h_avg_gf_10                  AS h2h_avg_gf_10,
        dec_h2h_avg_ga_10                  AS h2h_avg_ga_10,
        dec_h2h_avg_xg_diff_10             AS h2h_avg_xg_diff_10
    FROM in_h2h_history
),

h2h AS (
    SELECT
        match_id, team_id, opponent_id, date, season, league_source,

        -- Compteurs de confiance (0 = pas d'historique, pas NULL)
        COALESCE(h2h_played, 0)      AS h2h_played,
        COALESCE(h2h_home_played, 0) AS h2h_home_played,

        -- Taux cumulés avant le match (NULL si aucune confrontation)
        h2h_wins::DOUBLE   / NULLIF(h2h_played, 0)      AS h2h_win_rate_cutoff,
        h2h_draws::DOUBLE  / NULLIF(h2h_played, 0)      AS h2h_draw_rate_cutoff,
        h2h_losses::DOUBLE / NULLIF(h2h_played, 0)      AS h2h_loss_rate_cutoff,
        h2h_home_wins::DOUBLE / NULLIF(h2h_home_played, 0) AS h2h_home_win_rate_cutoff,

        -- Moyennes sur les 10 dernières confrontations (déjà matérialisées)
        h2h_avg_gf_10,
        h2h_avg_ga_10,
        h2h_avg_xg_diff_10

    FROM h2h_in
)

SELECT * FROM h2h

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
            dt_date                                                      AS "date",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(int_h2h_played AS HUGEINT)                              AS "h2h_played",
            CAST(int_h2h_home_played AS HUGEINT)                         AS "h2h_home_played",
            dec_h2h_win_rate_cutoff                                      AS "h2h_win_rate_cutoff",
            dec_h2h_draw_rate_cutoff                                     AS "h2h_draw_rate_cutoff",
            dec_h2h_loss_rate_cutoff                                     AS "h2h_loss_rate_cutoff",
            dec_h2h_home_win_rate_cutoff                                 AS "h2h_home_win_rate_cutoff",
            dec_h2h_avg_gf_10                                            AS "h2h_avg_gf_10",
            dec_h2h_avg_ga_10                                            AS "h2h_avg_ga_10",
            dec_h2h_avg_xg_diff_10                                       AS "h2h_avg_xg_diff_10"
        FROM {{ this }}
    ))
{% endif %}
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        "date"                                                       AS dt_date,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        CAST(h2h_played AS BIGINT)                                   AS int_h2h_played,
        CAST(h2h_home_played AS BIGINT)                              AS int_h2h_home_played,
        "h2h_win_rate_cutoff"                                        AS dec_h2h_win_rate_cutoff,
        "h2h_draw_rate_cutoff"                                       AS dec_h2h_draw_rate_cutoff,
        "h2h_loss_rate_cutoff"                                       AS dec_h2h_loss_rate_cutoff,
        "h2h_home_win_rate_cutoff"                                   AS dec_h2h_home_win_rate_cutoff,
        "h2h_avg_gf_10"                                              AS dec_h2h_avg_gf_10,
        "h2h_avg_ga_10"                                              AS dec_h2h_avg_ga_10,
        "h2h_avg_xg_diff_10"                                         AS dec_h2h_avg_xg_diff_10
    FROM mdl_body
)

SELECT * FROM mdl_out
