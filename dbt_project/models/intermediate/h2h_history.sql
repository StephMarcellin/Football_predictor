{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='h2h_history'
    )
}}

-- Convention (refonte nommage) : lecture de backbone sous ses nouveaux noms, remappés
-- vers des noms de travail courts dans la CTE all_matches ; renommage vers la
-- convention de docs/proposition_nommage_definitif.csv dans la CTE finale renamed.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
WITH all_matches AS (
    SELECT
        str_match_id            AS match_id,
        str_team_id             AS team_id,
        str_opponent_id         AS opponent_id,
        dt_date                 AS date,
        str_season              AS season,
        str_league_source       AS league_source,
        str_venue               AS venue,
        int_gf                  AS gf,
        int_ga                  AS ga,
        str_result_1n2          AS result_1n2,
        dec_np_xg               AS np_xg,
        dec_np_xg_conceded      AS np_xg_conceded
    FROM {{ ref('backbone') }}
),

-- Match non joué (result_1n2 NULL : à venir, reporté, annulé) → flags NULL, pas 0.
-- Avant la refonte, ces matchs comptaient comme joués (h2h_played) et comme
-- « ni victoire ni nul ni défaite » dans les cumuls.
match_flags AS (
    SELECT
        *,
        CASE WHEN result_1n2 IS NULL THEN NULL
             WHEN (result_1n2 = 'H' AND venue = 'Home')
               OR (result_1n2 = 'A' AND venue = 'Away')
             THEN 1 ELSE 0 END AS is_win,
        CASE WHEN result_1n2 IS NULL THEN NULL
             WHEN result_1n2 = 'D'
             THEN 1 ELSE 0 END AS is_draw,
        CASE WHEN result_1n2 IS NULL THEN NULL
             WHEN (result_1n2 = 'A' AND venue = 'Home')
               OR (result_1n2 = 'H' AND venue = 'Away')
             THEN 1 ELSE 0 END AS is_loss,
        CASE WHEN result_1n2 IS NULL THEN NULL ELSE 1 END AS is_played
    FROM all_matches
),

h2h_cumul AS (
    SELECT
        match_id,
        team_id,
        opponent_id,
        date,
        season,
        league_source,
        venue,
        gf,
        ga,
        result_1n2,
        np_xg,
        np_xg_conceded,

        -- ── Cumulé général (toute l'histoire, matchs joués uniquement) ────
        SUM(is_played) OVER w_all AS h2h_played,
        SUM(is_win)    OVER w_all AS h2h_wins,
        SUM(is_draw)   OVER w_all AS h2h_draws,
        SUM(is_loss)   OVER w_all AS h2h_losses,

        SUM(CASE WHEN venue = 'Home' THEN is_played ELSE 0 END) OVER w_all AS h2h_home_played,
        SUM(CASE WHEN venue = 'Home' THEN is_win    ELSE 0 END) OVER w_all AS h2h_home_wins,
        SUM(CASE WHEN venue = 'Home' THEN is_draw   ELSE 0 END) OVER w_all AS h2h_home_draws,
        SUM(CASE WHEN venue = 'Home' THEN is_loss   ELSE 0 END) OVER w_all AS h2h_home_losses,

        SUM(CASE WHEN venue = 'Away' THEN is_played ELSE 0 END) OVER w_all AS h2h_away_played,
        SUM(CASE WHEN venue = 'Away' THEN is_win    ELSE 0 END) OVER w_all AS h2h_away_wins,
        SUM(CASE WHEN venue = 'Away' THEN is_draw   ELSE 0 END) OVER w_all AS h2h_away_draws,
        SUM(CASE WHEN venue = 'Away' THEN is_loss   ELSE 0 END) OVER w_all AS h2h_away_losses,

        -- ── Fenêtre glissante 10 derniers matchs ─────────────────────────
        SUM(is_played) OVER w_10 AS h2h_played_10,
        SUM(is_win)    OVER w_10 AS h2h_wins_10,
        SUM(is_draw)   OVER w_10 AS h2h_draws_10,
        SUM(is_loss)   OVER w_10 AS h2h_losses_10,

        AVG(CAST(gf AS DOUBLE))           OVER w_10 AS h2h_avg_gf_10,
        AVG(CAST(ga AS DOUBLE))           OVER w_10 AS h2h_avg_ga_10,
        AVG(CAST(np_xg - np_xg_conceded AS DOUBLE)) OVER w_10 AS h2h_avg_xg_diff_10,

    FROM match_flags
    WINDOW
        w_all AS (
            PARTITION BY team_id, opponent_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        w_10 AS (
            PARTITION BY team_id, opponent_id
            ORDER BY date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        )
),

-- Renommage final (docs/proposition_nommage_definitif.csv)
renamed AS (
    SELECT
        match_id                                             AS str_match_id,
        CAST(team_id AS VARCHAR)                             AS str_team_id,
        CAST(opponent_id AS VARCHAR)                         AS str_opponent_id,
        date                                                 AS dt_date,
        season                                               AS str_season,
        league_source                                        AS str_league_source,
        venue                                                AS str_venue,
        gf                                                   AS int_gf,
        ga                                                   AS int_ga,
        result_1n2                                           AS str_result_1n2,
        np_xg                                                AS dec_np_xg,
        np_xg_conceded                                       AS dec_np_xg_conceded,
        h2h_played                                           AS int_h2h_played,
        h2h_wins                                             AS int_h2h_wins,
        h2h_draws                                            AS int_h2h_draws,
        h2h_losses                                           AS int_h2h_losses,
        h2h_home_played                                      AS int_h2h_home_played,
        h2h_home_wins                                        AS int_h2h_home_wins,
        h2h_home_draws                                       AS int_h2h_home_draws,
        h2h_home_losses                                      AS int_h2h_home_losses,
        h2h_away_played                                      AS int_h2h_away_played,
        h2h_away_wins                                        AS int_h2h_away_wins,
        h2h_away_draws                                       AS int_h2h_away_draws,
        h2h_away_losses                                      AS int_h2h_away_losses,
        h2h_played_10                                        AS int_h2h_played_10,
        h2h_wins_10                                          AS int_h2h_wins_10,
        h2h_draws_10                                         AS int_h2h_draws_10,
        h2h_losses_10                                        AS int_h2h_losses_10,
        h2h_avg_gf_10                                        AS dec_h2h_avg_gf_10,
        h2h_avg_ga_10                                        AS dec_h2h_avg_ga_10,
        h2h_avg_xg_diff_10                                   AS dec_h2h_avg_xg_diff_10
    FROM h2h_cumul
)

SELECT * FROM renamed

{% if is_incremental() %}
WHERE (str_match_id || '_' || str_team_id) NOT IN (
    SELECT (str_match_id || '_' || str_team_id)
    FROM (
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
        FROM {{ this }}
    )
)
{% endif %}
),

mdl_out AS (
    SELECT
        "str_match_id"                                               AS str_match_id,
        "str_team_id"                                                AS str_team_id,
        "str_opponent_id"                                            AS str_opponent_id,
        "dt_date"                                                    AS dt_date,
        "str_season"                                                 AS str_season,
        "str_league_source"                                          AS str_league_source,
        "str_venue"                                                  AS str_venue,
        "int_gf"                                                     AS int_gf,
        "int_ga"                                                     AS int_ga,
        "str_result_1n2"                                             AS str_result_1n2,
        "dec_np_xg"                                                  AS dec_np_xg,
        "dec_np_xg_conceded"                                         AS dec_np_xg_conceded,
        CAST(int_h2h_played AS BIGINT)                               AS int_h2h_played,
        CAST(int_h2h_wins AS BIGINT)                                 AS int_h2h_wins,
        CAST(int_h2h_draws AS BIGINT)                                AS int_h2h_draws,
        CAST(int_h2h_losses AS BIGINT)                               AS int_h2h_losses,
        CAST(int_h2h_home_played AS BIGINT)                          AS int_h2h_home_played,
        CAST(int_h2h_home_wins AS BIGINT)                            AS int_h2h_home_wins,
        CAST(int_h2h_home_draws AS BIGINT)                           AS int_h2h_home_draws,
        CAST(int_h2h_home_losses AS BIGINT)                          AS int_h2h_home_losses,
        CAST(int_h2h_away_played AS BIGINT)                          AS int_h2h_away_played,
        CAST(int_h2h_away_wins AS BIGINT)                            AS int_h2h_away_wins,
        CAST(int_h2h_away_draws AS BIGINT)                           AS int_h2h_away_draws,
        CAST(int_h2h_away_losses AS BIGINT)                          AS int_h2h_away_losses,
        CAST(int_h2h_played_10 AS BIGINT)                            AS int_h2h_played_10,
        CAST(int_h2h_wins_10 AS BIGINT)                              AS int_h2h_wins_10,
        CAST(int_h2h_draws_10 AS BIGINT)                             AS int_h2h_draws_10,
        CAST(int_h2h_losses_10 AS BIGINT)                            AS int_h2h_losses_10,
        "dec_h2h_avg_gf_10"                                          AS dec_h2h_avg_gf_10,
        "dec_h2h_avg_ga_10"                                          AS dec_h2h_avg_ga_10,
        "dec_h2h_avg_xg_diff_10"                                     AS dec_h2h_avg_xg_diff_10
    FROM mdl_body
)

SELECT * FROM mdl_out
