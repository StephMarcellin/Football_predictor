{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='h2h_history'
    )
}}

WITH all_matches AS (
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
        np_xg_conceded
    FROM {{ ref('backbone') }}
),

match_flags AS (
    SELECT
        *,
        CASE WHEN (result_1n2 = 'H' AND venue = 'Home')
                OR (result_1n2 = 'A' AND venue = 'Away')
             THEN 1 ELSE 0 END AS is_win,
        CASE WHEN result_1n2 = 'D'
             THEN 1 ELSE 0 END AS is_draw,
        CASE WHEN (result_1n2 = 'A' AND venue = 'Home')
                OR (result_1n2 = 'H' AND venue = 'Away')
             THEN 1 ELSE 0 END AS is_loss
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

        -- ── Cumulé général (toute l'histoire) ────────────────────────────
        SUM(1)        OVER w_all AS h2h_played,
        SUM(is_win)   OVER w_all AS h2h_wins,
        SUM(is_draw)  OVER w_all AS h2h_draws,
        SUM(is_loss)  OVER w_all AS h2h_losses,

        SUM(CASE WHEN venue = 'Home' THEN 1       ELSE 0 END) OVER w_all AS h2h_home_played,
        SUM(CASE WHEN venue = 'Home' THEN is_win  ELSE 0 END) OVER w_all AS h2h_home_wins,
        SUM(CASE WHEN venue = 'Home' THEN is_draw ELSE 0 END) OVER w_all AS h2h_home_draws,
        SUM(CASE WHEN venue = 'Home' THEN is_loss ELSE 0 END) OVER w_all AS h2h_home_losses,

        SUM(CASE WHEN venue = 'Away' THEN 1       ELSE 0 END) OVER w_all AS h2h_away_played,
        SUM(CASE WHEN venue = 'Away' THEN is_win  ELSE 0 END) OVER w_all AS h2h_away_wins,
        SUM(CASE WHEN venue = 'Away' THEN is_draw ELSE 0 END) OVER w_all AS h2h_away_draws,
        SUM(CASE WHEN venue = 'Away' THEN is_loss ELSE 0 END) OVER w_all AS h2h_away_losses,

        -- ── Fenêtre glissante 10 derniers matchs ─────────────────────────
        SUM(1)        OVER w_10 AS h2h_played_10,
        SUM(is_win)   OVER w_10 AS h2h_wins_10,
        SUM(is_draw)  OVER w_10 AS h2h_draws_10,
        SUM(is_loss)  OVER w_10 AS h2h_losses_10,

        AVG(CAST(gf AS DOUBLE))           OVER w_10 AS h2h_avg_gf_10,
        AVG(CAST(ga AS DOUBLE))           OVER w_10 AS h2h_avg_ga_10,

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
)

SELECT * FROM h2h_cumul

{% if is_incremental() %}
WHERE (match_id || '_' || team_id::VARCHAR) NOT IN (
    SELECT (match_id || '_' || team_id::VARCHAR)
    FROM {{ this }}
)
{% endif %}