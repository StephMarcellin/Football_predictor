{{
    config(
        materialized='incremental',
        unique_key=['date', 'team', 'opponent'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='features_rolling'
    )
}}

WITH
base_with_gap AS (
    SELECT *,
        CAST(date AS DATE) - LAG(CAST(date AS DATE))
            OVER (PARTITION BY team ORDER BY date) AS days_since_last_match
    FROM {{ ref('backbone') }}
),

season_ratings_lagged AS (
    SELECT team, league_source, season,
        MAX(season_att_rating) AS season_att_rating_raw,
        MAX(season_def_rating) AS season_def_rating_raw
    FROM {{ ref('backbone') }}
    GROUP BY team, league_source, season
),

season_ratings_prev AS (
    SELECT team, league_source, season,
        LAG(season_att_rating_raw) OVER (
            PARTITION BY team, league_source ORDER BY season
        ) AS season_att_rating,
        LAG(season_def_rating_raw) OVER (
            PARTITION BY team, league_source ORDER BY season
        ) AS season_def_rating
    FROM season_ratings_lagged
),

features_raw AS (
    SELECT
        date, team, opponent, venue, season, league_source,
        comp_category, match_id, result_1n2, formation,
        CASE WHEN venue='Home' THEN 1 ELSE 0 END AS is_home,
        days_since_last_match,
        CASE WHEN days_since_last_match > 20 THEN 1 ELSE 0 END AS is_return_from_break,
        CASE WHEN days_since_last_match < 4  THEN 1 ELSE 0 END AS is_short_rest,

        -- Rolling windows générées par macro
        {% for w in [3, 5, 10] %}
        {{ rolling_cols(w) }}
        {% endfor %}

        -- Features supplémentaires
        AVG(CASE WHEN result_1n2='D' THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team, season, league_source ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS draw_rate_5,

        AVG(CASE WHEN venue='Home' AND result_1n2='H' THEN 1.0
         WHEN venue='Home' THEN 0.0 ELSE NULL END)
            OVER (PARTITION BY team, league_source ORDER BY date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS home_win_rate_hist,

        TRY_CAST(split_part(formation, '-', 1) AS INTEGER) AS form_n_defenders,
        CASE
            WHEN len(string_split(formation, '-')) = 3
            THEN TRY_CAST(split_part(formation, '-', 2) AS INTEGER)
            WHEN len(string_split(formation, '-')) = 4
            THEN TRY_CAST(split_part(formation, '-', 2) AS INTEGER)
               + TRY_CAST(split_part(formation, '-', 3) AS INTEGER)
            ELSE NULL
        END AS form_n_midfielders,
        TRY_CAST(split_part(formation, '-', len(string_split(formation, '-'))) AS INTEGER) AS form_n_attackers,

        (
            CASE WHEN LAG(formation,1) OVER (PARTITION BY team, season, league_source ORDER BY date) = formation THEN 1.0 ELSE 0.0 END
          + CASE WHEN LAG(formation,2) OVER (PARTITION BY team, season, league_source ORDER BY date) = formation THEN 1.0 ELSE 0.0 END
          + CASE WHEN LAG(formation,3) OVER (PARTITION BY team, season, league_source ORDER BY date) = formation THEN 1.0 ELSE 0.0 END
          + CASE WHEN LAG(formation,4) OVER (PARTITION BY team, season, league_source ORDER BY date) = formation THEN 1.0 ELSE 0.0 END
          + CASE WHEN LAG(formation,5) OVER (PARTITION BY team, season, league_source ORDER BY date) = formation THEN 1.0 ELSE 0.0 END
        ) / 5.0 AS form_familiarity_5,

        CASE
            WHEN formation IS NULL THEN NULL
            WHEN LAG(formation,1) OVER (PARTITION BY team, season, league_source ORDER BY date) IS NULL THEN NULL
            WHEN formation = LAG(formation,1) OVER (PARTITION BY team, season, league_source ORDER BY date) THEN 0
            ELSE 1
        END AS form_change_flag,

        ws_dribbles_pg, ws_fouled_pg, ws_shots_ot_pg,
        odds_pinnacle_team, odds_pinnacle_draw, odds_pinnacle_opp,
        odds_avg_team, odds_avg_draw, odds_avg_opp,
        pinnacle_prob_team, pinnacle_prob_draw, pinnacle_prob_opp,
        market_prob_team, market_prob_draw, market_prob_opp

    FROM base_with_gap
)

SELECT f.*, sr.season_att_rating, sr.season_def_rating
FROM features_raw f
LEFT JOIN season_ratings_prev sr
    ON f.team=sr.team AND f.league_source=sr.league_source AND f.season=sr.season

{% if is_incremental() %}
WHERE (f.date::VARCHAR || '_' || f.team || '_' || f.opponent) NOT IN (
    SELECT (date::VARCHAR || '_' || team || '_' || opponent)
    FROM {{ this }}
)
{% endif %}