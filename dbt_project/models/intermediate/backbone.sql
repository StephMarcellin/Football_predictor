{{
    config(
        materialized='incremental',
        unique_key=['date', 'team', 'opponent'],
        on_schema_change='sync_all_columns',
        schema="intermediate",
        alias='backbone'
    )
}}

WITH
fbref_base AS (
    SELECT
        date, team, opponent, raw_team, raw_opponent,
        venue, season, league_source, result_1n2, comp_category, formation,
        CAST(gf AS INTEGER) AS gf,
        CAST(ga AS INTEGER) AS ga,
        CAST(NULLIF(TRIM(CAST(poss AS VARCHAR)), '') AS DOUBLE) AS possession
    FROM {{ source('silver', 'fbref_schedule') }}
),

fbref_keeper_cte AS (
    SELECT
        date, team, opponent, league_source,
        sota AS shots_on_target_faced, saves, save_pct,
        cs AS clean_sheet, pk_att AS pk_faced, pk_allowed AS pk_conceded
    FROM {{ source('silver', 'fbref_keeper') }}
),

fbref_shooting_cte AS (
    SELECT
        date, team, opponent, league_source,
        standard_sh AS shots_total, standard_sot AS shots_on_target,
        standard_sot_pct AS shots_on_target_pct, standard_g_sh AS goals_per_shot,
        standard_pk AS pk_goals, standard_pkatt AS pk_attempts
    FROM {{ source('silver', 'fbref_shooting') }}
),

fbref_misc_cte AS (
    SELECT
        date, team, opponent, league_source,
        crdy AS yellow_cards, crdr AS red_cards, crdy2 AS second_yellow_cards,
        fls AS fouls_committed, fld AS fouls_drawn, off AS offsides, crosses,
        int AS interceptions, tklw AS tackles_won,
        pkwon AS pk_won, pkcon AS pk_conceded_misc, og AS own_goals
    FROM {{ source('silver', 'fbref_misc') }}
),

understat_base AS (
    SELECT
        u.season, u.home_team, u.away_team, u.match_id, u.league_source,
        CAST(NULLIF(TRIM(CAST(us.home_np_xg      AS VARCHAR)), '') AS DOUBLE) AS home_np_xg,
        CAST(NULLIF(TRIM(CAST(us.away_np_xg      AS VARCHAR)), '') AS DOUBLE) AS away_np_xg,
        CAST(NULLIF(TRIM(CAST(us.home_ppda       AS VARCHAR)), '') AS DOUBLE) AS home_ppda,
        CAST(NULLIF(TRIM(CAST(us.away_ppda       AS VARCHAR)), '') AS DOUBLE) AS away_ppda,
        CAST(NULLIF(TRIM(CAST(us.home_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS home_np_xg_diff,
        CAST(NULLIF(TRIM(CAST(us.away_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS away_np_xg_diff
    FROM {{ source('silver', 'understat_schedule') }} u
    LEFT JOIN {{ source('silver', 'understat_stats') }} us
        ON u.match_id = us.match_id
),

fbref_merged AS (
    SELECT
        b.date, b.team, b.opponent, b.raw_team, b.raw_opponent,
        b.venue, b.season, b.league_source, b.result_1n2, b.comp_category, b.formation,
        b.gf, b.ga, b.possession,
        k.shots_on_target_faced, k.saves, k.save_pct, k.clean_sheet,
        k.pk_faced, k.pk_conceded,
        s.shots_total, s.shots_on_target, s.shots_on_target_pct,
        s.goals_per_shot, s.pk_goals, s.pk_attempts,
        m.yellow_cards, m.red_cards, m.second_yellow_cards,
        m.fouls_committed, m.fouls_drawn, m.offsides, m.crosses,
        m.interceptions, m.tackles_won, m.pk_won, m.pk_conceded_misc, m.own_goals
    FROM fbref_base b
    LEFT JOIN fbref_keeper_cte   k
        ON b.date=k.date AND b.team=k.team AND b.opponent=k.opponent AND b.league_source=k.league_source
    LEFT JOIN fbref_shooting_cte s
        ON b.date=s.date AND b.team=s.team AND b.opponent=s.opponent AND b.league_source=s.league_source
    LEFT JOIN fbref_misc_cte     m
        ON b.date=m.date AND b.team=m.team AND b.opponent=m.opponent AND b.league_source=m.league_source
),

fbref_understat AS (
    SELECT
        f.league_source, f.season, f.venue, u.match_id,
        f.team, f.opponent, f.raw_team, f.raw_opponent, f.date,
        f.result_1n2, f.comp_category, f.formation,
        f.gf, f.ga, f.possession,
        f.shots_on_target_faced, f.saves, f.save_pct, f.clean_sheet,
        f.shots_total, f.shots_on_target, f.shots_on_target_pct,
        f.goals_per_shot, f.yellow_cards, f.second_yellow_cards,
        f.red_cards, f.fouls_committed, f.fouls_drawn, f.interceptions, f.tackles_won,
        CASE WHEN f.venue='Home' THEN u.home_np_xg      ELSE u.away_np_xg      END AS np_xg,
        CASE WHEN f.venue='Home' THEN u.away_np_xg      ELSE u.home_np_xg      END AS np_xg_conceded,
        CASE WHEN f.venue='Home' THEN u.home_ppda       ELSE u.away_ppda       END AS ppda,
        CASE WHEN f.venue='Home' THEN u.away_ppda       ELSE u.home_ppda       END AS ppda_allowed,
        CASE WHEN f.venue='Home' THEN u.home_np_xg_diff ELSE u.away_np_xg_diff END AS np_xg_diff_match
    FROM fbref_merged f
    LEFT JOIN understat_base u
        ON  f.season       = u.season
        AND f.league_source = u.league_source
        AND f.team     = (CASE WHEN f.venue='Home' THEN u.home_team ELSE u.away_team END)
        AND f.opponent = (CASE WHEN f.venue='Home' THEN u.away_team ELSE u.home_team END)
),

whoscored_base AS (
    SELECT
        team, season, league_source,
        ws_home_att_rating, ws_away_att_rating,
        ws_home_def_rating, ws_away_def_rating,
        ws_home_dribbles_pg, ws_away_dribbles_pg,
        ws_home_fouled_pg, ws_away_fouled_pg,
        ws_home_shots_ot_pg, ws_away_shots_ot_pg
    FROM {{ source('silver', 'whoscored_team_season') }}
),

whoscored_features AS (
    SELECT
        b.date, b.team, b.season, b.league_source,
        CASE WHEN b.venue='Home' THEN ws.ws_home_att_rating  ELSE ws.ws_away_att_rating  END AS season_att_rating,
        CASE WHEN b.venue='Home' THEN ws.ws_home_def_rating  ELSE ws.ws_away_def_rating  END AS season_def_rating,
        CASE WHEN b.venue='Home' THEN ws.ws_home_dribbles_pg ELSE ws.ws_away_dribbles_pg END AS ws_dribbles_pg,
        CASE WHEN b.venue='Home' THEN ws.ws_home_fouled_pg   ELSE ws.ws_away_fouled_pg   END AS ws_fouled_pg,
        CASE WHEN b.venue='Home' THEN ws.ws_home_shots_ot_pg ELSE ws.ws_away_shots_ot_pg END AS ws_shots_ot_pg
    FROM fbref_understat b
    LEFT JOIN whoscored_base ws
        ON b.team=ws.team AND b.season=ws.season AND b.league_source=ws.league_source
),

odds_base AS (
    SELECT
        date::DATE AS date, season, league_source, home_team, away_team,
        odds_pinnacle_h, odds_pinnacle_d, odds_pinnacle_a,
        odds_avg_h, odds_avg_d, odds_avg_a,
        pinnacle_prob_h, pinnacle_prob_d, pinnacle_prob_a,
        market_prob_h, market_prob_d, market_prob_a
    FROM {{ source('silver', 'odds') }}
    WHERE pinnacle_prob_h IS NOT NULL
),

final AS (
    SELECT
        -- Identifiants
        f.date, f.team, f.opponent, f.raw_team, f.raw_opponent,
        f.venue, f.season, f.league_source, f.comp_category,
        f.result_1n2, f.match_id,
        f.formation, f.gf, f.ga, f.possession,

        -- FBref
        f.shots_on_target_faced, f.saves, f.save_pct, f.clean_sheet,
        f.shots_total, f.shots_on_target, f.goals_per_shot,
        f.yellow_cards, f.second_yellow_cards, f.red_cards,
        f.fouls_committed, f.interceptions, f.tackles_won,

        -- Understat
        f.np_xg, f.np_xg_conceded, f.ppda, f.ppda_allowed, f.np_xg_diff_match,

        -- WhoScored saison
        wf.season_att_rating, wf.season_def_rating,
        wf.ws_dribbles_pg, wf.ws_fouled_pg, wf.ws_shots_ot_pg,

        -- Cotes Pinnacle
        CASE WHEN f.venue='Home' THEN o.odds_pinnacle_h ELSE o.odds_pinnacle_a END AS odds_pinnacle_team,
        o.odds_pinnacle_d AS odds_pinnacle_draw,
        CASE WHEN f.venue='Home' THEN o.odds_pinnacle_a ELSE o.odds_pinnacle_h END AS odds_pinnacle_opp,
        CASE WHEN f.venue='Home' THEN o.odds_avg_h      ELSE o.odds_avg_a      END AS odds_avg_team,
        o.odds_avg_d AS odds_avg_draw,
        CASE WHEN f.venue='Home' THEN o.odds_avg_a      ELSE o.odds_avg_h      END AS odds_avg_opp,
        CASE WHEN f.venue='Home' THEN o.pinnacle_prob_h ELSE o.pinnacle_prob_a END AS pinnacle_prob_team,
        o.pinnacle_prob_d AS pinnacle_prob_draw,
        CASE WHEN f.venue='Home' THEN o.pinnacle_prob_a ELSE o.pinnacle_prob_h END AS pinnacle_prob_opp,
        CASE WHEN f.venue='Home' THEN o.market_prob_h   ELSE o.market_prob_a   END AS market_prob_team,
        o.market_prob_d AS market_prob_draw,
        CASE WHEN f.venue='Home' THEN o.market_prob_a   ELSE o.market_prob_h   END AS market_prob_opp

    FROM fbref_understat f
    LEFT JOIN whoscored_features wf
        ON f.date=wf.date AND f.team=wf.team AND f.season=wf.season AND f.league_source=wf.league_source
    LEFT JOIN odds_base o
        ON  f.date::DATE = o.date
        AND f.season     = o.season
        AND f.league_source = o.league_source
        AND (CASE WHEN f.venue='Home' THEN f.team     ELSE f.opponent END) = o.home_team
        AND (CASE WHEN f.venue='Home' THEN f.opponent ELSE f.team     END) = o.away_team
)

SELECT * FROM final
-- Filtre COVID Ligue 1 — matchs suspendus
WHERE NOT (
    season = '2019-2020'
    AND league_source IN ('Ligue 1','Ligue 2')
    AND date >= '2020-03-08'
)

{% if is_incremental() %}
AND (date::VARCHAR || '_' || team || '_' || opponent) NOT IN (
    SELECT (date::VARCHAR || '_' || team || '_' || opponent)
    FROM {{ this }}
)
{% endif %}

