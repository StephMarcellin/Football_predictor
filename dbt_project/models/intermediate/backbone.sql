{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema="intermediate",
        alias='backbone'
    )
}}

WITH
fbref_base AS (
    SELECT
        match_id,
        team_id, opponent_id,

        date, formation,
        venue, season, league_source, comp_category,
        result_1n2,
        gf,
        ga,
        CAST(NULLIF(TRIM(CAST(poss AS VARCHAR)), '') AS DOUBLE) AS possession
    FROM {{ ref('int_fbref_schedule') }}
),

fbref_keeper_cte AS (
    SELECT
        match_id,
        team_id, opponent_id,

        sota AS shots_on_target_faced,
        ga_keeper,
        saves, save_pct,
        cs AS clean_sheet,
        pk_att AS pk_faced,
        pk_allowed AS pk_conceded,
        pk_saved,
        pk_missed,
    FROM {{ ref('int_fbref_keeper') }}
),

fbref_shooting_cte AS (
    SELECT
        match_id,
        team_id, opponent_id,

        standard_gls as goals,
        standard_sh AS shots_total,
        standard_sot AS shots_on_target,
        standard_sot_pct AS shots_on_target_pct, 
        standard_g_sh AS goals_per_shot,
        standard_pk AS pk_goals, 
        standard_pkatt AS pk_attempts

    FROM {{ ref('int_fbref_shooting') }}
),

fbref_misc_cte AS (
    SELECT
        match_id,
        team_id, opponent_id,

        crdy AS yellow_cards, 
        crdr AS red_cards, 
        crdy2 AS second_yellow_cards,
        fls AS fouls_committed, 
        fld AS fouls_drawn, 
        off AS offsides, 
        crosses,
        int AS interceptions, 
        tklw AS tackles_won,
        pkwon AS pk_won, 
        pkcon AS pk_conceded_misc, 
        og AS own_goals

    FROM {{ ref('int_fbref_misc') }}
),

understat_base AS (
    SELECT
        u.match_id,
        u.team_id, u.opponent_id,

        CAST(NULLIF(TRIM(CAST(us.home_np_xg      AS VARCHAR)), '') AS DOUBLE) AS home_np_xg,
        CAST(NULLIF(TRIM(CAST(us.away_np_xg      AS VARCHAR)), '') AS DOUBLE) AS away_np_xg,
        CAST(NULLIF(TRIM(CAST(us.home_ppda       AS VARCHAR)), '') AS DOUBLE) AS home_ppda,
        CAST(NULLIF(TRIM(CAST(us.away_ppda       AS VARCHAR)), '') AS DOUBLE) AS away_ppda,
        CAST(NULLIF(TRIM(CAST(us.home_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS home_np_xg_diff,
        CAST(NULLIF(TRIM(CAST(us.away_np_xg_diff AS VARCHAR)), '') AS DOUBLE) AS away_np_xg_diff

    FROM {{ ref('int_understat_schedule') }} u
    LEFT JOIN {{ ref('int_understat_stats') }} us
        ON u.match_id = us.match_id
),

fbref_merged AS (
    SELECT
        b.match_id,
        b.team_id, b.opponent_id,

        b.date,  
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
        ON b.match_id=k.match_id
    LEFT JOIN fbref_shooting_cte s
        ON b.match_id=s.match_id
    LEFT JOIN fbref_misc_cte     m
        ON b.match_id=m.match_id
),

fbref_understat AS (
    SELECT
        f.match_id,
        f.team_id, f.opponent_id,
        f.date,

        f.league_source, f.season, f.venue, u.match_id,
        
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
        ON  f.match_id = u.match_id
),

whoscored_base AS (
    SELECT
        team_id, season, league_source,

        ws_home_att_rating, ws_away_att_rating,
        ws_home_def_rating, ws_away_def_rating,
        ws_home_dribbles_pg, ws_away_dribbles_pg,
        ws_home_fouled_pg, ws_away_fouled_pg,
        ws_home_shots_ot_pg, ws_away_shots_ot_pg
    FROM {{ ref('int_whoscored_team_season') }}
),

whoscored_features AS (
    SELECT
        b.match_id, b.team_id, b.season, b.league_source,
        CASE WHEN b.venue='Home' THEN ws.ws_home_att_rating  ELSE ws.ws_away_att_rating  END AS season_att_rating,
        CASE WHEN b.venue='Home' THEN ws.ws_home_def_rating  ELSE ws.ws_away_def_rating  END AS season_def_rating,
        CASE WHEN b.venue='Home' THEN ws.ws_home_dribbles_pg ELSE ws.ws_away_dribbles_pg END AS ws_dribbles_pg,
        CASE WHEN b.venue='Home' THEN ws.ws_home_fouled_pg   ELSE ws.ws_away_fouled_pg   END AS ws_fouled_pg,
        CASE WHEN b.venue='Home' THEN ws.ws_home_shots_ot_pg ELSE ws.ws_away_shots_ot_pg END AS ws_shots_ot_pg
    FROM fbref_understat b
    LEFT JOIN whoscored_base ws
         ON b.team_id=ws.team_id AND b.season=ws.season AND b.league_source=ws.league_source
),

odds_base AS (
    SELECT
        match_id,
        team_id, opponent_id, 

        odds_pinnacle_h, odds_pinnacle_d, odds_pinnacle_a,
        odds_avg_h, odds_avg_d, odds_avg_a,
        pinnacle_prob_h, pinnacle_prob_d, pinnacle_prob_a,
        market_prob_h, market_prob_d, market_prob_a

    FROM {{ ref('int_odds') }}
    WHERE pinnacle_prob_h IS NOT NULL
),

final AS (
    SELECT
        -- Identifiants
        f.match_id,
        f.team_id, f.opponent_id,

        f.date,
        f.venue, f.season, f.league_source, f.comp_category,
        f.result_1n2,
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
        CASE WHEN f.venue='Home' THEN o.market_prob_a   ELSE o.market_prob_h   END AS market_prob_opp,

    FROM fbref_understat f
    LEFT JOIN whoscored_features wf
        ON f.match_id = wf.match_id
        AND f.team_id = wf.team_id
    LEFT JOIN odds_base o
        ON  f.match_id = o.match_id
        AND f.team_id = o.team_id
)

SELECT * FROM final
-- Filtre COVID Ligue 1 — matchs suspendus
WHERE NOT (
    season = '2019-2020'
    AND league_source IN ('Ligue 1','Ligue 2')
    AND date >= '2020-03-08'
)

{% if is_incremental() %}
AND (match_id || '_' || team_id::VARCHAR) NOT IN (
    SELECT (match_id || '_' || team_id::VARCHAR)
    FROM {{ this }}
)
{% endif %}

