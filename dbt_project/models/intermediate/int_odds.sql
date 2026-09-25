{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_odds'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
WITH source AS (
    SELECT * FROM {{ source('silver', 'odds') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

-- Jointure identifiants + passage de TOUTES les colonnes de silver.odds
joined AS (
    SELECT
        r.match_id,
        tm_team.team_id,
        tm_opp.team_id AS opponent_id,
        s.* EXCLUDE (home_team, away_team)
    FROM source s
    LEFT JOIN team_mapping tm_team ON s.home_team = tm_team.club_name
    LEFT JOIN team_mapping tm_opp  ON s.away_team = tm_opp.club_name
    LEFT JOIN registry r
        ON  s.date          = r.match_date
        AND s.league_source = r.league_source
        AND s.season        = r.season
),

-- Probabilités implicites calculées (marge retirée) sur les nouveaux marchés
enriched AS (
    SELECT
        joined.*,

        -- 1N2 clôture — Pinnacle (référence sharp)
        {{ implied_prob_3way('pinnacle_close_1x2_home', 'pinnacle_close_1x2_draw', 'pinnacle_close_1x2_away', 'home') }} AS pinnacle_prob_close_h,
        {{ implied_prob_3way('pinnacle_close_1x2_home', 'pinnacle_close_1x2_draw', 'pinnacle_close_1x2_away', 'draw') }} AS pinnacle_prob_close_d,
        {{ implied_prob_3way('pinnacle_close_1x2_home', 'pinnacle_close_1x2_draw', 'pinnacle_close_1x2_away', 'away') }} AS pinnacle_prob_close_a,

        -- 1N2 clôture — moyenne marché
        {{ implied_prob_3way('market_avg_close_1x2_home', 'market_avg_close_1x2_draw', 'market_avg_close_1x2_away', 'home') }} AS market_prob_close_h,
        {{ implied_prob_3way('market_avg_close_1x2_home', 'market_avg_close_1x2_draw', 'market_avg_close_1x2_away', 'draw') }} AS market_prob_close_d,
        {{ implied_prob_3way('market_avg_close_1x2_home', 'market_avg_close_1x2_draw', 'market_avg_close_1x2_away', 'away') }} AS market_prob_close_a,

        -- Over/Under 2.5 — Pinnacle ouverture
        {{ implied_prob_2way('pinnacle_ou25_over',  'pinnacle_ou25_under') }} AS pinnacle_prob_over25,
        {{ implied_prob_2way('pinnacle_ou25_under', 'pinnacle_ou25_over')  }} AS pinnacle_prob_under25,

        -- Over/Under 2.5 — Pinnacle clôture
        {{ implied_prob_2way('pinnacle_close_ou25_over',  'pinnacle_close_ou25_under') }} AS pinnacle_prob_close_over25,
        {{ implied_prob_2way('pinnacle_close_ou25_under', 'pinnacle_close_ou25_over')  }} AS pinnacle_prob_close_under25

    FROM joined
)

-- Drift ouverture→clôture (proba clôture − proba ouverture) — money informé
SELECT
    enriched.*,
    (pinnacle_prob_close_h - pinnacle_prob_h) AS pinnacle_drift_h,
    (pinnacle_prob_close_d - pinnacle_prob_d) AS pinnacle_drift_d,
    (pinnacle_prob_close_a - pinnacle_prob_a) AS pinnacle_drift_a
FROM enriched
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        "date"                                                       AS dt_date,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "result_fdc"                                                 AS str_result_fdc,
        "div"                                                        AS str_div,
        "time"                                                       AS str_time,
        "odds_pinnacle_h"                                            AS dec_odds_pinnacle_h,
        "odds_pinnacle_d"                                            AS dec_odds_pinnacle_d,
        "odds_pinnacle_a"                                            AS dec_odds_pinnacle_a,
        "odds_avg_h"                                                 AS dec_odds_avg_h,
        "odds_avg_d"                                                 AS dec_odds_avg_d,
        "odds_avg_a"                                                 AS dec_odds_avg_a,
        "odds_max_h"                                                 AS dec_odds_max_h,
        "odds_max_d"                                                 AS dec_odds_max_d,
        "odds_max_a"                                                 AS dec_odds_max_a,
        "pinnacle_prob_h"                                            AS dec_pinnacle_prob_h,
        "pinnacle_prob_d"                                            AS dec_pinnacle_prob_d,
        "pinnacle_prob_a"                                            AS dec_pinnacle_prob_a,
        "market_prob_h"                                              AS dec_market_prob_h,
        "market_prob_d"                                              AS dec_market_prob_d,
        "market_prob_a"                                              AS dec_market_prob_a,
        "ah_line"                                                    AS dec_ah_line,
        "ah_line_close"                                              AS dec_ah_line_close,
        CAST(away_corners AS INTEGER)                                AS int_away_corners,
        CAST(away_fouls AS INTEGER)                                  AS int_away_fouls,
        CAST(away_reds AS INTEGER)                                   AS int_away_reds,
        CAST(away_shots AS INTEGER)                                  AS int_away_shots,
        CAST(away_shots_target AS INTEGER)                           AS int_away_shots_target,
        CAST(away_yellows AS INTEGER)                                AS int_away_yellows,
        "bet365_1x2_away"                                            AS dec_bet365_1x2_away,
        "bet365_1x2_draw"                                            AS dec_bet365_1x2_draw,
        "bet365_1x2_home"                                            AS dec_bet365_1x2_home,
        "bet365_ah_away"                                             AS dec_bet365_ah_away,
        "bet365_ah_home"                                             AS dec_bet365_ah_home,
        "bet365_close_1x2_away"                                      AS dec_bet365_close_1x2_away,
        "bet365_close_1x2_draw"                                      AS dec_bet365_close_1x2_draw,
        "bet365_close_1x2_home"                                      AS dec_bet365_close_1x2_home,
        "bet365_close_ah_away"                                       AS dec_bet365_close_ah_away,
        "bet365_close_ah_home"                                       AS dec_bet365_close_ah_home,
        "bet365_close_ou25_over"                                     AS dec_bet365_close_ou25_over,
        "bet365_close_ou25_under"                                    AS dec_bet365_close_ou25_under,
        "bet365_ou25_over"                                           AS dec_bet365_ou25_over,
        "bet365_ou25_under"                                          AS dec_bet365_ou25_under,
        "betbrain_ah_line"                                           AS dec_betbrain_ah_line,
        "betbrain_avg_1x2_away"                                      AS dec_betbrain_avg_1x2_away,
        "betbrain_avg_1x2_draw"                                      AS dec_betbrain_avg_1x2_draw,
        "betbrain_avg_1x2_home"                                      AS dec_betbrain_avg_1x2_home,
        "betbrain_avg_ah_away"                                       AS dec_betbrain_avg_ah_away,
        "betbrain_avg_ah_home"                                       AS dec_betbrain_avg_ah_home,
        "betbrain_avg_ou25_over"                                     AS dec_betbrain_avg_ou25_over,
        "betbrain_avg_ou25_under"                                    AS dec_betbrain_avg_ou25_under,
        "betbrain_max_1x2_away"                                      AS dec_betbrain_max_1x2_away,
        "betbrain_max_1x2_draw"                                      AS dec_betbrain_max_1x2_draw,
        "betbrain_max_1x2_home"                                      AS dec_betbrain_max_1x2_home,
        "betbrain_max_ah_away"                                       AS dec_betbrain_max_ah_away,
        "betbrain_max_ah_home"                                       AS dec_betbrain_max_ah_home,
        "betbrain_max_ou25_over"                                     AS dec_betbrain_max_ou25_over,
        "betbrain_max_ou25_under"                                    AS dec_betbrain_max_ou25_under,
        CAST(betbrain_n_1x2 AS INTEGER)                              AS int_betbrain_n_1x2,
        CAST(betbrain_n_ah AS INTEGER)                               AS int_betbrain_n_ah,
        CAST(betbrain_n_ou25 AS INTEGER)                             AS int_betbrain_n_ou25,
        "betfair_1x2_away"                                           AS dec_betfair_1x2_away,
        "betfair_1x2_draw"                                           AS dec_betfair_1x2_draw,
        "betfair_1x2_home"                                           AS dec_betfair_1x2_home,
        "betfair_close_1x2_away"                                     AS dec_betfair_close_1x2_away,
        "betfair_close_1x2_draw"                                     AS dec_betfair_close_1x2_draw,
        "betfair_close_1x2_home"                                     AS dec_betfair_close_1x2_home,
        "betfair_exch_1x2_away"                                      AS dec_betfair_exch_1x2_away,
        "betfair_exch_1x2_draw"                                      AS dec_betfair_exch_1x2_draw,
        "betfair_exch_1x2_home"                                      AS dec_betfair_exch_1x2_home,
        "betfair_exch_ah_away"                                       AS dec_betfair_exch_ah_away,
        "betfair_exch_ah_home"                                       AS dec_betfair_exch_ah_home,
        "betfair_exch_close_1x2_away"                                AS dec_betfair_exch_close_1x2_away,
        "betfair_exch_close_1x2_draw"                                AS dec_betfair_exch_close_1x2_draw,
        "betfair_exch_close_1x2_home"                                AS dec_betfair_exch_close_1x2_home,
        "betfair_exch_close_ah_away"                                 AS dec_betfair_exch_close_ah_away,
        "betfair_exch_close_ah_home"                                 AS dec_betfair_exch_close_ah_home,
        "betfair_exch_close_ou25_over"                               AS dec_betfair_exch_close_ou25_over,
        "betfair_exch_close_ou25_under"                              AS dec_betfair_exch_close_ou25_under,
        "betfair_exch_ou25_over"                                     AS dec_betfair_exch_ou25_over,
        "betfair_exch_ou25_under"                                    AS dec_betfair_exch_ou25_under,
        "betfred_1x2_away"                                           AS dec_betfred_1x2_away,
        "betfred_1x2_draw"                                           AS dec_betfred_1x2_draw,
        "betfred_1x2_home"                                           AS dec_betfred_1x2_home,
        "betfred_close_1x2_away"                                     AS dec_betfred_close_1x2_away,
        "betfred_close_1x2_draw"                                     AS dec_betfred_close_1x2_draw,
        "betfred_close_1x2_home"                                     AS dec_betfred_close_1x2_home,
        "betmgm_1x2_away"                                            AS dec_betmgm_1x2_away,
        "betmgm_1x2_draw"                                            AS dec_betmgm_1x2_draw,
        "betmgm_1x2_home"                                            AS dec_betmgm_1x2_home,
        "betmgm_close_1x2_away"                                      AS dec_betmgm_close_1x2_away,
        "betmgm_close_1x2_draw"                                      AS dec_betmgm_close_1x2_draw,
        "betmgm_close_1x2_home"                                      AS dec_betmgm_close_1x2_home,
        "betvictor_1x2_away"                                         AS dec_betvictor_1x2_away,
        "betvictor_1x2_draw"                                         AS dec_betvictor_1x2_draw,
        "betvictor_1x2_home"                                         AS dec_betvictor_1x2_home,
        "betvictor_close_1x2_away"                                   AS dec_betvictor_close_1x2_away,
        "betvictor_close_1x2_draw"                                   AS dec_betvictor_close_1x2_draw,
        "betvictor_close_1x2_home"                                   AS dec_betvictor_close_1x2_home,
        "betwin_1x2_away"                                            AS dec_betwin_1x2_away,
        "betwin_1x2_draw"                                            AS dec_betwin_1x2_draw,
        "betwin_1x2_home"                                            AS dec_betwin_1x2_home,
        "betwin_close_1x2_away"                                      AS dec_betwin_close_1x2_away,
        "betwin_close_1x2_draw"                                      AS dec_betwin_close_1x2_draw,
        "betwin_close_1x2_home"                                      AS dec_betwin_close_1x2_home,
        "coral_1x2_away"                                             AS dec_coral_1x2_away,
        "coral_1x2_draw"                                             AS dec_coral_1x2_draw,
        "coral_1x2_home"                                             AS dec_coral_1x2_home,
        "coral_close_1x2_away"                                       AS dec_coral_close_1x2_away,
        "coral_close_1x2_draw"                                       AS dec_coral_close_1x2_draw,
        "coral_close_1x2_home"                                       AS dec_coral_close_1x2_home,
        CAST(ft_away_goals AS INTEGER)                               AS int_ft_away_goals,
        CAST(ft_home_goals AS INTEGER)                               AS int_ft_home_goals,
        CAST(home_corners AS INTEGER)                                AS int_home_corners,
        CAST(home_fouls AS INTEGER)                                  AS int_home_fouls,
        CAST(home_reds AS INTEGER)                                   AS int_home_reds,
        CAST(home_shots AS INTEGER)                                  AS int_home_shots,
        CAST(home_shots_target AS INTEGER)                           AS int_home_shots_target,
        CAST(home_yellows AS INTEGER)                                AS int_home_yellows,
        CAST(ht_away_goals AS INTEGER)                               AS int_ht_away_goals,
        CAST(ht_home_goals AS INTEGER)                               AS int_ht_home_goals,
        "ht_result"                                                  AS str_ht_result,
        "interwetten_1x2_away"                                       AS dec_interwetten_1x2_away,
        "interwetten_1x2_draw"                                       AS dec_interwetten_1x2_draw,
        "interwetten_1x2_home"                                       AS dec_interwetten_1x2_home,
        "interwetten_close_1x2_away"                                 AS dec_interwetten_close_1x2_away,
        "interwetten_close_1x2_draw"                                 AS dec_interwetten_close_1x2_draw,
        "interwetten_close_1x2_home"                                 AS dec_interwetten_close_1x2_home,
        "ladbrokes_1x2_away"                                         AS dec_ladbrokes_1x2_away,
        "ladbrokes_1x2_draw"                                         AS dec_ladbrokes_1x2_draw,
        "ladbrokes_1x2_home"                                         AS dec_ladbrokes_1x2_home,
        "ladbrokes_close_1x2_away"                                   AS dec_ladbrokes_close_1x2_away,
        "ladbrokes_close_1x2_draw"                                   AS dec_ladbrokes_close_1x2_draw,
        "ladbrokes_close_1x2_home"                                   AS dec_ladbrokes_close_1x2_home,
        "market_avg_1x2_away"                                        AS dec_market_avg_1x2_away,
        "market_avg_1x2_draw"                                        AS dec_market_avg_1x2_draw,
        "market_avg_1x2_home"                                        AS dec_market_avg_1x2_home,
        "market_avg_ah_away"                                         AS dec_market_avg_ah_away,
        "market_avg_ah_home"                                         AS dec_market_avg_ah_home,
        "market_avg_close_1x2_away"                                  AS dec_market_avg_close_1x2_away,
        "market_avg_close_1x2_draw"                                  AS dec_market_avg_close_1x2_draw,
        "market_avg_close_1x2_home"                                  AS dec_market_avg_close_1x2_home,
        "market_avg_close_ah_away"                                   AS dec_market_avg_close_ah_away,
        "market_avg_close_ah_home"                                   AS dec_market_avg_close_ah_home,
        "market_avg_close_ou25_over"                                 AS dec_market_avg_close_ou25_over,
        "market_avg_close_ou25_under"                                AS dec_market_avg_close_ou25_under,
        "market_avg_ou25_over"                                       AS dec_market_avg_ou25_over,
        "market_avg_ou25_under"                                      AS dec_market_avg_ou25_under,
        "market_max_1x2_away"                                        AS dec_market_max_1x2_away,
        "market_max_1x2_draw"                                        AS dec_market_max_1x2_draw,
        "market_max_1x2_home"                                        AS dec_market_max_1x2_home,
        "market_max_ah_away"                                         AS dec_market_max_ah_away,
        "market_max_ah_home"                                         AS dec_market_max_ah_home,
        "market_max_close_1x2_away"                                  AS dec_market_max_close_1x2_away,
        "market_max_close_1x2_draw"                                  AS dec_market_max_close_1x2_draw,
        "market_max_close_1x2_home"                                  AS dec_market_max_close_1x2_home,
        "market_max_close_ah_away"                                   AS dec_market_max_close_ah_away,
        "market_max_close_ah_home"                                   AS dec_market_max_close_ah_home,
        "market_max_close_ou25_over"                                 AS dec_market_max_close_ou25_over,
        "market_max_close_ou25_under"                                AS dec_market_max_close_ou25_under,
        "market_max_ou25_over"                                       AS dec_market_max_ou25_over,
        "market_max_ou25_under"                                      AS dec_market_max_ou25_under,
        "onexbet_1x2_away"                                           AS dec_onexbet_1x2_away,
        "onexbet_1x2_draw"                                           AS dec_onexbet_1x2_draw,
        "onexbet_1x2_home"                                           AS dec_onexbet_1x2_home,
        "onexbet_close_1x2_away"                                     AS dec_onexbet_close_1x2_away,
        "onexbet_close_1x2_draw"                                     AS dec_onexbet_close_1x2_draw,
        "onexbet_close_1x2_home"                                     AS dec_onexbet_close_1x2_home,
        "pinnacle_1x2_away"                                          AS dec_pinnacle_1x2_away,
        "pinnacle_1x2_draw"                                          AS dec_pinnacle_1x2_draw,
        "pinnacle_1x2_home"                                          AS dec_pinnacle_1x2_home,
        "pinnacle_ah_away"                                           AS dec_pinnacle_ah_away,
        "pinnacle_ah_home"                                           AS dec_pinnacle_ah_home,
        "pinnacle_close_1x2_away"                                    AS dec_pinnacle_close_1x2_away,
        "pinnacle_close_1x2_draw"                                    AS dec_pinnacle_close_1x2_draw,
        "pinnacle_close_1x2_home"                                    AS dec_pinnacle_close_1x2_home,
        "pinnacle_close_ah_away"                                     AS dec_pinnacle_close_ah_away,
        "pinnacle_close_ah_home"                                     AS dec_pinnacle_close_ah_home,
        "pinnacle_close_ou25_over"                                   AS dec_pinnacle_close_ou25_over,
        "pinnacle_close_ou25_under"                                  AS dec_pinnacle_close_ou25_under,
        "pinnacle_ou25_over"                                         AS dec_pinnacle_ou25_over,
        "pinnacle_ou25_under"                                        AS dec_pinnacle_ou25_under,
        "referee"                                                    AS str_referee,
        "vcbet_1x2_away"                                             AS dec_vcbet_1x2_away,
        "vcbet_1x2_draw"                                             AS dec_vcbet_1x2_draw,
        "vcbet_1x2_home"                                             AS dec_vcbet_1x2_home,
        "vcbet_close_1x2_away"                                       AS dec_vcbet_close_1x2_away,
        "vcbet_close_1x2_draw"                                       AS dec_vcbet_close_1x2_draw,
        "vcbet_close_1x2_home"                                       AS dec_vcbet_close_1x2_home,
        "william_hill_1x2_away"                                      AS dec_william_hill_1x2_away,
        "william_hill_1x2_draw"                                      AS dec_william_hill_1x2_draw,
        "william_hill_1x2_home"                                      AS dec_william_hill_1x2_home,
        "william_hill_close_1x2_away"                                AS dec_william_hill_close_1x2_away,
        "william_hill_close_1x2_draw"                                AS dec_william_hill_close_1x2_draw,
        "william_hill_close_1x2_home"                                AS dec_william_hill_close_1x2_home,
        "pinnacle_prob_close_h"                                      AS dec_pinnacle_prob_close_h,
        "pinnacle_prob_close_d"                                      AS dec_pinnacle_prob_close_d,
        "pinnacle_prob_close_a"                                      AS dec_pinnacle_prob_close_a,
        "market_prob_close_h"                                        AS dec_market_prob_close_h,
        "market_prob_close_d"                                        AS dec_market_prob_close_d,
        "market_prob_close_a"                                        AS dec_market_prob_close_a,
        "pinnacle_prob_over25"                                       AS dec_pinnacle_prob_over25,
        "pinnacle_prob_under25"                                      AS dec_pinnacle_prob_under25,
        "pinnacle_prob_close_over25"                                 AS dec_pinnacle_prob_close_over25,
        "pinnacle_prob_close_under25"                                AS dec_pinnacle_prob_close_under25,
        "pinnacle_drift_h"                                           AS dec_pinnacle_drift_h,
        "pinnacle_drift_d"                                           AS dec_pinnacle_drift_d,
        "pinnacle_drift_a"                                           AS dec_pinnacle_drift_a
    FROM mdl_body
)

SELECT * FROM mdl_out
