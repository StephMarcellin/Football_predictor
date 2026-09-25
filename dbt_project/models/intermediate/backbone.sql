{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema="intermediate",
        alias='backbone'
    )
}}

-- Grain : 1 ligne par (match, équipe). Base = int_fbref_schedule (toutes compétitions,
-- matchs à venir inclus), enrichie par les autres int_* sur (str_match_id, str_team_id).
-- Ne contient QUE des faits au grain match. Les agrégats de saison (int_whoscored_team_season,
-- grain équipe × saison) n'ont rien à faire ici : ils sont joints en gold, décalés en N-1
-- (cf. gold.equipe_match), sinon fuite de données (agrégat de la saison en cours).
-- Noms de sortie conformes à docs/proposition_nommage_definitif.csv (préfixes de type).
-- ⚠ Changement de schéma de sortie : premier run en --full-refresh obligatoire.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_odds lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_odds AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS INTEGER)                                 AS "team_id",
        CAST(str_opponent_id AS INTEGER)                             AS "opponent_id",
        dt_date                                                      AS "date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        str_result_fdc                                               AS "result_fdc",
        str_div                                                      AS "div",
        str_time                                                     AS "time",
        dec_odds_pinnacle_h                                          AS "odds_pinnacle_h",
        dec_odds_pinnacle_d                                          AS "odds_pinnacle_d",
        dec_odds_pinnacle_a                                          AS "odds_pinnacle_a",
        dec_odds_avg_h                                               AS "odds_avg_h",
        dec_odds_avg_d                                               AS "odds_avg_d",
        dec_odds_avg_a                                               AS "odds_avg_a",
        dec_odds_max_h                                               AS "odds_max_h",
        dec_odds_max_d                                               AS "odds_max_d",
        dec_odds_max_a                                               AS "odds_max_a",
        dec_pinnacle_prob_h                                          AS "pinnacle_prob_h",
        dec_pinnacle_prob_d                                          AS "pinnacle_prob_d",
        dec_pinnacle_prob_a                                          AS "pinnacle_prob_a",
        dec_market_prob_h                                            AS "market_prob_h",
        dec_market_prob_d                                            AS "market_prob_d",
        dec_market_prob_a                                            AS "market_prob_a",
        dec_ah_line                                                  AS "ah_line",
        dec_ah_line_close                                            AS "ah_line_close",
        CAST(int_away_corners AS DOUBLE)                             AS "away_corners",
        CAST(int_away_fouls AS DOUBLE)                               AS "away_fouls",
        CAST(int_away_reds AS DOUBLE)                                AS "away_reds",
        CAST(int_away_shots AS DOUBLE)                               AS "away_shots",
        CAST(int_away_shots_target AS DOUBLE)                        AS "away_shots_target",
        CAST(int_away_yellows AS DOUBLE)                             AS "away_yellows",
        dec_bet365_1x2_away                                          AS "bet365_1x2_away",
        dec_bet365_1x2_draw                                          AS "bet365_1x2_draw",
        dec_bet365_1x2_home                                          AS "bet365_1x2_home",
        dec_bet365_ah_away                                           AS "bet365_ah_away",
        dec_bet365_ah_home                                           AS "bet365_ah_home",
        dec_bet365_close_1x2_away                                    AS "bet365_close_1x2_away",
        dec_bet365_close_1x2_draw                                    AS "bet365_close_1x2_draw",
        dec_bet365_close_1x2_home                                    AS "bet365_close_1x2_home",
        dec_bet365_close_ah_away                                     AS "bet365_close_ah_away",
        dec_bet365_close_ah_home                                     AS "bet365_close_ah_home",
        dec_bet365_close_ou25_over                                   AS "bet365_close_ou25_over",
        dec_bet365_close_ou25_under                                  AS "bet365_close_ou25_under",
        dec_bet365_ou25_over                                         AS "bet365_ou25_over",
        dec_bet365_ou25_under                                        AS "bet365_ou25_under",
        dec_betbrain_ah_line                                         AS "betbrain_ah_line",
        dec_betbrain_avg_1x2_away                                    AS "betbrain_avg_1x2_away",
        dec_betbrain_avg_1x2_draw                                    AS "betbrain_avg_1x2_draw",
        dec_betbrain_avg_1x2_home                                    AS "betbrain_avg_1x2_home",
        dec_betbrain_avg_ah_away                                     AS "betbrain_avg_ah_away",
        dec_betbrain_avg_ah_home                                     AS "betbrain_avg_ah_home",
        dec_betbrain_avg_ou25_over                                   AS "betbrain_avg_ou25_over",
        dec_betbrain_avg_ou25_under                                  AS "betbrain_avg_ou25_under",
        dec_betbrain_max_1x2_away                                    AS "betbrain_max_1x2_away",
        dec_betbrain_max_1x2_draw                                    AS "betbrain_max_1x2_draw",
        dec_betbrain_max_1x2_home                                    AS "betbrain_max_1x2_home",
        dec_betbrain_max_ah_away                                     AS "betbrain_max_ah_away",
        dec_betbrain_max_ah_home                                     AS "betbrain_max_ah_home",
        dec_betbrain_max_ou25_over                                   AS "betbrain_max_ou25_over",
        dec_betbrain_max_ou25_under                                  AS "betbrain_max_ou25_under",
        CAST(int_betbrain_n_1x2 AS DOUBLE)                           AS "betbrain_n_1x2",
        CAST(int_betbrain_n_ah AS DOUBLE)                            AS "betbrain_n_ah",
        CAST(int_betbrain_n_ou25 AS DOUBLE)                          AS "betbrain_n_ou25",
        dec_betfair_1x2_away                                         AS "betfair_1x2_away",
        dec_betfair_1x2_draw                                         AS "betfair_1x2_draw",
        dec_betfair_1x2_home                                         AS "betfair_1x2_home",
        dec_betfair_close_1x2_away                                   AS "betfair_close_1x2_away",
        dec_betfair_close_1x2_draw                                   AS "betfair_close_1x2_draw",
        dec_betfair_close_1x2_home                                   AS "betfair_close_1x2_home",
        dec_betfair_exch_1x2_away                                    AS "betfair_exch_1x2_away",
        dec_betfair_exch_1x2_draw                                    AS "betfair_exch_1x2_draw",
        dec_betfair_exch_1x2_home                                    AS "betfair_exch_1x2_home",
        dec_betfair_exch_ah_away                                     AS "betfair_exch_ah_away",
        dec_betfair_exch_ah_home                                     AS "betfair_exch_ah_home",
        dec_betfair_exch_close_1x2_away                              AS "betfair_exch_close_1x2_away",
        dec_betfair_exch_close_1x2_draw                              AS "betfair_exch_close_1x2_draw",
        dec_betfair_exch_close_1x2_home                              AS "betfair_exch_close_1x2_home",
        dec_betfair_exch_close_ah_away                               AS "betfair_exch_close_ah_away",
        dec_betfair_exch_close_ah_home                               AS "betfair_exch_close_ah_home",
        dec_betfair_exch_close_ou25_over                             AS "betfair_exch_close_ou25_over",
        dec_betfair_exch_close_ou25_under                            AS "betfair_exch_close_ou25_under",
        dec_betfair_exch_ou25_over                                   AS "betfair_exch_ou25_over",
        dec_betfair_exch_ou25_under                                  AS "betfair_exch_ou25_under",
        dec_betfred_1x2_away                                         AS "betfred_1x2_away",
        dec_betfred_1x2_draw                                         AS "betfred_1x2_draw",
        dec_betfred_1x2_home                                         AS "betfred_1x2_home",
        dec_betfred_close_1x2_away                                   AS "betfred_close_1x2_away",
        dec_betfred_close_1x2_draw                                   AS "betfred_close_1x2_draw",
        dec_betfred_close_1x2_home                                   AS "betfred_close_1x2_home",
        dec_betmgm_1x2_away                                          AS "betmgm_1x2_away",
        dec_betmgm_1x2_draw                                          AS "betmgm_1x2_draw",
        dec_betmgm_1x2_home                                          AS "betmgm_1x2_home",
        dec_betmgm_close_1x2_away                                    AS "betmgm_close_1x2_away",
        dec_betmgm_close_1x2_draw                                    AS "betmgm_close_1x2_draw",
        dec_betmgm_close_1x2_home                                    AS "betmgm_close_1x2_home",
        dec_betvictor_1x2_away                                       AS "betvictor_1x2_away",
        dec_betvictor_1x2_draw                                       AS "betvictor_1x2_draw",
        dec_betvictor_1x2_home                                       AS "betvictor_1x2_home",
        dec_betvictor_close_1x2_away                                 AS "betvictor_close_1x2_away",
        dec_betvictor_close_1x2_draw                                 AS "betvictor_close_1x2_draw",
        dec_betvictor_close_1x2_home                                 AS "betvictor_close_1x2_home",
        dec_betwin_1x2_away                                          AS "betwin_1x2_away",
        dec_betwin_1x2_draw                                          AS "betwin_1x2_draw",
        dec_betwin_1x2_home                                          AS "betwin_1x2_home",
        dec_betwin_close_1x2_away                                    AS "betwin_close_1x2_away",
        dec_betwin_close_1x2_draw                                    AS "betwin_close_1x2_draw",
        dec_betwin_close_1x2_home                                    AS "betwin_close_1x2_home",
        dec_coral_1x2_away                                           AS "coral_1x2_away",
        dec_coral_1x2_draw                                           AS "coral_1x2_draw",
        dec_coral_1x2_home                                           AS "coral_1x2_home",
        dec_coral_close_1x2_away                                     AS "coral_close_1x2_away",
        dec_coral_close_1x2_draw                                     AS "coral_close_1x2_draw",
        dec_coral_close_1x2_home                                     AS "coral_close_1x2_home",
        CAST(int_ft_away_goals AS DOUBLE)                            AS "ft_away_goals",
        CAST(int_ft_home_goals AS DOUBLE)                            AS "ft_home_goals",
        CAST(int_home_corners AS DOUBLE)                             AS "home_corners",
        CAST(int_home_fouls AS DOUBLE)                               AS "home_fouls",
        CAST(int_home_reds AS DOUBLE)                                AS "home_reds",
        CAST(int_home_shots AS DOUBLE)                               AS "home_shots",
        CAST(int_home_shots_target AS DOUBLE)                        AS "home_shots_target",
        CAST(int_home_yellows AS DOUBLE)                             AS "home_yellows",
        CAST(int_ht_away_goals AS DOUBLE)                            AS "ht_away_goals",
        CAST(int_ht_home_goals AS DOUBLE)                            AS "ht_home_goals",
        str_ht_result                                                AS "ht_result",
        dec_interwetten_1x2_away                                     AS "interwetten_1x2_away",
        dec_interwetten_1x2_draw                                     AS "interwetten_1x2_draw",
        dec_interwetten_1x2_home                                     AS "interwetten_1x2_home",
        dec_interwetten_close_1x2_away                               AS "interwetten_close_1x2_away",
        dec_interwetten_close_1x2_draw                               AS "interwetten_close_1x2_draw",
        dec_interwetten_close_1x2_home                               AS "interwetten_close_1x2_home",
        dec_ladbrokes_1x2_away                                       AS "ladbrokes_1x2_away",
        dec_ladbrokes_1x2_draw                                       AS "ladbrokes_1x2_draw",
        dec_ladbrokes_1x2_home                                       AS "ladbrokes_1x2_home",
        dec_ladbrokes_close_1x2_away                                 AS "ladbrokes_close_1x2_away",
        dec_ladbrokes_close_1x2_draw                                 AS "ladbrokes_close_1x2_draw",
        dec_ladbrokes_close_1x2_home                                 AS "ladbrokes_close_1x2_home",
        dec_market_avg_1x2_away                                      AS "market_avg_1x2_away",
        dec_market_avg_1x2_draw                                      AS "market_avg_1x2_draw",
        dec_market_avg_1x2_home                                      AS "market_avg_1x2_home",
        dec_market_avg_ah_away                                       AS "market_avg_ah_away",
        dec_market_avg_ah_home                                       AS "market_avg_ah_home",
        dec_market_avg_close_1x2_away                                AS "market_avg_close_1x2_away",
        dec_market_avg_close_1x2_draw                                AS "market_avg_close_1x2_draw",
        dec_market_avg_close_1x2_home                                AS "market_avg_close_1x2_home",
        dec_market_avg_close_ah_away                                 AS "market_avg_close_ah_away",
        dec_market_avg_close_ah_home                                 AS "market_avg_close_ah_home",
        dec_market_avg_close_ou25_over                               AS "market_avg_close_ou25_over",
        dec_market_avg_close_ou25_under                              AS "market_avg_close_ou25_under",
        dec_market_avg_ou25_over                                     AS "market_avg_ou25_over",
        dec_market_avg_ou25_under                                    AS "market_avg_ou25_under",
        dec_market_max_1x2_away                                      AS "market_max_1x2_away",
        dec_market_max_1x2_draw                                      AS "market_max_1x2_draw",
        dec_market_max_1x2_home                                      AS "market_max_1x2_home",
        dec_market_max_ah_away                                       AS "market_max_ah_away",
        dec_market_max_ah_home                                       AS "market_max_ah_home",
        dec_market_max_close_1x2_away                                AS "market_max_close_1x2_away",
        dec_market_max_close_1x2_draw                                AS "market_max_close_1x2_draw",
        dec_market_max_close_1x2_home                                AS "market_max_close_1x2_home",
        dec_market_max_close_ah_away                                 AS "market_max_close_ah_away",
        dec_market_max_close_ah_home                                 AS "market_max_close_ah_home",
        dec_market_max_close_ou25_over                               AS "market_max_close_ou25_over",
        dec_market_max_close_ou25_under                              AS "market_max_close_ou25_under",
        dec_market_max_ou25_over                                     AS "market_max_ou25_over",
        dec_market_max_ou25_under                                    AS "market_max_ou25_under",
        dec_onexbet_1x2_away                                         AS "onexbet_1x2_away",
        dec_onexbet_1x2_draw                                         AS "onexbet_1x2_draw",
        dec_onexbet_1x2_home                                         AS "onexbet_1x2_home",
        dec_onexbet_close_1x2_away                                   AS "onexbet_close_1x2_away",
        dec_onexbet_close_1x2_draw                                   AS "onexbet_close_1x2_draw",
        dec_onexbet_close_1x2_home                                   AS "onexbet_close_1x2_home",
        dec_pinnacle_1x2_away                                        AS "pinnacle_1x2_away",
        dec_pinnacle_1x2_draw                                        AS "pinnacle_1x2_draw",
        dec_pinnacle_1x2_home                                        AS "pinnacle_1x2_home",
        dec_pinnacle_ah_away                                         AS "pinnacle_ah_away",
        dec_pinnacle_ah_home                                         AS "pinnacle_ah_home",
        dec_pinnacle_close_1x2_away                                  AS "pinnacle_close_1x2_away",
        dec_pinnacle_close_1x2_draw                                  AS "pinnacle_close_1x2_draw",
        dec_pinnacle_close_1x2_home                                  AS "pinnacle_close_1x2_home",
        dec_pinnacle_close_ah_away                                   AS "pinnacle_close_ah_away",
        dec_pinnacle_close_ah_home                                   AS "pinnacle_close_ah_home",
        dec_pinnacle_close_ou25_over                                 AS "pinnacle_close_ou25_over",
        dec_pinnacle_close_ou25_under                                AS "pinnacle_close_ou25_under",
        dec_pinnacle_ou25_over                                       AS "pinnacle_ou25_over",
        dec_pinnacle_ou25_under                                      AS "pinnacle_ou25_under",
        str_referee                                                  AS "referee",
        dec_vcbet_1x2_away                                           AS "vcbet_1x2_away",
        dec_vcbet_1x2_draw                                           AS "vcbet_1x2_draw",
        dec_vcbet_1x2_home                                           AS "vcbet_1x2_home",
        dec_vcbet_close_1x2_away                                     AS "vcbet_close_1x2_away",
        dec_vcbet_close_1x2_draw                                     AS "vcbet_close_1x2_draw",
        dec_vcbet_close_1x2_home                                     AS "vcbet_close_1x2_home",
        dec_william_hill_1x2_away                                    AS "william_hill_1x2_away",
        dec_william_hill_1x2_draw                                    AS "william_hill_1x2_draw",
        dec_william_hill_1x2_home                                    AS "william_hill_1x2_home",
        dec_william_hill_close_1x2_away                              AS "william_hill_close_1x2_away",
        dec_william_hill_close_1x2_draw                              AS "william_hill_close_1x2_draw",
        dec_william_hill_close_1x2_home                              AS "william_hill_close_1x2_home",
        dec_pinnacle_prob_close_h                                    AS "pinnacle_prob_close_h",
        dec_pinnacle_prob_close_d                                    AS "pinnacle_prob_close_d",
        dec_pinnacle_prob_close_a                                    AS "pinnacle_prob_close_a",
        dec_market_prob_close_h                                      AS "market_prob_close_h",
        dec_market_prob_close_d                                      AS "market_prob_close_d",
        dec_market_prob_close_a                                      AS "market_prob_close_a",
        dec_pinnacle_prob_over25                                     AS "pinnacle_prob_over25",
        dec_pinnacle_prob_under25                                    AS "pinnacle_prob_under25",
        dec_pinnacle_prob_close_over25                               AS "pinnacle_prob_close_over25",
        dec_pinnacle_prob_close_under25                              AS "pinnacle_prob_close_under25",
        dec_pinnacle_drift_h                                         AS "pinnacle_drift_h",
        dec_pinnacle_drift_d                                         AS "pinnacle_drift_d",
        dec_pinnacle_drift_a                                         AS "pinnacle_drift_a"
    FROM {{ ref('int_odds') }}
),

mdl_body AS (
WITH
fbref_base AS (
    SELECT
        str_match_id,
        str_team_id, str_opponent_id,

        dt_date, str_formation,
        str_venue, str_season, str_league_source, str_comp_category,
        str_result_1n2,
        int_gf,
        int_ga,
        int_poss
    FROM {{ ref('int_fbref_schedule') }}
),

fbref_keeper_cte AS (
    SELECT
        str_match_id,
        str_team_id,

        int_sota                                      AS int_shots_on_target_faced,
        int_saves,
        dec_save_pct,
        int_cs                      AS int_clean_sheet
    FROM {{ ref('int_fbref_keeper') }}
),

fbref_shooting_cte AS (
    SELECT
        str_match_id,
        str_team_id,

        int_standard_sh                               AS int_shots_total,
        int_standard_sot                              AS int_shots_on_target,
        dec_standard_g_sh                             AS dec_goals_per_shot
    FROM {{ ref('int_fbref_shooting') }}
),

fbref_misc_cte AS (
    SELECT
        str_match_id,
        str_team_id,

        int_crdy                                      AS int_yellow_cards,
        int_crdr                                      AS int_red_cards,
        int_crdy2                                     AS int_second_yellow_cards,
        int_fls                                       AS int_fouls_committed,
        int_int                                       AS int_interceptions,
        int_tklw                                      AS int_tackles_won
    FROM {{ ref('int_fbref_misc') }}
),

-- Understat : 1 ligne par match (colonnes home_/away_), rattachée au match_id unifié
-- directement dans int_understat_stats (plus besoin de repasser par le calendrier).
understat_base AS (
    SELECT
        str_match_id,
        dec_home_np_xg, dec_away_np_xg,
        dec_home_ppda,  dec_away_ppda,
        dec_home_np_xg_diff, dec_away_np_xg_diff
    FROM {{ ref('int_understat_stats') }}
    WHERE str_match_id IS NOT NULL
),

fbref_merged AS (
    SELECT
        b.*,
        k.int_shots_on_target_faced, k.int_saves, k.dec_save_pct, k.int_clean_sheet,
        s.int_shots_total, s.int_shots_on_target, s.dec_goals_per_shot,
        m.int_yellow_cards, m.int_red_cards, m.int_second_yellow_cards,
        m.int_fouls_committed, m.int_interceptions, m.int_tackles_won
    FROM fbref_base b
    LEFT JOIN fbref_keeper_cte   k
        ON b.str_match_id = k.str_match_id AND b.str_team_id = k.str_team_id
    LEFT JOIN fbref_shooting_cte s
        ON b.str_match_id = s.str_match_id AND b.str_team_id = s.str_team_id
    LEFT JOIN fbref_misc_cte     m
        ON b.str_match_id = m.str_match_id AND b.str_team_id = m.str_team_id
),

-- Pivot Understat home/away → point de vue de l'équipe (venue)
fbref_understat AS (
    SELECT
        f.*,
        CASE WHEN f.str_venue = 'Home' THEN u.dec_home_np_xg      ELSE u.dec_away_np_xg      END AS dec_np_xg,
        CASE WHEN f.str_venue = 'Home' THEN u.dec_away_np_xg      ELSE u.dec_home_np_xg      END AS dec_np_xg_conceded,
        CASE WHEN f.str_venue = 'Home' THEN u.dec_home_ppda       ELSE u.dec_away_ppda       END AS dec_ppda,
        CASE WHEN f.str_venue = 'Home' THEN u.dec_away_ppda       ELSE u.dec_home_ppda       END AS dec_ppda_allowed,
        CASE WHEN f.str_venue = 'Home' THEN u.dec_home_np_xg_diff ELSE u.dec_away_np_xg_diff END AS dec_np_xg_diff_match
    FROM fbref_merged f
    LEFT JOIN understat_base u
        ON f.str_match_id = u.str_match_id
),

-- int_odds n'est pas encore refondu (anciens noms, team_id INTEGER) : cast explicite
-- en VARCHAR pour la jointure (DuckDB ne caste plus implicitement VARCHAR ↔ INTEGER).
odds_base AS (
    SELECT
        match_id                                      AS str_match_id,
        CAST(team_id AS VARCHAR)                      AS str_team_id,

        odds_pinnacle_h, odds_pinnacle_d, odds_pinnacle_a,
        odds_avg_h, odds_avg_d, odds_avg_a,
        pinnacle_prob_h, pinnacle_prob_d, pinnacle_prob_a,
        market_prob_h, market_prob_d, market_prob_a,

        -- 1N2 clôture (Pinnacle + marché) — probas no-vig
        pinnacle_prob_close_h, pinnacle_prob_close_d, pinnacle_prob_close_a,
        market_prob_close_h,   market_prob_close_d,   market_prob_close_a,

        -- Drift ouverture→clôture (Pinnacle)
        pinnacle_drift_h, pinnacle_drift_d, pinnacle_drift_a,

        -- Over/Under 2.5 (Pinnacle) — neutre au venue, pas de pivot
        pinnacle_prob_over25,       pinnacle_prob_under25,
        pinnacle_prob_close_over25, pinnacle_prob_close_under25

    FROM in_int_odds
    WHERE pinnacle_prob_h IS NOT NULL
),

final AS (
    SELECT
        -- Identifiants
        f.str_match_id,
        f.str_team_id, f.str_opponent_id,

        f.dt_date,
        f.str_venue, f.str_season, f.str_league_source, f.str_comp_category,
        f.str_result_1n2,
        f.str_formation, f.int_gf, f.int_ga, f.int_poss,

        -- FBref
        f.int_shots_on_target_faced, f.int_saves, f.dec_save_pct, f.int_clean_sheet,
        f.int_shots_total, f.int_shots_on_target, f.dec_goals_per_shot,
        f.int_yellow_cards, f.int_second_yellow_cards, f.int_red_cards,
        f.int_fouls_committed, f.int_interceptions, f.int_tackles_won,

        -- Understat
        f.dec_np_xg, f.dec_np_xg_conceded, f.dec_ppda, f.dec_ppda_allowed, f.dec_np_xg_diff_match,

        -- Cotes Pinnacle / marché, pivotées par venue
        CASE WHEN f.str_venue = 'Home' THEN o.odds_pinnacle_h ELSE o.odds_pinnacle_a END AS dec_odds_pinnacle_team,
        o.odds_pinnacle_d                                                                 AS dec_odds_pinnacle_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.odds_pinnacle_a ELSE o.odds_pinnacle_h END AS dec_odds_pinnacle_opp,
        CASE WHEN f.str_venue = 'Home' THEN o.odds_avg_h      ELSE o.odds_avg_a      END AS dec_odds_avg_team,
        o.odds_avg_d                                                                      AS dec_odds_avg_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.odds_avg_a      ELSE o.odds_avg_h      END AS dec_odds_avg_opp,
        CASE WHEN f.str_venue = 'Home' THEN o.pinnacle_prob_h ELSE o.pinnacle_prob_a END AS dec_pinnacle_prob_team,
        o.pinnacle_prob_d                                                                 AS dec_pinnacle_prob_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.pinnacle_prob_a ELSE o.pinnacle_prob_h END AS dec_pinnacle_prob_opp,
        CASE WHEN f.str_venue = 'Home' THEN o.market_prob_h   ELSE o.market_prob_a   END AS dec_market_prob_team,
        o.market_prob_d                                                                   AS dec_market_prob_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.market_prob_a   ELSE o.market_prob_h   END AS dec_market_prob_opp,

        -- Cotes de clôture (Pinnacle) — probas no-vig, pivotées par venue
        CASE WHEN f.str_venue = 'Home' THEN o.pinnacle_prob_close_h ELSE o.pinnacle_prob_close_a END AS dec_pinnacle_prob_close_team,
        o.pinnacle_prob_close_d                                                                       AS dec_pinnacle_prob_close_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.pinnacle_prob_close_a ELSE o.pinnacle_prob_close_h END AS dec_pinnacle_prob_close_opp,
        CASE WHEN f.str_venue = 'Home' THEN o.market_prob_close_h   ELSE o.market_prob_close_a   END AS dec_market_prob_close_team,
        o.market_prob_close_d                                                                         AS dec_market_prob_close_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.market_prob_close_a   ELSE o.market_prob_close_h   END AS dec_market_prob_close_opp,

        -- Drift ouverture→clôture (Pinnacle), pivoté par venue
        CASE WHEN f.str_venue = 'Home' THEN o.pinnacle_drift_h ELSE o.pinnacle_drift_a END AS dec_pinnacle_drift_team,
        o.pinnacle_drift_d                                                                  AS dec_pinnacle_drift_draw,
        CASE WHEN f.str_venue = 'Home' THEN o.pinnacle_drift_a ELSE o.pinnacle_drift_h END AS dec_pinnacle_drift_opp,

        -- Over/Under 2.5 (Pinnacle) — identique pour les deux équipes du match
        o.pinnacle_prob_over25                                                              AS dec_pinnacle_prob_over25,
        o.pinnacle_prob_under25                                                             AS dec_pinnacle_prob_under25,
        o.pinnacle_prob_close_over25                                                        AS dec_pinnacle_prob_close_over25,
        o.pinnacle_prob_close_under25                                                       AS dec_pinnacle_prob_close_under25

    FROM fbref_understat f
    LEFT JOIN odds_base o
        ON  f.str_match_id = o.str_match_id
        AND f.str_team_id  = o.str_team_id
)

SELECT * FROM final
-- Filtre COVID Ligue 1 / Ligue 2 — matchs suspendus
WHERE NOT (
    str_season = '2019-2020'
    AND str_league_source IN ('Ligue 1', 'Ligue 2')
    AND dt_date >= '2020-03-08'
)

{% if is_incremental() %}
AND (str_match_id || '_' || str_team_id) NOT IN (
    SELECT (str_match_id || '_' || str_team_id)
    FROM {{ this }}
)
{% endif %}
)

SELECT * FROM mdl_body
