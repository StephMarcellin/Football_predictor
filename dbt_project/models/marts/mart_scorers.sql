{{ config(materialized='table', schema='marts') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- mart_scorers — grain (match_id, team_id, player_id) — cible scored (a marqué ≥1).
-- Population : joueur_match (finition). Enrichi de :
--   • profil de création + exposition (joueur_saison)
--   • contexte offensif de l'équipe + défensif de l'adversaire
--     (equipe_match, fenêtres rolling 3/5/10, anti-leakage)
--   • priors de qualité de saison PRÉCÉDENTE (equipe_match, _lag)
-- Label dérivé des events (is_goal, hors csc). Pure sélection (aucun calcul).
-- ══════════════════════════════════════════════════════════════════════════════

-- Refonte nommage : joueur_match et equipe_match sont lus sous leurs nouveaux noms
-- et remappés vers les anciens (CTE joueur_match_in, equipe_match_in) — les sorties
-- de ce mart ne changent pas.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        -- qualifiers_json non lu : absent de la sortie Spark (retiré par spark_events.py)
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('int_whoscored_events') }}
),

-- joueur_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_joueur_match AS (
    SELECT
        str_match_id                                                 AS "str_match_id",
        str_team_id                                                  AS "str_team_id",
        str_opponent_id                                              AS "str_opponent_id",
        str_player_id                                                AS "str_player_id",
        int_is_home                                                  AS "int_is_home",
        dec_scorer_xg_per90_lag                                      AS "dec_scorer_xg_per90_lag",
        dec_scorer_shots_per90_lag                                   AS "dec_scorer_shots_per90_lag",
        dec_scorer_team_shot_share_lag                               AS "dec_scorer_team_shot_share_lag",
        CAST(int_scorer_penalty_taker_lag AS HUGEINT)                AS "int_scorer_penalty_taker_lag",
        CAST(int_scorer_freekick_taker_lag AS HUGEINT)               AS "int_scorer_freekick_taker_lag",
        dec_off_xg_per_shot_lag                                      AS "dec_off_xg_per_shot_lag",
        dec_scorer_context_vs_opponent_style                         AS "dec_scorer_context_vs_opponent_style"
    FROM {{ ref('joueur_match') }}
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
with joueur_match_in as (
    select
        str_match_id                              as match_id,
        cast(str_team_id as bigint)              as team_id,
        cast(str_opponent_id as bigint)          as opponent_id,
        cast(str_player_id as bigint)             as player_id,
        int_is_home                               as is_home,
        dec_scorer_xg_per90_lag                   as scorer_xg_per90_lag,
        dec_scorer_shots_per90_lag                as scorer_shots_per90_lag,
        dec_scorer_team_shot_share_lag            as scorer_team_shot_share_lag,
        int_scorer_penalty_taker_lag              as scorer_penalty_taker_lag,
        int_scorer_freekick_taker_lag             as scorer_freekick_taker_lag,
        dec_off_xg_per_shot_lag                   as off_xg_per_shot_lag,
        dec_scorer_context_vs_opponent_style      as scorer_context_vs_opponent_style
    from in_joueur_match
),

equipe_match_in as (
    select
        str_match_id                              as match_id,
        cast(str_team_id as bigint)              as team_id,
        str_season                                as season,
        {% for w in [3, 5, 10] %}
        dec_avg_np_xg_rolling_{{ w }}             as avg_np_xg_rolling_{{ w }},
        dec_failed_to_score_rate_rolling_{{ w }}  as failed_to_score_rate_rolling_{{ w }},
        dec_win_rate_rolling_{{ w }}              as win_rate_rolling_{{ w }},
        dec_avg_np_xg_conceded_rolling_{{ w }}    as avg_np_xg_conceded_rolling_{{ w }},
        dec_clean_sheet_rate_rolling_{{ w }}      as clean_sheet_rate_rolling_{{ w }},
        {% endfor %}
        dec_season_xg_per_shot_for_lag            as season_xg_per_shot_for_lag,
        dec_season_xg_per_shot_against_lag        as season_xg_per_shot_against_lag
    from {{ ref('equipe_match') }}
),

goals as (
    select match_id, player_id, 1 as scored
    from in_int_whoscored_events
    where is_goal and not is_own_goal
    group by 1, 2
)

select
    jm.*,
    em.season,   -- requis par le split temporel (retiré des features par prepare_x)

    -- ── Profil de création + exposition (joueur_saison) ──────────────────────
    js.off_chances_created_per90_lag,
    js.off_key_passes_per90_lag,
    js.off_xgchain_per90_lag,
    js.off_xgbuildup_per90_lag,
    js.scorer_xgot_overperformance_lag,
    js.n_apps_lag,
    js.minutes_lag,
    js.profile_confidence_flag,

    -- ── Contexte offensif de l'ÉQUIPE (equipe_match, rolling anti-leakage) ────
    em.avg_np_xg_rolling_3,  em.avg_np_xg_rolling_5,  em.avg_np_xg_rolling_10,
    em.failed_to_score_rate_rolling_3, em.failed_to_score_rate_rolling_5, em.failed_to_score_rate_rolling_10,
    em.win_rate_rolling_3,   em.win_rate_rolling_5,   em.win_rate_rolling_10,

    -- ── Contexte défensif de l'ADVERSAIRE (equipe_match sur opponent_id) ──────
    opp.avg_np_xg_conceded_rolling_3  as opp_avg_np_xg_conceded_rolling_3,
    opp.avg_np_xg_conceded_rolling_5  as opp_avg_np_xg_conceded_rolling_5,
    opp.avg_np_xg_conceded_rolling_10 as opp_avg_np_xg_conceded_rolling_10,
    opp.clean_sheet_rate_rolling_3  as opp_clean_sheet_rate_rolling_3,
    opp.clean_sheet_rate_rolling_5  as opp_clean_sheet_rate_rolling_5,
    opp.clean_sheet_rate_rolling_10 as opp_clean_sheet_rate_rolling_10,

    -- ── Priors de qualité de saison PRÉCÉDENTE (equipe_match, _lag) ───────────
    em.season_xg_per_shot_for_lag,
    em.season_xg_per_shot_against_lag,
    opp.season_xg_per_shot_for_lag     as opp_season_xg_per_shot_for_lag,
    opp.season_xg_per_shot_against_lag as opp_season_xg_per_shot_against_lag,

    coalesce(g.scored, 0) as scored
from joueur_match_in jm
left join in_joueur_saison js  using (match_id, team_id, player_id)
left join equipe_match_in  em  on em.match_id  = jm.match_id and em.team_id  = jm.team_id
left join equipe_match_in  opp on opp.match_id = jm.match_id and opp.team_id = jm.opponent_id
left join goals g on g.match_id = jm.match_id and g.player_id = jm.player_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "is_home"                                                    AS int_is_home,
        "scorer_xg_per90_lag"                                        AS dec_scorer_xg_per90_lag,
        "scorer_shots_per90_lag"                                     AS dec_scorer_shots_per90_lag,
        "scorer_team_shot_share_lag"                                 AS dec_scorer_team_shot_share_lag,
        CAST(scorer_penalty_taker_lag AS BIGINT)                     AS int_scorer_penalty_taker_lag,
        CAST(scorer_freekick_taker_lag AS BIGINT)                    AS int_scorer_freekick_taker_lag,
        "off_xg_per_shot_lag"                                        AS dec_off_xg_per_shot_lag,
        "scorer_context_vs_opponent_style"                           AS dec_scorer_context_vs_opponent_style,
        "season"                                                     AS str_season,
        "off_chances_created_per90_lag"                              AS dec_off_chances_created_per90_lag,
        "off_key_passes_per90_lag"                                   AS dec_off_key_passes_per90_lag,
        "off_xgchain_per90_lag"                                      AS dec_off_xgchain_per90_lag,
        "off_xgbuildup_per90_lag"                                    AS dec_off_xgbuildup_per90_lag,
        "scorer_xgot_overperformance_lag"                            AS dec_scorer_xgot_overperformance_lag,
        "n_apps_lag"                                                 AS int_n_apps_lag,
        CAST(minutes_lag AS BIGINT)                                  AS int_minutes_lag,
        "profile_confidence_flag"                                    AS str_profile_confidence_flag,
        "avg_np_xg_rolling_3"                                        AS dec_avg_np_xg_rolling_3,
        "avg_np_xg_rolling_5"                                        AS dec_avg_np_xg_rolling_5,
        "avg_np_xg_rolling_10"                                       AS dec_avg_np_xg_rolling_10,
        "failed_to_score_rate_rolling_3"                             AS dec_failed_to_score_rate_rolling_3,
        "failed_to_score_rate_rolling_5"                             AS dec_failed_to_score_rate_rolling_5,
        "failed_to_score_rate_rolling_10"                            AS dec_failed_to_score_rate_rolling_10,
        "win_rate_rolling_3"                                         AS dec_win_rate_rolling_3,
        "win_rate_rolling_5"                                         AS dec_win_rate_rolling_5,
        "win_rate_rolling_10"                                        AS dec_win_rate_rolling_10,
        "opp_avg_np_xg_conceded_rolling_3"                           AS dec_opp_avg_np_xg_conceded_rolling_3,
        "opp_avg_np_xg_conceded_rolling_5"                           AS dec_opp_avg_np_xg_conceded_rolling_5,
        "opp_avg_np_xg_conceded_rolling_10"                          AS dec_opp_avg_np_xg_conceded_rolling_10,
        "opp_clean_sheet_rate_rolling_3"                             AS dec_opp_clean_sheet_rate_rolling_3,
        "opp_clean_sheet_rate_rolling_5"                             AS dec_opp_clean_sheet_rate_rolling_5,
        "opp_clean_sheet_rate_rolling_10"                            AS dec_opp_clean_sheet_rate_rolling_10,
        "season_xg_per_shot_for_lag"                                 AS dec_season_xg_per_shot_for_lag,
        "season_xg_per_shot_against_lag"                             AS dec_season_xg_per_shot_against_lag,
        "opp_season_xg_per_shot_for_lag"                             AS dec_opp_season_xg_per_shot_for_lag,
        "opp_season_xg_per_shot_against_lag"                         AS dec_opp_season_xg_per_shot_against_lag,
        "scored"                                                     AS int_scored
    FROM mdl_body
)

SELECT * FROM mdl_out
