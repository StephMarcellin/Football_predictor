{{ config(materialized='table', schema='gold') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- equipe_gardien_match — grain (match_id, team_id).
-- Profil de saison décalé du gardien titulaire (famille 9), rattaché à l'équipe.
-- Refonte nommage : clés renommées selon docs/proposition_nommage_definitif.csv
-- (str_match_id, str_team_id) ; les 4 features keeper_* ne sont pas dans le CSV
-- et gardent leur nom.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- gardien_saison lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_gardien_saison AS (
    SELECT
        CAST(str_keeper_id AS BIGINT)                                AS "keeper_id",
        str_season                                                   AS "season",
        dec_keeper_psxg_plus_minus_lag                               AS "keeper_psxg_plus_minus_lag",
        dec_keeper_psxg_per_shot_lag                                 AS "keeper_psxg_per_shot_lag",
        dec_keeper_save_pct_lag                                      AS "keeper_save_pct_lag",
        CAST(int_keeper_shots_faced_lag AS HUGEINT)                  AS "keeper_shots_faced_lag",
        str_profile_confidence_flag                                  AS "profile_confidence_flag"
    FROM {{ ref('gardien_saison') }}
),

-- int_whoscored_player_match lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_player_match AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        int_shirt_no                                                 AS "shirt_no",
        str_position                                                 AS "position",
        bool_is_first_eleven                                         AS "is_first_eleven",
        bool_is_man_of_the_match                                     AS "is_man_of_the_match",
        int_height                                                   AS "height",
        int_weight                                                   AS "weight",
        int_age                                                      AS "age",
        dec_rating                                                   AS "rating",
        str_stats_json                                               AS "stats_json",
        CAST(int_touches AS DOUBLE)                                  AS "touches",
        CAST(int_possession AS DOUBLE)                               AS "possession",
        CAST(int_passes_total AS DOUBLE)                             AS "passes_total",
        CAST(int_passes_accurate AS DOUBLE)                          AS "passes_accurate",
        CAST(int_passes_key AS DOUBLE)                               AS "passes_key",
        CAST(int_shots_total AS DOUBLE)                              AS "shots_total",
        CAST(int_shots_on_target AS DOUBLE)                          AS "shots_on_target",
        CAST(int_shots_off_target AS DOUBLE)                         AS "shots_off_target",
        CAST(int_shots_blocked AS DOUBLE)                            AS "shots_blocked",
        CAST(int_shots_on_post AS DOUBLE)                            AS "shots_on_post",
        CAST(int_dribbles_attempted AS DOUBLE)                       AS "dribbles_attempted",
        CAST(int_dribbles_won AS DOUBLE)                             AS "dribbles_won",
        CAST(int_dribbles_lost AS DOUBLE)                            AS "dribbles_lost",
        CAST(int_dribbled_past AS DOUBLE)                            AS "dribbled_past",
        CAST(int_dispossessed AS DOUBLE)                             AS "dispossessed",
        CAST(int_tackles_total AS DOUBLE)                            AS "tackles_total",
        CAST(int_tackle_successful AS DOUBLE)                        AS "tackle_successful",
        CAST(int_tackle_unsuccesful AS DOUBLE)                       AS "tackle_unsuccesful",
        CAST(int_interceptions AS DOUBLE)                            AS "interceptions",
        CAST(int_clearances AS DOUBLE)                               AS "clearances",
        CAST(int_aerials_total AS DOUBLE)                            AS "aerials_total",
        CAST(int_aerials_won AS DOUBLE)                              AS "aerials_won",
        CAST(int_offensive_aerials AS DOUBLE)                        AS "offensive_aerials",
        CAST(int_defensive_aerials AS DOUBLE)                        AS "defensive_aerials",
        CAST(int_fouls_commited AS DOUBLE)                           AS "fouls_commited",
        CAST(int_offsides_caught AS DOUBLE)                          AS "offsides_caught",
        CAST(int_errors AS DOUBLE)                                   AS "errors",
        CAST(int_corners_total AS DOUBLE)                            AS "corners_total",
        CAST(int_corners_accurate AS DOUBLE)                         AS "corners_accurate",
        CAST(int_throw_ins_total AS DOUBLE)                          AS "throw_ins_total",
        CAST(int_throw_ins_accurate AS DOUBLE)                       AS "throw_ins_accurate",
        CAST(int_total_saves AS DOUBLE)                              AS "total_saves",
        CAST(int_parried_safe AS DOUBLE)                             AS "parried_safe",
        CAST(int_parried_danger AS DOUBLE)                           AS "parried_danger",
        CAST(int_claims_high AS DOUBLE)                              AS "claims_high",
        CAST(int_collected AS DOUBLE)                                AS "collected"
    FROM {{ ref('int_whoscored_player_match') }}
),

mdl_body AS (
with keeper_starter as (
    select match_id, team_id, player_id
    from in_int_whoscored_player_match
    where position = 'GK' and is_first_eleven
    qualify row_number() over (partition by match_id, team_id order by player_id) = 1
),

backbone_in as (
    select
        str_match_id                   as match_id,
        cast(str_team_id as bigint)   as team_id,
        str_season                     as season
    from {{ ref('backbone') }}
)

select
    ks.match_id                    as str_match_id,
    cast(ks.team_id as varchar)    as str_team_id,
    gs.keeper_psxg_plus_minus_lag,
    gs.keeper_psxg_per_shot_lag,
    gs.keeper_save_pct_lag,
    gs.keeper_shots_faced_lag
from keeper_starter ks
left join backbone_in b using (match_id, team_id)
left join in_gardien_saison gs
    on gs.keeper_id = ks.player_id and gs.season = b.season
),

mdl_out AS (
    SELECT
        "str_match_id"                                               AS str_match_id,
        "str_team_id"                                                AS str_team_id,
        "keeper_psxg_plus_minus_lag"                                 AS dec_keeper_psxg_plus_minus_lag,
        "keeper_psxg_per_shot_lag"                                   AS dec_keeper_psxg_per_shot_lag,
        "keeper_save_pct_lag"                                        AS dec_keeper_save_pct_lag,
        CAST(keeper_shots_faced_lag AS BIGINT)                       AS int_keeper_shots_faced_lag
    FROM mdl_body
)

SELECT * FROM mdl_out
