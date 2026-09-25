{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_whoscored_team_match_agg'
    )
}}

-- Agrégation au niveau match des stats minute-par-minute WhoScored.
-- Compteurs bruts : SUM(). Ratios (pass_success, aerial_success, etc.) :
-- recalculés depuis les sommes (accurate/total), pas moyennés depuis le
-- JSON source qui est un cumulatif glissant biaisé (cf. rating, écarté).

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_team_match_minute lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_team_match_minute AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_minute                                                   AS "minute",
        dec_rating                                                   AS "rating",
        dec_shots_total                                              AS "shots_total",
        dec_shots_on_target                                          AS "shots_on_target",
        dec_shots_off_target                                         AS "shots_off_target",
        dec_shots_blocked                                            AS "shots_blocked",
        dec_clearances                                               AS "clearances",
        dec_interceptions                                            AS "interceptions",
        dec_possession                                               AS "possession",
        dec_touches                                                  AS "touches",
        dec_passes_total                                             AS "passes_total",
        dec_passes_accurate                                          AS "passes_accurate",
        dec_passes_key                                               AS "passes_key",
        dec_pass_success                                             AS "pass_success",
        dec_aerials_total                                            AS "aerials_total",
        dec_aerials_won                                              AS "aerials_won",
        dec_aerial_success                                           AS "aerial_success",
        dec_corners_total                                            AS "corners_total",
        dec_corners_accurate                                         AS "corners_accurate",
        dec_throw_ins_total                                          AS "throw_ins_total",
        dec_throw_ins_accurate                                       AS "throw_ins_accurate",
        dec_throw_in_accuracy                                        AS "throw_in_accuracy",
        dec_offsides_caught                                          AS "offsides_caught",
        dec_fouls_committed                                          AS "fouls_committed",
        dec_tackles_total                                            AS "tackles_total",
        dec_tackles_successful                                       AS "tackles_successful",
        dec_tackles_unsuccessful                                     AS "tackles_unsuccessful",
        dec_tackle_success                                           AS "tackle_success",
        dec_dribbled_past                                            AS "dribbled_past",
        dec_dribbles_won                                             AS "dribbles_won",
        dec_dribbles_attempted                                       AS "dribbles_attempted",
        dec_dribbles_lost                                            AS "dribbles_lost",
        dec_dribble_success                                          AS "dribble_success",
        dec_dispossessed                                             AS "dispossessed",
        dec_errors                                                   AS "errors",
        dec_defensive_aerials                                        AS "defensive_aerials",
        dec_offensive_aerials                                        AS "offensive_aerials"
    FROM {{ ref('int_whoscored_team_match_minute') }}
),

mdl_body AS (
WITH agg AS (
    SELECT
        match_id,
        team_id,

        SUM(shots_total) AS shots_total,
        SUM(shots_on_target) AS shots_on_target,
        SUM(shots_off_target) AS shots_off_target,
        SUM(shots_blocked) AS shots_blocked,
        SUM(clearances) AS clearances,
        SUM(interceptions) AS interceptions,
        SUM(possession) AS possession,
        SUM(touches) AS touches,
        SUM(passes_total) AS passes_total,
        SUM(passes_accurate) AS passes_accurate,
        SUM(passes_key) AS passes_key,
        SUM(aerials_total) AS aerials_total,
        SUM(aerials_won) AS aerials_won,
        SUM(corners_total) AS corners_total,
        SUM(corners_accurate) AS corners_accurate,
        SUM(throw_ins_total) AS throw_ins_total,
        SUM(throw_ins_accurate) AS throw_ins_accurate,
        SUM(offsides_caught) AS offsides_caught,
        SUM(fouls_committed) AS fouls_committed,
        SUM(tackles_total) AS tackles_total,
        SUM(tackles_successful) AS tackles_successful,
        SUM(tackles_unsuccessful) AS tackles_unsuccessful,
        SUM(dribbled_past) AS dribbled_past,
        SUM(dribbles_won) AS dribbles_won,
        SUM(dribbles_attempted) AS dribbles_attempted,
        SUM(dribbles_lost) AS dribbles_lost,
        SUM(dispossessed) AS dispossessed,
        SUM(errors) AS errors,
        SUM(defensive_aerials) AS defensive_aerials,
        SUM(offensive_aerials) AS offensive_aerials

    FROM in_int_whoscored_team_match_minute
    GROUP BY match_id, team_id
)

SELECT
    match_id,
    team_id,

    -- Compteurs bruts
    shots_total,
    shots_on_target,
    shots_off_target,
    shots_blocked,
    clearances,
    interceptions,
    possession,
    touches,
    passes_total,
    passes_accurate,
    passes_key,
    aerials_total,
    aerials_won,
    corners_total,
    corners_accurate,
    throw_ins_total,
    throw_ins_accurate,
    offsides_caught,
    fouls_committed,
    tackles_total,
    tackles_successful,
    tackles_unsuccessful,
    dribbled_past,
    dribbles_won,
    dribbles_attempted,
    dribbles_lost,
    dispossessed,
    errors,
    defensive_aerials,
    offensive_aerials,

    -- Ratios recalculés (NULL si dénominateur = 0, pas de crash division/0)
    passes_accurate / NULLIF(passes_total, 0) * 100 AS pass_success_pct,
    aerials_won / NULLIF(aerials_total, 0) * 100 AS aerial_success_pct,
    tackles_successful / NULLIF(tackles_total, 0) * 100 AS tackle_success_pct,
    dribbles_won / NULLIF(dribbles_attempted, 0) * 100 AS dribble_success_pct,
    throw_ins_accurate / NULLIF(throw_ins_total, 0) * 100 AS throw_in_accuracy_pct

FROM agg
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "shots_total"                                                AS dec_shots_total,
        "shots_on_target"                                            AS dec_shots_on_target,
        "shots_off_target"                                           AS dec_shots_off_target,
        "shots_blocked"                                              AS dec_shots_blocked,
        "clearances"                                                 AS dec_clearances,
        "interceptions"                                              AS dec_interceptions,
        "possession"                                                 AS dec_possession,
        "touches"                                                    AS dec_touches,
        "passes_total"                                               AS dec_passes_total,
        "passes_accurate"                                            AS dec_passes_accurate,
        "passes_key"                                                 AS dec_passes_key,
        "aerials_total"                                              AS dec_aerials_total,
        "aerials_won"                                                AS dec_aerials_won,
        "corners_total"                                              AS dec_corners_total,
        "corners_accurate"                                           AS dec_corners_accurate,
        "throw_ins_total"                                            AS dec_throw_ins_total,
        "throw_ins_accurate"                                         AS dec_throw_ins_accurate,
        "offsides_caught"                                            AS dec_offsides_caught,
        "fouls_committed"                                            AS dec_fouls_committed,
        "tackles_total"                                              AS dec_tackles_total,
        "tackles_successful"                                         AS dec_tackles_successful,
        "tackles_unsuccessful"                                       AS dec_tackles_unsuccessful,
        "dribbled_past"                                              AS dec_dribbled_past,
        "dribbles_won"                                               AS dec_dribbles_won,
        "dribbles_attempted"                                         AS dec_dribbles_attempted,
        "dribbles_lost"                                              AS dec_dribbles_lost,
        "dispossessed"                                               AS dec_dispossessed,
        "errors"                                                     AS dec_errors,
        "defensive_aerials"                                          AS dec_defensive_aerials,
        "offensive_aerials"                                          AS dec_offensive_aerials,
        "pass_success_pct"                                           AS dec_pass_success_pct,
        "aerial_success_pct"                                         AS dec_aerial_success_pct,
        "tackle_success_pct"                                         AS dec_tackle_success_pct,
        "dribble_success_pct"                                        AS dec_dribble_success_pct,
        "throw_in_accuracy_pct"                                      AS dec_throw_in_accuracy_pct
    FROM mdl_body
)

SELECT * FROM mdl_out
