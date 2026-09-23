{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_whoscored_team_match_agg'
    )
}}

-- Agrégation au niveau match des stats minute-par-minute WhoScored.
-- Compteurs bruts : SUM(). Ratios (pass_success, aerial_success, etc.) :
-- recalculés depuis les sommes (accurate/total), pas moyennés depuis le
-- JSON source qui est un cumulatif glissant biaisé (cf. rating, écarté).

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

    FROM {{ ref('int_whoscored_team_match_minute') }}
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