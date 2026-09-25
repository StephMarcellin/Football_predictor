{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_understat_schedule'
    )
}}

-- Grain : 1 ligne par match (point de vue domicile). str_team_id = équipe à domicile,
-- str_opponent_id = équipe à l'extérieur. Silver déjà typé (BIGINT / DOUBLE / BOOLEAN) :
-- les TRY_CAST alignent sur la convention de nommage et typent scraped_at (VARCHAR).

WITH source AS (
    SELECT * FROM {{ source('silver', 'understat_schedule') }}
),

typed AS (
    SELECT
        TRY_CAST(league_source AS VARCHAR)                    AS str_league_source,
        TRY_CAST(match_id AS VARCHAR)                         AS str_us_match_id,
        TRY_CAST(home_team AS VARCHAR)                        AS str_home_team,
        TRY_CAST(away_team AS VARCHAR)                        AS str_away_team,
        TRY_CAST(home_goals AS INTEGER)                       AS int_home_goals,
        TRY_CAST(away_goals AS INTEGER)                       AS int_away_goals,
        TRY_CAST(home_xg AS DOUBLE)                           AS dec_home_xg,
        TRY_CAST(away_xg AS DOUBLE)                           AS dec_away_xg,
        TRY_CAST(is_result AS BOOLEAN)                        AS bool_is_result,
        TRY_CAST(match_url AS VARCHAR)                        AS str_match_url,
        TRY_CAST(season AS VARCHAR)                           AS str_season,
        TRY_CAST(source AS VARCHAR)                           AS str_source,
        TRY_CAST(NULLIF(TRIM(scraped_at), '') AS TIMESTAMP)   AS dt_scraped_at,
        TRY_CAST(comp_category AS VARCHAR)                    AS str_comp_category,
        TRY_CAST(raw_home_team AS VARCHAR)                    AS str_raw_home_team,
        TRY_CAST(raw_away_team AS VARCHAR)                    AS str_raw_away_team
    FROM source
),

-- Correctif ciblé (seul cas connu) : en Ligue 1 2015-2016, le club corse était le
-- GFC Ajaccio (team_id 3558), mais la normalisation silver d'Understat le mappe sur
-- AC Ajaccio (1147) — team_mapping n'est pas sensible à la saison. Sans ce correctif,
-- 38 matchs ne trouvent pas leur match_id. À déplacer en amont si un 2e cas apparaît.
patched AS (
    SELECT
        * REPLACE (
            CASE
                WHEN str_season = '2015-2016' AND str_league_source = 'Ligue 1'
                 AND str_home_team = 'AC Ajaccio' THEN 'GFC Ajaccio'
                ELSE str_home_team
            END AS str_home_team,
            CASE
                WHEN str_season = '2015-2016' AND str_league_source = 'Ligue 1'
                 AND str_away_team = 'AC Ajaccio' THEN 'GFC Ajaccio'
                ELSE str_away_team
            END AS str_away_team
        )
    FROM typed
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

-- Understat ne fournit pas la date du match : la jointure se fait sur
-- (saison, championnat, domicile, extérieur). Cette paire est unique en championnat,
-- SAUF si le registre contient un match de barrage classé dans le même league_source
-- (Spezia-Verona 2022-2023 : match de saison le 05/03/2023 + barrage de relégation le
-- 11/06/2023 → fan-out +1 ligne). Understat ne couvre que la saison régulière :
-- on retient la première date de la paire (MIN + jointure, pas de ROW_NUMBER sur DATE,
-- cf. bug DuckDB 1.5.1).
registry_first AS (
    SELECT
        season, league_source, home_team_id, away_team_id,
        MIN(match_date) AS first_match_date
    FROM registry
    GROUP BY season, league_source, home_team_id, away_team_id
),

-- 1. Assemblage et résolution des identifiants
enriched AS (
    SELECT
        r.match_id                           AS str_match_id,
        TRY_CAST(tm_team.team_id AS VARCHAR) AS str_team_id,
        TRY_CAST(tm_opp.team_id  AS VARCHAR) AS str_opponent_id,
        s.* EXCLUDE (str_home_team, str_away_team, str_raw_home_team, str_raw_away_team)
    FROM patched s
    LEFT JOIN team_mapping tm_team ON s.str_home_team = tm_team.club_name
    LEFT JOIN team_mapping tm_opp  ON s.str_away_team = tm_opp.club_name
    LEFT JOIN registry_first rf
        ON  rf.league_source = s.str_league_source
        AND rf.season        = s.str_season
        AND rf.home_team_id  = tm_team.team_id
        AND rf.away_team_id  = tm_opp.team_id
    LEFT JOIN registry r
        ON  r.league_source = rf.league_source
        AND r.season        = rf.season
        AND r.home_team_id  = rf.home_team_id
        AND r.away_team_id  = rf.away_team_id
        AND r.match_date    = rf.first_match_date
)

SELECT * FROM enriched