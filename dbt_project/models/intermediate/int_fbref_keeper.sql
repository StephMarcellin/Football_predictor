{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_fbref_keeper'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'fbref_keeper') }}
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ ref('team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
)

SELECT
    r.match_id,
    tm_team.team_id,
    tm_opp.team_id AS opponent_id,
    s.* EXCLUDE (team, opponent, raw_team, raw_opponent, cs),
    -- Garde-fou clean_sheet : la source silver.fbref_keeper.cs est salie
    -- (106 valeurs = 2, plus des clean sheets étiquetés 0, et 18 « clean sheets »
    -- avec buts encaissés). On dérive cs du champ autoritaire LOCAL ga_keeper
    -- (concorde à 99,9 % avec le ga du schedule). Corrige les ~300 étiquettes fausses.
    CASE WHEN s.ga_keeper = 0 THEN 1 WHEN s.ga_keeper IS NULL THEN NULL ELSE 0 END AS cs
FROM source s

LEFT JOIN team_mapping tm_team ON s.team     = tm_team.club_name
LEFT JOIN team_mapping tm_opp  ON s.opponent = tm_opp.club_name
LEFT JOIN registry r
    ON  s.date          = r.match_date
    AND s.league_source = r.league_source
    AND s.season        = r.season
    AND (
        (s.venue = 'Home' AND tm_team.team_id = r.home_team_id)
        OR
        (s.venue = 'Away' AND tm_team.team_id = r.away_team_id)
        OR
        (s.venue = 'Neutral' AND (tm_team.team_id = r.home_team_id OR tm_team.team_id = r.away_team_id))
    )