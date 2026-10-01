{{
    config(
        materialized='table',
        schema='referentiel',
        alias='referentiel_teams'
    )
}}

-- Dimension équipe : 1 ligne par team_id canonique.
-- Source : referentiel.team_mapping, au grain ALIAS (une ligne par orthographe
-- connue d'un club : « Ajaccio », « AC Ajaccio »… → même team_id).
-- On la réduit au grain équipe pour pouvoir joindre sur team_id sans
-- multiplier les lignes.
--
-- Si un team_id porte plusieurs club_name, MAX() en garde un seul
-- (choix alphabétique, déterministe). Le test ci-dessous le signale.

WITH

team_mapping AS (
    SELECT
        CAST(team_id AS BIGINT) AS team_id,
        club_name
    FROM {{ source('referentiel', 'team_mapping') }}
    WHERE team_id IS NOT NULL
),

mdl_body AS (
    SELECT
        team_id,
        MAX(club_name)            AS club_name,
    FROM team_mapping
    GROUP BY team_id
),

mdl_out AS (
    SELECT
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "club_name"                                                  AS str_club_name,
    FROM mdl_body
)

SELECT * FROM mdl_out
