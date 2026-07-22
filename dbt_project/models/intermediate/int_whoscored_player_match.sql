{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_player_match'
    )
}}

-- Stats + note WhoScored par joueur par match, avec identités normalisées.
-- Calqué sur int_whoscored_events : on traduit l'id d'équipe WhoScored vers
-- l'id canonique du projet et on récupère le match_id unifié, via le pont
-- int_whoscored_match_index. player_id reste l'id WhoScored (clé vers
-- silver.stg_whoscored_players_ref).

WITH source AS (
    SELECT * FROM {{ source('silver', 'stg_whoscored_player_match') }}
),

-- Pont d'identité : pour chaque ws_match_id, le match_id unifié et la
-- correspondance id WhoScored → id canonique des deux équipes.
match_index AS (
    SELECT
        ws_match_id,
        match_id,
        ws_home_team_id,
        ws_away_team_id,
        team_id     AS home_team_id,   -- id canonique (team_mapping)
        opponent_id AS away_team_id
    FROM {{ ref('int_whoscored_match_index') }}
)

SELECT
    idx.match_id,

    -- Conversion id WhoScored → id canonique selon le côté de l'équipe.
    CASE
        WHEN p.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN p.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,

    -- On garde toutes les colonnes joueur SAUF les clés brutes remplacées.
    p.* EXCLUDE (ws_match_id, team_id)
FROM source p
LEFT JOIN match_index idx ON p.ws_match_id = idx.ws_match_id
