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
    SELECT
        *,
        -- Parsing UNIQUE du document, en structure native DuckDB.
        -- Double cast obligatoire : VARCHAR → MAP passe par le parseur de
        -- LITTÉRAL MAP de DuckDB (syntaxe {k=v}) et échoue sur du JSON.
        -- VARCHAR → JSON → MAP appelle le parseur JSON, le bon.
        -- Validé : 0 échec sur 1 514 441 lignes.
        CAST(CAST(stats_json AS JSON) AS MAP(VARCHAR, MAP(VARCHAR, DOUBLE)))
            AS stats_map
    FROM {{ source('silver', 'stg_whoscored_player_match') }}
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

-- Stats de comptage à éclater depuis stats_json (SUM des séries {minute: valeur}).
-- On ne stocke QUE des comptes ; les % (passSuccess…) se dérivent en aval.
{% set count_stats = [
    'touches', 'possession', 'passesTotal', 'passesAccurate', 'passesKey',
    'shotsTotal', 'shotsOnTarget', 'shotsOffTarget', 'shotsBlocked', 'shotsOnPost',
    'dribblesAttempted', 'dribblesWon', 'dribblesLost', 'dribbledPast', 'dispossessed',
    'tacklesTotal', 'tackleSuccessful', 'tackleUnsuccesful', 'interceptions', 'clearances',
    'aerialsTotal', 'aerialsWon', 'offensiveAerials', 'defensiveAerials',
    'foulsCommited', 'offsidesCaught', 'errors',
    'cornersTotal', 'cornersAccurate', 'throwInsTotal', 'throwInsAccurate',
    'totalSaves', 'parriedSafe', 'parriedDanger', 'claimsHigh', 'collected'
] %}

SELECT
    idx.match_id,

    -- Conversion id WhoScored → id canonique selon le côté de l'équipe.
    CASE
        WHEN p.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN p.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,

    -- On garde toutes les colonnes joueur SAUF les clés brutes remplacées.
    -- stats_json reste conservé comme filet de sécurité (ré-extraction possible).
    p.* EXCLUDE (ws_match_id, team_id, stats_map),

    -- Éclatage du stats_json : SUM des séries {minute: valeur} par stat.
    -- Lecture dans stats_map, déjà parsée dans le CTE source : aucun parsing
    -- JSON ici. L'ancienne version appelait json_extract 36 fois par ligne,
    -- soit 36 parsings du même document — cause de l'OOM à 9,3 GiB.
    -- Clé absente (ex. dribbles pour un gardien) → COALESCE 0, pas NULL.
    {% for s in count_stats %}
    COALESCE(list_sum(map_values(p.stats_map['{{ s }}'])), 0)
        AS {{ modules.re.sub('([A-Z])', '_\\1', s) | lower }}{{ "," if not loop.last }}
    {% endfor %}
FROM source p
LEFT JOIN match_index idx ON p.ws_match_id = idx.ws_match_id
