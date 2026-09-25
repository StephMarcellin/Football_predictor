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

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_match_index lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_match_index AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_ws_match_id                                              AS "ws_match_id",
        dt_match_date                                                AS "match_date",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
        CAST(str_ws_home_team_id AS INTEGER)                         AS "ws_home_team_id",
        CAST(str_ws_away_team_id AS INTEGER)                         AS "ws_away_team_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        str_comp_category                                            AS "comp_category"
    FROM {{ ref('int_whoscored_match_index') }}
),

mdl_body AS (
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
    FROM in_int_whoscored_match_index
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
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "shirt_no"                                                   AS int_shirt_no,
        "position"                                                   AS str_position,
        "is_first_eleven"                                            AS bool_is_first_eleven,
        "is_man_of_the_match"                                        AS bool_is_man_of_the_match,
        "height"                                                     AS int_height,
        "weight"                                                     AS int_weight,
        "age"                                                        AS int_age,
        "rating"                                                     AS dec_rating,
        "stats_json"                                                 AS str_stats_json,
        CAST(touches AS INTEGER)                                     AS int_touches,
        CAST(possession AS INTEGER)                                  AS int_possession,
        CAST(passes_total AS INTEGER)                                AS int_passes_total,
        CAST(passes_accurate AS INTEGER)                             AS int_passes_accurate,
        CAST(passes_key AS INTEGER)                                  AS int_passes_key,
        CAST(shots_total AS INTEGER)                                 AS int_shots_total,
        CAST(shots_on_target AS INTEGER)                             AS int_shots_on_target,
        CAST(shots_off_target AS INTEGER)                            AS int_shots_off_target,
        CAST(shots_blocked AS INTEGER)                               AS int_shots_blocked,
        CAST(shots_on_post AS INTEGER)                               AS int_shots_on_post,
        CAST(dribbles_attempted AS INTEGER)                          AS int_dribbles_attempted,
        CAST(dribbles_won AS INTEGER)                                AS int_dribbles_won,
        CAST(dribbles_lost AS INTEGER)                               AS int_dribbles_lost,
        CAST(dribbled_past AS INTEGER)                               AS int_dribbled_past,
        CAST(dispossessed AS INTEGER)                                AS int_dispossessed,
        CAST(tackles_total AS INTEGER)                               AS int_tackles_total,
        CAST(tackle_successful AS INTEGER)                           AS int_tackle_successful,
        CAST(tackle_unsuccesful AS INTEGER)                          AS int_tackle_unsuccesful,
        CAST(interceptions AS INTEGER)                               AS int_interceptions,
        CAST(clearances AS INTEGER)                                  AS int_clearances,
        CAST(aerials_total AS INTEGER)                               AS int_aerials_total,
        CAST(aerials_won AS INTEGER)                                 AS int_aerials_won,
        CAST(offensive_aerials AS INTEGER)                           AS int_offensive_aerials,
        CAST(defensive_aerials AS INTEGER)                           AS int_defensive_aerials,
        CAST(fouls_commited AS INTEGER)                              AS int_fouls_commited,
        CAST(offsides_caught AS INTEGER)                             AS int_offsides_caught,
        CAST(errors AS INTEGER)                                      AS int_errors,
        CAST(corners_total AS INTEGER)                               AS int_corners_total,
        CAST(corners_accurate AS INTEGER)                            AS int_corners_accurate,
        CAST(throw_ins_total AS INTEGER)                             AS int_throw_ins_total,
        CAST(throw_ins_accurate AS INTEGER)                          AS int_throw_ins_accurate,
        CAST(total_saves AS INTEGER)                                 AS int_total_saves,
        CAST(parried_safe AS INTEGER)                                AS int_parried_safe,
        CAST(parried_danger AS INTEGER)                              AS int_parried_danger,
        CAST(claims_high AS INTEGER)                                 AS int_claims_high,
        CAST(collected AS INTEGER)                                   AS int_collected
    FROM mdl_body
)

SELECT * FROM mdl_out
