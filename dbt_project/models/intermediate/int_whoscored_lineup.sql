{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_lineup'
    )
}}

-- Composition positionnelle par période de formation.
-- Grain : (match × équipe × formation_seq × joueur) — une ligne par TITULAIRE
-- (slot 1..11) de chaque période tactique. Les remplaçants (slot 0) sont exclus.
--
-- Éclatage : player_ids, formation_slots et formation_positions sont des tableaux
-- JSON stockés en silver. On les déplie en parallèle par index, puis on récupère
-- la position de chaque joueur via SON slot : position = formation_positions[slot].
-- ATTENTION : on ne peut PAS aligner player_ids[i] avec formation_positions[i]
-- directement — en cas de carton rouge le tableau de slots a un trou (ex. slot 10
-- absent), et l'alignement par index donnerait la mauvaise coordonnée.
--
-- Prérequis : formation_slots doit être rempli en silver (re-load des archives).
-- Normalisation team_id WhoScored → canonique + match_id unifié via le pont,
-- même patron que les autres int_whoscored_*.

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
    SELECT * FROM {{ source('silver', 'stg_whoscored_formations') }}
),

match_index AS (
    SELECT
        ws_match_id,
        match_id,
        ws_home_team_id,
        ws_away_team_id,
        team_id     AS home_team_id,
        opponent_id AS away_team_id
    FROM in_int_whoscored_match_index
),

-- Parse des trois tableaux JSON en listes DuckDB typées.
parsed AS (
    SELECT
        ws_match_id, team_id, formation_seq, formation_id, period,
        start_minute, end_minute, captain_player_id,
        from_json(player_ids,          '["BIGINT"]')  AS pids,
        from_json(formation_slots,     '["INTEGER"]') AS slots,
        from_json(formation_positions, '[{"vertical":"DOUBLE","horizontal":"DOUBLE"}]') AS pos
    FROM source
),

-- Dépliage : une ligne par joueur titulaire, position récupérée par slot.
exploded AS (
    SELECT
        p.ws_match_id,
        p.team_id,
        p.formation_seq,
        p.formation_id,
        p.period,
        p.start_minute,
        -- p.end_minute,
        GREATEST(p.start_minute, IF(p.end_minute > 150, p.start_minute, p.end_minute)) AS end_minute,
        p.pids[i]  AS player_id,
        p.slots[i] AS slot,
        p.pos[p.slots[i]].vertical   AS grid_vertical,
        p.pos[p.slots[i]].horizontal AS grid_horizontal,
        (p.pids[i] = p.captain_player_id) AS is_captain
    FROM parsed p, range(1, len(p.pids) + 1) AS t(i)
    WHERE p.slots[i] > 0          -- titulaires seulement (0 = banc)
)

SELECT
    idx.match_id,

    -- Conversion id WhoScored → id canonique selon le côté de l'équipe.
    CASE
        WHEN x.team_id = idx.ws_home_team_id THEN idx.home_team_id
        WHEN x.team_id = idx.ws_away_team_id THEN idx.away_team_id
        ELSE NULL
    END AS team_id,

    x.* EXCLUDE (ws_match_id, team_id)
FROM exploded x
LEFT JOIN match_index idx ON x.ws_match_id = idx.ws_match_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "formation_seq"                                              AS int_formation_seq,
        CAST(formation_id AS VARCHAR)                                AS str_formation_id,
        "period"                                                     AS int_period,
        "start_minute"                                               AS int_start_minute,
        "end_minute"                                                 AS int_end_minute,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "slot"                                                       AS int_slot,
        "grid_vertical"                                              AS dec_grid_vertical,
        "grid_horizontal"                                            AS dec_grid_horizontal,
        "is_captain"                                                 AS bool_is_captain
    FROM mdl_body
)

SELECT * FROM mdl_out
