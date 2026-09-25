{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_whoscored_lineup_agg'
    )
}}

-- Features positionnelles au grain joueur×match, condensées depuis
-- int_whoscored_lineup (grain formation-période × joueur).
-- Un joueur peut apparaître sur plusieurs périodes tactiques (changement de
-- formation) : on agrège en pondérant chaque période par sa durée en minutes.
--
-- avg_vertical / avg_horizontal : position moyenne sur la grille (0 = ligne de
-- but propre, 5 = axe). primary_slot : le slot où il a passé le plus de temps.
-- Couverture liée à formation_slots (se remplit avec le load des archives).

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_lineup lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_lineup AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_formation_seq                                            AS "formation_seq",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        int_period                                                   AS "period",
        int_start_minute                                             AS "start_minute",
        int_end_minute                                               AS "end_minute",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        int_slot                                                     AS "slot",
        dec_grid_vertical                                            AS "grid_vertical",
        dec_grid_horizontal                                          AS "grid_horizontal",
        bool_is_captain                                              AS "is_captain"
    FROM {{ ref('int_whoscored_lineup') }}
),

mdl_body AS (
WITH periods AS (
    SELECT
        match_id,
        team_id,
        player_id,
        slot,
        grid_vertical,
        grid_horizontal,
        is_captain,
        -- Durée de la période (minutes). Garde-fou contre les bornes manquantes.
        GREATEST(COALESCE(end_minute, 90) - COALESCE(start_minute, 0), 0) AS dur
    FROM in_int_whoscored_lineup
)

SELECT
    match_id,
    team_id,
    player_id,

    -- Position moyenne pondérée par la durée des périodes
    SUM(grid_vertical   * dur) / NULLIF(SUM(dur), 0) AS avg_vertical,
    SUM(grid_horizontal * dur) / NULLIF(SUM(dur), 0) AS avg_horizontal,

    -- Slot principal (celui où le joueur a passé le plus de minutes)
    arg_max(slot, dur)                               AS primary_slot,

    -- Capitaine sur au moins une période
    bool_or(is_captain)                              AS is_captain,

    -- Nombre de périodes tactiques distinctes du joueur (proxy de repositionnement)
    COUNT(*)                                         AS n_formation_periods

FROM periods
GROUP BY match_id, team_id, player_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "avg_vertical"                                               AS dec_avg_vertical,
        "avg_horizontal"                                             AS dec_avg_horizontal,
        "primary_slot"                                               AS int_primary_slot,
        "is_captain"                                                 AS bool_is_captain,
        "n_formation_periods"                                        AS int_n_formation_periods
    FROM mdl_body
)

SELECT * FROM mdl_out
