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
    FROM {{ ref('int_whoscored_lineup') }}
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
