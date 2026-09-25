{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_player_minutes'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- int_player_minutes — grain (match_id, team_id, player_id)
-- Minutes jouées par joueur et par match, dérivées de la timeline de formation
-- WhoScored (int_whoscored_lineup). Base du per-90 en couche Gold.
--
-- Méthode : les minutes WhoScored sont "expanded" (absolues, temps additionnel
-- inclus, la 2e mi-temps démarre après le temps additionnel de la 1re) → on
-- somme simplement les durées des segments, SANS recoller les mi-temps ni
-- utiliser `period`. Confirmé empiriquement : titulaires à ~93 min médian.
--
-- Aucun garde-fou de correction : on fait confiance à la source (les erreurs
-- end < start et sentinelles 32767 sont corrigées en amont, côté silver). La
-- justesse se vérifie par TEST (accepted_range dans schema.yml), pas par un
-- plafond qui masquerait un problème.
--
-- Le seul filtre est un filtre de PÉRIMÈTRE, pas de correction : match_id NULL =
-- matchs orphelins D2 sans id unifié, non joignables en aval. À terme il vivra
-- en amont dans int_whoscored_lineup (filtre d'orphelins) et disparaîtra d'ici.
-- ══════════════════════════════════════════════════════════════════════════════

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
SELECT
    match_id,
    team_id,
    player_id,
    SUM(end_minute - start_minute) AS minutes_played
FROM in_int_whoscored_lineup
WHERE match_id IS NOT NULL
GROUP BY match_id, team_id, player_id
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(minutes_played AS BIGINT)                               AS int_minutes_played
    FROM mdl_body
)

SELECT * FROM mdl_out
