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

SELECT
    match_id,
    team_id,
    player_id,
    SUM(end_minute - start_minute) AS minutes_played
FROM {{ ref('int_whoscored_lineup') }}
WHERE match_id IS NOT NULL
GROUP BY match_id, team_id, player_id
