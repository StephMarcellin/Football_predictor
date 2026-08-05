{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_keeper_shots'
    )
}}

-- Attribution du gardien à chaque tir cadré subi — brique de int_keeper_psxg.
-- Grain : un tir cadré (SavedShot + Goal, hors CSC/penalty), avec son xGOT et le
-- gardien qui l'a subi.
--
-- Deux voies d'attribution (vérifiées sur le corpus) :
--   • tirs arrêtés (15) : gardien = Save miroir, relié au tir par le QUALIFIER 233
--     (related_event_id est NULL — le vrai lien passe par qual 233, comme int_penalties).
--     Save.player_id = GK lineup à 99,995 % ; les écarts = changements de gardien, où le
--     Save est plus juste. → voie PRIMAIRE.
--   • buts (16) : pas de Save miroir → gardien = GK (slot=1) de l'équipe qui défend,
--     période de formation active (start_minute max <= minute du tir). Sert aussi de
--     filet pour un arrêt sans Save.
--
-- keeper_id = COALESCE(save, lineup). Les ~2 % sans gardien (lineup manquant :
-- D2/Minor Club sous-couverts) → keeper_id NULL, exclus de l'agrégat PSxG.

WITH shots AS (
    SELECT
        p.match_id,
        p.row_num,
        p.season,
        p.is_goal,
        p.xgot,
        sp.team_id          AS att_team,        -- équipe qui tire
        sp.expanded_minute,
        sp.type_id,
        sp.league_source
    FROM {{ source('machine_learning', 'xgot_predictions') }} p
    JOIN {{ ref('int_shot_placement') }} sp
        ON sp.match_id = p.match_id AND sp.row_num = p.row_num
),

-- Les deux équipes canoniques du match → l'équipe qui défend = l'autre.
match_teams AS (
    SELECT match_id, team_id, opponent_id
    FROM {{ ref('int_whoscored_match_index') }}
),
with_def AS (
    SELECT
        s.*,
        CASE
            WHEN s.att_team = mt.team_id     THEN mt.opponent_id
            WHEN s.att_team = mt.opponent_id THEN mt.team_id
        END AS def_team
    FROM shots s
    LEFT JOIN match_teams mt USING (match_id)
),

-- ── Voie 1 : Save miroir via qualifier 233 (tirs arrêtés) ─────────────────────
q233 AS (
    SELECT match_id, row_num, TRY_CAST(qual_value AS INTEGER) AS save_event_id
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id = 233
),
saves AS (
    SELECT match_id, team_id AS def_team, event_id, player_id AS save_player
    FROM {{ ref('int_whoscored_events') }}
    WHERE type_id = 10
),
with_save AS (
    SELECT
        w.*,
        sv.save_player
    FROM with_def w
    LEFT JOIN q233 q
        ON q.match_id = w.match_id AND q.row_num = w.row_num
    LEFT JOIN saves sv
        ON  sv.match_id = w.match_id
        AND sv.event_id = q.save_event_id
        AND sv.def_team = w.def_team
),

-- ── Voie 2 : GK (slot=1) de l'équipe qui défend, période de formation active ───
gk_periods AS (
    SELECT match_id, team_id, start_minute, player_id AS gk_player
    FROM {{ ref('int_whoscored_lineup') }}
    WHERE slot = 1
),
with_lineup AS (
    SELECT
        ws.*,
        gp.gk_player
    FROM with_save ws
    LEFT JOIN gk_periods gp
        ON  gp.match_id     = ws.match_id
        AND gp.team_id      = ws.def_team
        AND gp.start_minute <= ws.expanded_minute
    -- garde la période la plus récente débutée avant le tir (gère le temps additionnel
    -- et les changements de formation/gardien) ; garantit aussi 1 ligne par tir.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ws.match_id, ws.row_num
        ORDER BY gp.start_minute DESC
    ) = 1
),

-- Couverture : l'équipe qui défend a-t-elle un GK lineup dans ce match ?
-- Sinon ses buts ne sont pas attribuables → les tirs de ce match sont non fiables
-- pour le PSxG (arrêts crédités sans le risque de but correspondant). Sert de
-- garde-fou symétrique en aval (int_keeper_psxg).
def_has_gk AS (
    SELECT DISTINCT match_id, team_id
    FROM gk_periods
)

SELECT
    wl.match_id,
    wl.row_num,
    wl.season,
    wl.league_source,
    wl.type_id,
    wl.is_goal,
    wl.xgot,
    wl.def_team,
    COALESCE(wl.save_player, wl.gk_player) AS keeper_id,
    CASE
        WHEN wl.save_player IS NOT NULL THEN 'save'
        WHEN wl.gk_player   IS NOT NULL THEN 'lineup'
        ELSE 'none'
    END AS attribution_method,
    -- TRUE si les buts sont attribuables dans ce match (buts + arrêts symétriques)
    (dg.match_id IS NOT NULL) AS def_gk_available
FROM with_lineup wl
LEFT JOIN def_has_gk dg
    ON dg.match_id = wl.match_id AND dg.team_id = wl.def_team
