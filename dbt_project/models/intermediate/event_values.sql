{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='event_values'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- On ne traite que les événements pas encore présents dans event_values.
-- Le filtre porte sur int_event_enriched qui est déjà matérialisé.
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_event_enriched') }}
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- SOURCE
-- Lecture depuis int_event_enriched déjà matérialisé.
-- Toutes les colonnes dont les axes ont besoin sont déjà présentes.
-- ══════════════════════════════════════════════════════════════════════════════
source AS (
    SELECT *
    FROM {{ ref('int_event_enriched') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- AXE 1 : danger_position
-- Où sur le terrain s'est passée l'action ?
-- Offensif (passes, tirs, dribbles) : x/100
--   → plus on est proche du but adverse, plus c'est dangereux
-- Défensif (tacles, interceptions, etc.) : (100-x)/100
--   → un tacle à x=20 (dans sa propre surface) vaut plus qu'un tacle à x=80
-- Duel pur (Aerial, Foul, Dispossessed) : routé via qual 285/286
--   car ces types peuvent être offensifs ou défensifs selon le contexte
-- NULL si x est manquant
-- ══════════════════════════════════════════════════════════════════════════════
axis_danger AS (
    SELECT
        match_id,
        row_num,
        CASE
            WHEN type_id IN (1, 3, 13, 14, 15, 16, 42)
                THEN x / 100.0
            WHEN type_id IN (7, 8, 12, 45, 49, 74)
                THEN (100.0 - x) / 100.0
            WHEN type_id IN (44, 4, 50) AND has_defensive_qual = 1
                THEN (100.0 - x) / 100.0
            WHEN type_id IN (44, 4, 50) AND has_offensive_qual = 1
                THEN x / 100.0
            ELSE NULL
        END AS danger_position
    FROM source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- AXE 2 : chance_creation
-- L'action a-t-elle produit une opportunité mesurable ?
-- Hiérarchie décroissante : on prend le signal le plus fort présent.
-- Les tirs sont scorés via type_id (pas de qualificateur dédié pour eux).
-- TakeOn et GoodSkill ont un score de base même sans qualificateur :
--   un dribble réussi en zone dangereuse doit valoir quelque chose.
-- ══════════════════════════════════════════════════════════════════════════════
axis_chance AS (
    SELECT
        match_id,
        row_num,
        CASE
            WHEN type_id = 16                         THEN 1.00  -- But
            WHEN is_leading_to_goal         = 1       THEN 0.90  -- Action menant au but
            WHEN is_intentional_goal_assist  = 1      THEN 0.85  -- Assist intentionnel
            WHEN is_intentional_assist      = 1       THEN 0.80  -- Assist (définition large)
            WHEN is_big_chance_created      = 1       THEN 0.75  -- Grande occasion créée
            WHEN is_key_pass                = 1       THEN 0.65  -- Passe clé
            WHEN is_shot_assist             = 1       THEN 0.55  -- Passe menant au tir
            WHEN is_leading_to_attempt      = 1       THEN 0.50  -- Action menant à un tir
            WHEN type_id = 15                         THEN 0.40  -- Tir cadré (SavedShot)
            WHEN type_id IN (13, 14)                  THEN 0.10  -- Tir raté ou sur poteau
            WHEN type_id = 3 AND outcome_id = 1       THEN 0.30  -- TakeOn réussi
            WHEN type_id = 42                         THEN 0.25  -- GoodSkill
            WHEN type_id = 3 AND outcome_id != 1      THEN 0.05  -- TakeOn raté
            ELSE                                           0.00
        END AS chance_creation
    FROM source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- AXE 3 : def_execution_quality
-- L'action défensive a-t-elle été bien exécutée ?
-- Substitut de chance_creation pour le côté défensif :
--   un tacle réussi = 1.0, un challenge = 0.6 (effort sans récupération),
--   une tentative ratée = 0.3.
-- NULL pour toutes les actions offensives — cet axe ne s'applique pas.
-- ══════════════════════════════════════════════════════════════════════════════
axis_def_exec AS (
    SELECT
        match_id,
        row_num,
        CASE
            -- Actions défensives réussies
            WHEN type_id IN (7, 8, 12, 49, 74) AND outcome_id = 1
                THEN 1.00
            -- Challenge : pressing actif même sans récupération du ballon
            WHEN type_id = 45
                THEN 0.60
            -- Actions défensives ratées : effort comptabilisé mais réduit
            WHEN type_id IN (7, 8, 12, 49, 74) AND outcome_id != 1
                THEN 0.30
            -- Duels purs côté défensif (routage via qual 285)
            WHEN type_id IN (44, 4, 50) AND has_defensive_qual = 1 AND outcome_id = 1
                THEN 1.00
            WHEN type_id IN (44, 4, 50) AND has_defensive_qual = 1 AND outcome_id != 1
                THEN 0.30
            -- Actions offensives : axe non applicable
            ELSE NULL
        END AS def_execution_quality
    FROM source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- AXE 4 : pressure_context
-- L'action s'est-elle produite dans un contexte de duel physique ?
-- Proxy limité par l'absence de tracking data : on détecte si un duel était
-- engagé au même instant via OppositeRelatedEvent (qual 233).
-- Bonus de +0.20 si l'action se passe dans sa propre moitié (x < 50) :
--   agir sous pression dans sa surface est plus stressant qu'en terrain adverse.
-- Valeur plafonnée à 1.0 via LEAST.
-- ══════════════════════════════════════════════════════════════════════════════
axis_pressure AS (
    SELECT
        match_id,
        row_num,
        CASE
            -- L'action EST un duel : pression maximale par définition
            WHEN type_id IN (44, 4, 45, 50, 7)
                THEN LEAST(1.0,
                        0.80
                        + CASE WHEN x IS NOT NULL AND x < 50 THEN 0.20 ELSE 0.0 END
                     )
            -- L'action est liée à un duel via OppositeRelatedEvent
            WHEN has_opposite_event = 1
                THEN LEAST(1.0,
                        0.60
                        + CASE WHEN x IS NOT NULL AND x < 50 THEN 0.20 ELSE 0.0 END
                     )
            -- Aucun signal de duel : seul le bonus de zone s'applique
            ELSE
                CASE WHEN x IS NOT NULL AND x < 50 THEN 0.20 ELSE 0.0 END
        END AS pressure_context
    FROM source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- AXE 5 : context_weight
-- Criticité narrative de l'action selon le score state et la période.
-- Plus le contexte est tendu (défaite tardive, égalité tardive), plus le poids
-- est élevé. Valeurs calibrées manuellement — pas de ML ici.
-- Reconstruit le score state depuis team_score / opp_score / expanded_minute.
-- ══════════════════════════════════════════════════════════════════════════════
axis_context AS (
    SELECT
        match_id,
        row_num,
        CASE
            WHEN expanded_minute >= 75 AND team_score < opp_score               THEN 1.00  -- losing_late
            WHEN expanded_minute >= 75 AND team_score > opp_score               THEN 0.85  -- winning_late
            WHEN expanded_minute >= 75 AND team_score = opp_score AND team_score > 0
                                                                                THEN 0.75  -- drawing_late
            WHEN expanded_minute >= 75 AND team_score = 0 AND opp_score = 0    THEN 0.60  -- blank_late
            WHEN team_score < opp_score                                         THEN 0.70  -- losing
            WHEN team_score = opp_score AND team_score > 0                      THEN 0.50  -- drawing
            WHEN team_score > opp_score                                         THEN 0.40  -- winning
            ELSE                                                                     0.30  -- blank (0-0, < 75')
        END AS context_weight
    FROM source
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- Join de tous les axes sur (match_id, row_num).
-- Calcul de action_value : combine les axes selon le type d'action.
--
-- Offensif : sqrt(danger × chance_creation) × (0.5 + 0.3×pressure + 0.2×context)
--   → moyenne géométrique : si danger OU chance_creation = 0, action_value s'effondre
--   → cohérent football : une passe clé dans sa propre surface vaut peu
--
-- Défensif : danger × def_execution_quality × (0.5 + 0.3×pressure + 0.2×context)
--   → produit direct : un tacle raté loin du but vaut très peu
--
-- NULL si x manquant (danger_position non calculable)
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    s.match_id,
    s.team_id,
    s.player_id,
    s.event_id,
    s.row_num,
    s.expanded_minute,
    s.period,
    s.type_id,
    s.type_name,
    s.outcome_id,
    s.is_shot,
    s.x,
    s.y,
    s.match_date,
    s.season,
    s.league_source,
    s.scraped_at,

    -- Les 5 axes exposés séparément pour debug et pour player_history
    ad.danger_position,
    ac.chance_creation,
    ade.def_execution_quality,
    ap.pressure_context,
    act.context_weight,

    -- action_value : note globale combinant les axes
    CASE
        -- Actions offensives
        WHEN s.type_id IN (1, 3, 13, 14, 15, 16, 42)
            THEN SQRT(ad.danger_position * ac.chance_creation)
                 * (0.5 + 0.3 * ap.pressure_context + 0.2 * act.context_weight)

        -- Actions défensives
        WHEN s.type_id IN (7, 8, 12, 45, 49, 74)
            THEN ad.danger_position
                 * ade.def_execution_quality
                 * (0.5 + 0.3 * ap.pressure_context + 0.2 * act.context_weight)

        -- Duels purs défensifs (routage via qual 285)
        WHEN s.type_id IN (44, 4, 50) AND s.has_defensive_qual = 1
            THEN ad.danger_position
                 * ade.def_execution_quality
                 * (0.5 + 0.3 * ap.pressure_context + 0.2 * act.context_weight)

        -- Duels purs offensifs (routage via qual 286)
        -- chance_creation = 0.0 pour les duels sans qualificateur → action_value = 0
        -- cas marginal acceptable en V1
        WHEN s.type_id IN (44, 4, 50) AND s.has_offensive_qual = 1
            THEN SQRT(ad.danger_position * ac.chance_creation)
                 * (0.5 + 0.3 * ap.pressure_context + 0.2 * act.context_weight)

        ELSE NULL
    END                                                             AS action_value

FROM source s

LEFT JOIN axis_danger   ad  ON ad.match_id  = s.match_id AND ad.row_num  = s.row_num
LEFT JOIN axis_chance   ac  ON ac.match_id  = s.match_id AND ac.row_num  = s.row_num
LEFT JOIN axis_def_exec ade ON ade.match_id = s.match_id AND ade.row_num = s.row_num
LEFT JOIN axis_pressure ap  ON ap.match_id  = s.match_id AND ap.row_num  = s.row_num
LEFT JOIN axis_context  act ON act.match_id = s.match_id AND act.row_num = s.row_num