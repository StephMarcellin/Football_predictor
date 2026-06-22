{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_possession_chains'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
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
-- CERTAIN_POSSESSION
-- Pour chaque événement, on détermine si la possession est certaine ou non.
-- Possession certaine = on sait avec certitude quelle équipe a le ballon.
-- Possession indéterminée = ballon en jeu libre, duel en cours → NULL.
--
-- Possessions certaines :
--   Pass réussie          → l'équipe qui passe a le ballon
--   TakeOn réussi         → l'attaquant a passé le défenseur
--   Interception réussie  → l'équipe qui intercepte a le ballon
--   BallRecovery réussie  → l'équipe qui récupère a le ballon
--   But                   → fin de séquence claire
--   Save                  → le gardien a le ballon
--   KeeperPickup/Claim    → le gardien a le ballon
--
-- Possessions indéterminées (NULL) :
--   Pass ratée, TakeOn raté, BlockedPass, Aerial, Clearance,
--   Challenge, Tackle, BallTouch raté, tirs ratés → ballon en jeu libre
-- ══════════════════════════════════════════════════════════════════════════════
certain_possession AS (
    SELECT
        match_id,
        team_id,
        player_id,
        event_id,
        row_num,
        expanded_minute,
        second,
        period,
        type_id,
        type_name,
        outcome_id,
        is_shot,
        x,
        y,
        season,
        league_source,
        scraped_at,

        CASE
            -- Contact de possession : le joueur contrôle le ballon
            WHEN type_id IN (1, 3, 13, 14, 15, 16, 42, 61, 12, 52, 11, 41)
                THEN team_id
            -- Récupérations et interceptions réussies
            WHEN type_id IN (8, 49) AND outcome_id = 1
                THEN team_id
            -- Pas une possession
            ELSE NULL
        END AS certain_possessor

    FROM {{ ref('int_event_enriched') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
    AND player_id IS NOT NULL
    AND type_id NOT IN (17, 18, 19, 30, 32, 34, 40)
    -- Card, SubstitutionOff, SubstitutionOn, End, Start, FormationSet, FormationChange
),

-- ══════════════════════════════════════════════════════════════════════════════
-- POSSESSION_GROUPS
-- On crée un compteur qui avance uniquement quand le possesseur certain change.
-- Les événements indéterminés héritent du groupe de la dernière possession certaine.
--
-- Exemple :
--   row  certain_possessor  increment  possession_group
--   1    382                1          1
--   2    382                0          1   ← même équipe, pas d'incrément
--   3    NULL               0          1   ← indéterminé, hérite du groupe 1
--   4    NULL               0          1   ← indéterminé, hérite du groupe 1
--   5    277                1          2   ← nouvelle équipe, incrément
--   6    277                0          2   ← même équipe, pas d'incrément
-- ══════════════════════════════════════════════════════════════════════════════
possession_groups AS (
    SELECT
        *,
        -- Incrément : 1 uniquement quand le possesseur certain change d'équipe
        CASE
            WHEN certain_possessor IS NOT NULL
             AND certain_possessor != LAG(certain_possessor) OVER (
                    PARTITION BY match_id
                    ORDER BY expanded_minute, second, row_num
                 )
            THEN 1
            -- Premier événement certain du match : on démarre le premier groupe
            WHEN certain_possessor IS NOT NULL
             AND LAG(certain_possessor) OVER (
                    PARTITION BY match_id
                    ORDER BY expanded_minute, second, row_num
                 ) IS NULL
            THEN 1
            ELSE 0
        END AS possession_group_increment
    FROM certain_possession
),

-- ══════════════════════════════════════════════════════════════════════════════
-- POSSESSION_RESOLVED
-- Cumul des incréments → numéro de groupe.
-- Dans chaque groupe, on récupère le possesseur via MIN() :
--   un groupe ne contient qu'un seul certain_possessor (par construction),
--   les autres lignes sont NULL → MIN() ignore les NULL et retourne le bon team_id.
-- ══════════════════════════════════════════════════════════════════════════════
-- Étape 1 : calcul du numéro de groupe
-- Étape 1 : numéro de groupe cumulatif
possession_numbered AS (
    SELECT
        *,
        SUM(possession_group_increment) OVER (
            PARTITION BY match_id
            ORDER BY expanded_minute, second, row_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS possession_group
    FROM possession_groups
),

-- Étape 2 : dernier certain_possessor connu AVANT ou SUR la ligne courante
-- On prend le MAX sur une fenêtre bornée à la ligne courante → propagation forward
possession_last_known AS (
    SELECT
        *,
        MAX(certain_possessor) OVER (
            PARTITION BY match_id, possession_group
            ORDER BY expanded_minute, second, row_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS resolved_possessor
    FROM possession_numbered
),

-- Étape 3 : correction du premier événement du match
-- Le premier certain_possessor du match n'est pas une rupture — c'est juste le début
possession_resolved AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_id
            ORDER BY expanded_minute, second, row_num
        ) AS match_row_number
    FROM possession_last_known
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_BOUNDARIES
-- La rupture = le resolved_possessor change entre deux lignes consécutives.
-- chain_number = SUM(is_rupture) cumulatif, décalé pour que la rupture
-- appartienne à la chaîne qu'elle clôt.
-- ══════════════════════════════════════════════════════════════════════════════
chain_boundaries AS (
    SELECT
        *,
        CASE
            -- Pas de rupture sur la toute première ligne du match
            WHEN match_row_number = 1
                THEN 0
            WHEN LEAD(resolved_possessor) OVER (
                    PARTITION BY match_id
                    ORDER BY expanded_minute, second, row_num
                ) IS DISTINCT FROM resolved_possessor
                THEN 1
            ELSE 0
        END AS is_rupture
    FROM possession_resolved
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- Une ligne par action avec son chain_id.
-- chain_team_id = resolved_possessor de la chaîne (équipe en possession).
-- Les événements à possession indéterminée ont resolved_possessor propagé
-- depuis la dernière possession certaine.
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    match_id,
    season,
    league_source,

    -- Identifiant global de la chaîne
    match_id || '_' || CAST(
        SUM(is_rupture) OVER (
            PARTITION BY match_id
            ORDER BY expanded_minute, second, row_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) - is_rupture
    AS VARCHAR)                                             AS chain_id,

    -- Numéro local de chaîne dans le match
    SUM(is_rupture) OVER (
        PARTITION BY match_id
        ORDER BY expanded_minute, second, row_num
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) - is_rupture                                          AS chain_number,

    -- Équipe en possession durant cette chaîne
    resolved_possessor                                      AS chain_team_id,

    team_id,
    player_id,
    event_id,
    row_num,
    expanded_minute,
    second,
    period,
    type_id,
    type_name,
    outcome_id,
    is_shot,
    x,
    y,
    is_rupture,
    certain_possessor,
    scraped_at

FROM chain_boundaries
ORDER BY match_id, expanded_minute, second, row_num