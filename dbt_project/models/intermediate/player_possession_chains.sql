{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_possession_chains'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- events_qual lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_events_qual AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        int_row_num                                                  AS "row_num",
        CAST(str_qual_type_id AS INTEGER)                            AS "qual_type_id",
        str_qual_type_name                                           AS "qual_type_name",
        str_qual_value                                               AS "qual_value"
    FROM {{ ref('events_qual') }}
),

-- int_event_enriched lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_event_enriched AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        bool_is_touch                                                AS "is_touch",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_is_leading_to_goal                                       AS "is_leading_to_goal",
        int_is_intentional_goal_assist                               AS "is_intentional_goal_assist",
        int_is_intentional_assist                                    AS "is_intentional_assist",
        int_is_big_chance_created                                    AS "is_big_chance_created",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        int_is_leading_to_attempt                                    AS "is_leading_to_attempt",
        int_has_defensive_qual                                       AS "has_defensive_qual",
        int_has_offensive_qual                                       AS "has_offensive_qual",
        int_has_opposite_event                                       AS "has_opposite_event",
        int_team_score                                               AS "team_score",
        int_opp_score                                                AS "opp_score"
    FROM {{ ref('int_event_enriched') }}
),

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_event_enriched
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            str_chain_id                                                 AS "chain_id",
            CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
            CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            CAST(str_event_id AS INTEGER)                                AS "event_id",
            int_row_num                                                  AS "row_num",
            int_expanded_minute                                          AS "expanded_minute",
            int_second                                                   AS "second",
            int_period                                                   AS "period",
            CAST(str_type_id AS INTEGER)                                 AS "type_id",
            str_type_name                                                AS "type_name",
            CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
            bool_is_shot                                                 AS "is_shot",
            dec_x                                                        AS "x",
            dec_y                                                        AS "y",
            int_is_rupture                                               AS "is_rupture",
            str_chain_trigger                                            AS "chain_trigger",
            int_certain_possessor                                        AS "certain_possessor",
            CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_event_enriched
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
        ie.match_id,
        ie.team_id,
        ie.player_id,
        ie.event_id,
        ie.row_num,
        ie.expanded_minute,
        ie.second,
        ie.period,
        ie.type_id,
        ie.type_name,
        ie.outcome_id,
        ie.is_shot,
        ie.x,
        ie.y,
        ie.season,
        ie.league_source,
        ie.scraped_at,

        -- Remise en jeu formelle : force une nouvelle chaîne
        CASE WHEN EXISTS (
            SELECT 1 FROM in_events_qual eq
            WHERE eq.match_id    = ie.match_id
              AND eq.row_num     = ie.row_num
              AND eq.qual_type_id IN (5, 6, 107, 124, 241)
        ) THEN 1 ELSE 0 END                             AS is_set_piece,

        CASE
            -- Action active : le joueur contrôle intentionnellement le ballon
            WHEN et.possession_type = 'actif'
                THEN team_id
            -- Pas une possession certaine
            ELSE NULL
        END AS certain_possessor,

        CASE
            WHEN ie.type_id = 12  -- Clearance
            AND LEAD(certain_possessor IGNORE NULLS) OVER (
                    PARTITION BY ie.match_id
                    ORDER BY ie.expanded_minute, ie.second, ie.row_num
                ) = ie.team_id   -- Le prochain événement est de la même équipe
            THEN 1
            ELSE 0
        END AS is_winning_clearance

    FROM in_int_event_enriched ie
    LEFT JOIN {{ ref('event_types') }} et ON ie.type_id = et.type_id
    WHERE ie.match_id IN (SELECT match_id FROM new_matches)
    AND ie.player_id IS NOT NULL
    AND ie.type_id NOT IN (17, 18, 19, 30, 32, 34, 40)
    -- Card, SubstitutionOff, SubstitutionOn, End, Start, FormationSet, FormationChange
),

chain_trigger_quals AS (
    SELECT
        match_id,
        row_num,
        MAX(CASE WHEN qual_type_id = 6   THEN 1 ELSE 0 END) AS is_corner_taken,
        MAX(CASE WHEN qual_type_id = 5   THEN 1 ELSE 0 END) AS is_free_kick,
        MAX(CASE WHEN qual_type_id = 107 THEN 1 ELSE 0 END) AS is_throw_in,
        MAX(CASE WHEN qual_type_id = 124 THEN 1 ELSE 0 END) AS is_goal_kick
    FROM in_events_qual
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND qual_type_id IN (5, 6, 107, 124)
    GROUP BY match_id, row_num
),

foul_resolution AS (
    SELECT
        match_id,
        row_num,
        -- Le row_num le plus élevé du groupe de Foul simultané
        MAX(row_num) OVER (
            PARTITION BY match_id, expanded_minute, second
        )                                                       AS last_row_num_in_foul_group,
        -- Une des deux lignes du groupe a-t-elle outcome_id = 1 ?
        MAX(CASE WHEN outcome_id = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY match_id, expanded_minute, second
        )                                                       AS foul_won
    FROM certain_possession
    WHERE type_id = 4
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
        pc.*,
        fr.last_row_num_in_foul_group,
        fr.foul_won,
        CASE
            WHEN is_set_piece = 1 THEN 1
            WHEN LAG(is_winning_clearance) OVER (
                PARTITION BY pc.match_id
                ORDER BY pc.expanded_minute, pc.second, pc.row_num
            ) = 1 THEN 1
            WHEN LAG(type_id) OVER (
                    PARTITION BY pc.match_id
                    ORDER BY pc.expanded_minute, pc.second, pc.row_num
                ) = 41
            AND LAG(outcome_id) OVER (
                    PARTITION BY pc.match_id
                    ORDER BY pc.expanded_minute, pc.second, pc.row_num
                ) = 1                               THEN 1
            -- Nouvelle règle : la ligne précédente est la dernière d'un groupe
            -- de Foul simultané, et ce groupe a été gagné par une équipe
            WHEN LAG(pc.row_num) OVER (
                    PARTITION BY pc.match_id
                    ORDER BY pc.expanded_minute, pc.second, pc.row_num
                 ) = LAG(fr.last_row_num_in_foul_group) OVER (
                    PARTITION BY pc.match_id
                    ORDER BY pc.expanded_minute, pc.second, pc.row_num
                 )
             AND LAG(foul_won) OVER (
                    PARTITION BY pc.match_id
                    ORDER BY pc.expanded_minute, pc.second, pc.row_num
                 ) = 1                               THEN 1
            WHEN certain_possessor IS NOT NULL
             AND certain_possessor != LAG(certain_possessor IGNORE NULLS) OVER (
                    PARTITION BY pc.match_id
                     ORDER BY pc.expanded_minute, pc.second, pc.row_num   
                 )
            THEN 1
            WHEN certain_possessor IS NOT NULL
             AND LAG(certain_possessor IGNORE NULLS) OVER (
                    PARTITION BY pc.match_id
                    ORDER BY pc.expanded_minute, pc.second, pc.row_num
                 ) IS NULL
            THEN 1
            ELSE 0
        END AS possession_group_increment
    FROM certain_possession pc
    LEFT JOIN foul_resolution fr
        ON  fr.match_id = pc.match_id
        AND fr.row_num  = pc.row_num
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



chain_first_events AS (
    SELECT
        match_id,
        possession_group,
        MIN(row_num) AS first_row_num
    FROM possession_resolved
    GROUP BY match_id, possession_group
),

chain_triggers AS (
    SELECT
        cfe.match_id,
        cfe.possession_group,
        CASE
            WHEN pr.type_id IN (7, 8, 49)      THEN 'recovery'
            WHEN ctq.is_corner_taken = 1        THEN 'corner'
            WHEN ctq.is_goal_kick    = 1        THEN 'goal_kick'
            WHEN ctq.is_throw_in     = 1        THEN 'throw_in'
            WHEN ctq.is_free_kick    = 1        THEN 'free_kick'
            ELSE                                     'open_play'
        END AS chain_trigger
    FROM chain_first_events cfe
    JOIN possession_resolved pr
        ON  pr.match_id = cfe.match_id
        AND pr.row_num  = cfe.first_row_num
    LEFT JOIN chain_trigger_quals ctq
        ON  ctq.match_id = pr.match_id
        AND ctq.row_num  = pr.row_num
),

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_BOUNDARIES
-- La rupture = le resolved_possessor change entre deux lignes consécutives.
-- chain_number = SUM(is_rupture) cumulatif, décalé pour que la rupture
-- appartienne à la chaîne qu'elle clôt.
-- ══════════════════════════════════════════════════════════════════════════════
chain_boundaries AS (
    SELECT
        pr.*,
        ct.chain_trigger,
        CASE
            WHEN pr.match_row_number = 1
                THEN 0
            WHEN LEAD(pr.resolved_possessor) OVER (
                    PARTITION BY pr.match_id
                    ORDER BY pr.expanded_minute, pr.second, pr.row_num
                ) IS DISTINCT FROM pr.resolved_possessor
                THEN 1
            ELSE 0
        END AS is_rupture
    FROM possession_resolved pr
    LEFT JOIN chain_triggers ct
        ON  ct.match_id        = pr.match_id
        AND ct.possession_group = pr.possession_group
),

-- ══════════════════════════════════════════════════════════════════════════════
-- On détecte les aerial orphelins : les aerials qui n'ont pas de pair (OppositeRelatedEvent) dans la même chaîne.
aerial_orphans AS (
    SELECT
        ppc.match_id,
        ppc.row_num,
        ppc.team_id,
        ppc.expanded_minute,
        ppc.second,
        ppc.possession_group
    FROM chain_boundaries ppc
    LEFT JOIN in_events_qual eq
        ON  eq.match_id     = ppc.match_id
        AND eq.event_id     = ppc.event_id
        AND eq.qual_type_id = 233
    WHERE ppc.type_id   = 44
      AND eq.qual_value IS NULL
),

-- ══════════════════════════════════════════════════════════════════════════════
-- AERIAL_ORPHAN_PAIRS: on trouve le pair de chaque Aerial orphelin dans la chaîne adverse, et on récupère son possession_group.
aerial_orphan_pairs AS (
    SELECT
        ao.match_id,
        ao.row_num,
        MIN(cb.possession_group) AS pair_possession_group
    FROM aerial_orphans ao
    JOIN chain_boundaries cb
        ON  cb.match_id        = ao.match_id
        AND cb.team_id        != ao.team_id
        AND cb.type_id         = 44
        AND cb.expanded_minute = ao.expanded_minute
        AND cb.second          = ao.second
    GROUP BY ao.match_id, ao.row_num
),
-- ══════════════════════════════════════════════════════════════════════════════
-- AERIAL_CORRECTIONS
-- Pour chaque Aerial, on vérifie si son pair (OppositeRelatedEvent) est dans
-- une chaîne différente. Si oui, on leur assigne le même chain_id en prenant
-- le plus petit possession_group des deux — pour garder les deux Aerial
-- dans la même chaîne.
-- ══════════════════════════════════════════════════════════════════════════════

aerial_with_pair_group AS (
    SELECT
        cb.match_id,
        cb.row_num,
        cb.possession_group,
        cb_pair.possession_group AS pair_possession_group
    FROM chain_boundaries cb
    LEFT JOIN in_events_qual eq
        ON  eq.match_id     = cb.match_id
        AND eq.event_id     = cb.event_id
        AND eq.qual_type_id = 233
    LEFT JOIN chain_boundaries cb_pair
        ON  cb_pair.match_id = cb.match_id
        AND cb_pair.event_id = CAST(eq.qual_value AS INTEGER)
        AND cb_pair.team_id != cb.team_id
    WHERE cb.type_id = 44
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cb.match_id, cb.row_num
        ORDER BY COALESCE(cb_pair.possession_group, cb.possession_group)
    ) = 1
),

aerial_best_group AS (
    SELECT
        awpg.match_id,
        awpg.row_num,
        LEAST(
            awpg.possession_group,
            COALESCE(
                awpg.pair_possession_group,
                aop.pair_possession_group,
                awpg.possession_group
            )
        ) AS best_possession_group
    FROM aerial_with_pair_group awpg
    LEFT JOIN aerial_orphan_pairs aop
        ON  aop.match_id = awpg.match_id
        AND aop.row_num  = awpg.row_num
),

aerial_corrections AS (
    SELECT
        match_id,
        row_num,
        match_id || '_' || CAST(best_possession_group AS VARCHAR) AS chain_id_corrected,
        best_possession_group                                       AS chain_number_corrected
    FROM aerial_best_group
)
-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- Une ligne par action avec son chain_id.
-- chain_team_id = resolved_possessor de la chaîne (équipe en possession).
-- Les événements à possession indéterminée ont resolved_possessor propagé
-- depuis la dernière possession certaine.
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
    cb.match_id,
    cb.season,
    cb.league_source,

    COALESCE(ac.chain_id_corrected,
        cb.match_id || '_' || CAST(cb.possession_group AS VARCHAR)
    )                                                       AS chain_id,

    COALESCE(ac.chain_number_corrected,
        cb.possession_group
    )                                                       AS chain_number,

    -- Équipe en possession durant cette chaîne
    resolved_possessor                                      AS chain_team_id,

    cb.team_id,
    cb.player_id,
    cb.event_id,
    cb.row_num,
    cb.expanded_minute,
    cb.second,
    cb.period,
    cb.type_id,
    cb.type_name,
    cb.outcome_id,
    cb.is_shot,
    cb.x,
    cb.y,
    cb.is_rupture,
    cb.chain_trigger,
    cb.certain_possessor,
    cb.scraped_at

FROM chain_boundaries cb
LEFT JOIN aerial_corrections ac
    ON  ac.match_id = cb.match_id
    AND ac.row_num  = cb.row_num
ORDER BY cb.match_id, cb.expanded_minute, cb.second, cb.row_num
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "chain_id"                                                   AS str_chain_id,
        CAST(chain_number AS BIGINT)                                 AS int_chain_number,
        CAST(chain_team_id AS VARCHAR)                               AS str_chain_team_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(event_id AS VARCHAR)                                    AS str_event_id,
        "row_num"                                                    AS int_row_num,
        "expanded_minute"                                            AS int_expanded_minute,
        "second"                                                     AS int_second,
        "period"                                                     AS int_period,
        CAST(type_id AS VARCHAR)                                     AS str_type_id,
        "type_name"                                                  AS str_type_name,
        CAST(outcome_id AS VARCHAR)                                  AS str_outcome_id,
        "is_shot"                                                    AS bool_is_shot,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "is_rupture"                                                 AS int_is_rupture,
        "chain_trigger"                                              AS str_chain_trigger,
        "certain_possessor"                                          AS int_certain_possessor,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at
    FROM mdl_body
)

SELECT * FROM mdl_out
