{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_event_id_a'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_network_duels'
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
            int_expanded_minute                                          AS "expanded_minute",
            int_second                                                   AS "second",
            CAST(str_duel_type_id AS INTEGER)                            AS "duel_type_id",
            str_duel_type                                                AS "duel_type",
            dec_x                                                        AS "x",
            dec_y                                                        AS "y",
            int_event_id_a                                               AS "event_id_a",
            int_team_id_a                                                AS "team_id_a",
            int_player_id_a                                              AS "player_id_a",
            int_outcome_a                                                AS "outcome_a",
            int_event_id_b                                               AS "event_id_b",
            int_team_id_b                                                AS "team_id_b",
            int_player_id_b                                              AS "player_id_b",
            int_outcome_b                                                AS "outcome_b"
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
-- DUEL_LINKS
-- Point de départ : on filtre events_qual sur qual_type_id = 233
-- pour n'avoir que les événements qui ont un miroir adverse.
-- On joint int_event_enriched sur (match_id, row_num) pour enrichir le côté A
-- avec team_id, player_id, type_id, outcome_id, x, y.
-- ══════════════════════════════════════════════════════════════════════════════
duel_links AS (
    SELECT
        eq.match_id,
        eq.row_num,
        CAST(eq.qual_value AS BIGINT)   AS opposite_event_id,
        ie.team_id,
        ie.player_id,
        ie.event_id,
        ie.type_id,
        ie.type_name,
        ie.outcome_id,
        ie.x,
        ie.y,
        ie.expanded_minute,
        ie.second,
        ie.season,
        ie.league_source
    FROM in_events_qual eq
    JOIN in_int_event_enriched ie
        ON  ie.match_id = eq.match_id
        AND ie.row_num  = eq.row_num
    WHERE eq.match_id    IN (SELECT match_id FROM new_matches)
      AND eq.qual_type_id = 233
      AND ie.type_id      IN (3, 4, 7, 44, 45, 50)
      AND ie.player_id    IS NOT NULL
),

-- ══════════════════════════════════════════════════════════════════════════════
-- DUELS_PAIRED
-- On joint duel_links sur lui-même pour ramener le côté B.
-- La jointure côté B se fait sur (match_id, team_id != team_id_a, event_id = opposite_event_id)
-- pour contraindre explicitement vers l'équipe adverse — évite l'ambiguïté
-- quand opposite_event_id existe pour les deux équipes.
-- Le filtre dl_a.row_num < dl_b.row_num déduplique : chaque duel
-- est présent deux fois dans duel_links, on n'en garde qu'une.
-- ══════════════════════════════════════════════════════════════════════════════
duels_paired AS (
    SELECT
        dl_a.match_id,
        dl_a.season,
        dl_a.league_source,
        dl_a.expanded_minute,
        dl_a.second,
        dl_a.type_id                    AS duel_type_id,
        dl_a.type_name                  AS duel_type,
        dl_a.x,
        dl_a.y,

        -- Côté A
        dl_a.event_id                   AS event_id_a,
        dl_a.team_id                    AS team_id_a,
        dl_a.player_id                  AS player_id_a,
        dl_a.outcome_id                 AS outcome_a,

        -- Côté B
        dl_b.event_id                   AS event_id_b,
        dl_b.team_id                    AS team_id_b,
        dl_b.player_id                  AS player_id_b,
        dl_b.outcome_id                 AS outcome_b

    FROM duel_links dl_a
    JOIN duel_links dl_b
        ON  dl_b.match_id  = dl_a.match_id
        AND dl_b.event_id  = dl_a.opposite_event_id
        AND dl_b.team_id  != dl_a.team_id

    WHERE dl_a.row_num < dl_b.row_num
)

SELECT * FROM duels_paired
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "expanded_minute"                                            AS int_expanded_minute,
        "second"                                                     AS int_second,
        CAST(duel_type_id AS VARCHAR)                                AS str_duel_type_id,
        "duel_type"                                                  AS str_duel_type,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "event_id_a"                                                 AS int_event_id_a,
        "team_id_a"                                                  AS int_team_id_a,
        "player_id_a"                                                AS int_player_id_a,
        "outcome_a"                                                  AS int_outcome_a,
        "event_id_b"                                                 AS int_event_id_b,
        "team_id_b"                                                  AS int_team_id_b,
        "player_id_b"                                                AS int_player_id_b,
        "outcome_b"                                                  AS int_outcome_b
    FROM mdl_body
)

SELECT * FROM mdl_out
