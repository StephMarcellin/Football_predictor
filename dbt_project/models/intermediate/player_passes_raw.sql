{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_passes_raw'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

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

-- player_possession_chains lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_possession_chains AS (
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
    FROM {{ ref('player_possession_chains') }}
),

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            str_chain_id                                                 AS "chain_id",
            str_chain_trigger                                            AS "chain_trigger",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_passer_id AS INTEGER)                               AS "passer_id",
            CAST(str_receiver_id AS INTEGER)                             AS "receiver_id",
            int_row_num                                                  AS "row_num",
            int_expanded_minute                                          AS "expanded_minute",
            int_second                                                   AS "second",
            dec_x                                                        AS "x",
            dec_y                                                        AS "y",
            dec_end_x                                                    AS "end_x",
            dec_end_y                                                    AS "end_y",
            int_is_key_pass                                              AS "is_key_pass",
            int_is_shot_assist                                           AS "is_shot_assist",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            bool_is_progressive                                          AS "is_progressive",
            bool_is_creative                                             AS "is_creative",
            bool_is_buildup                                              AS "is_buildup"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- TEAM_EVENTS
-- On filtre sur team_id = chain_team_id pour ne garder que les événements
-- de l'équipe en possession. Le LEAD est appliqué sur cette séquence épurée :
-- receiver_id pointe toujours vers le prochain joueur de la même équipe
-- dans la chaîne, sans sauter par-dessus des événements adverses.
-- ══════════════════════════════════════════════════════════════════════════════
team_events AS (
    SELECT
        match_id,
        chain_id,
        chain_trigger,
        chain_team_id,
        team_id,
        player_id                                                   AS passer_id,
        LEAD(player_id) OVER (
            PARTITION BY match_id, chain_id
            ORDER BY expanded_minute, second, row_num
        )                                                           AS receiver_id,
        row_num,
        expanded_minute,
        second,
        type_id,
        outcome_id,
        x,
        y,
        season,
        league_source
    FROM in_player_possession_chains
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND team_id = chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSES_RAW
-- Filtre sur les passes réussies.
-- JOIN sur int_event_enriched pour récupérer end_x, end_y, is_key_pass,
-- is_shot_assist — absents de player_possession_chains.
-- La jointure se fait sur (match_id, row_num), clé unique partagée.
-- ══════════════════════════════════════════════════════════════════════════════
passes_raw AS (
    SELECT
        te.match_id,
        te.chain_id,
        te.chain_trigger,
        te.chain_team_id                                            AS team_id,
        te.passer_id,
        te.receiver_id,
        te.row_num,
        te.expanded_minute,
        te.second,
        te.x,
        te.y,
        ie.end_x,
        ie.end_y,
        ie.is_key_pass,
        ie.is_shot_assist,
        te.season,
        te.league_source
    FROM team_events te
    JOIN in_int_event_enriched ie
        ON  ie.match_id = te.match_id
        AND ie.row_num  = te.row_num
    WHERE te.type_id   = 1
      AND te.outcome_id = 1
      AND te.receiver_id IS NOT NULL
      AND te.receiver_id != te.passer_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- PASSES_NETWORK
-- Calcul des 3 flags qualifiant la nature de la passe.
-- is_progressive : la passe fait avancer le ballon d'au moins 10 mètres
--                  vers le but adverse (end_x > x + 10)
-- is_creative    : la passe mène directement à un tir
--                  (is_key_pass=1 ou is_shot_assist=1)
-- is_buildup     : la passe est jouée dans la moitié défensive (x < 50)
-- ══════════════════════════════════════════════════════════════════════════════
passes_network AS (
    SELECT
        match_id,
        chain_id,
        chain_trigger,
        team_id,
        passer_id,
        receiver_id,
        row_num,
        expanded_minute,
        second,
        x,
        y,
        end_x,
        end_y,
        is_key_pass,
        is_shot_assist,
        season,
        league_source,

        -- Passe progressive : avance vers le but adverse
        CASE WHEN end_x > x + 10               THEN TRUE ELSE FALSE END AS is_progressive,

        -- Passe créative : mène directement à un tir
        CASE WHEN is_key_pass = 1
              OR is_shot_assist = 1             THEN TRUE ELSE FALSE END AS is_creative,

        -- Passe de construction : dans la moitié défensive
        CASE WHEN x < 50                        THEN TRUE ELSE FALSE END AS is_buildup

    FROM passes_raw
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM passes_network
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "chain_id"                                                   AS str_chain_id,
        "chain_trigger"                                              AS str_chain_trigger,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(passer_id AS VARCHAR)                                   AS str_passer_id,
        CAST(receiver_id AS VARCHAR)                                 AS str_receiver_id,
        "row_num"                                                    AS int_row_num,
        "expanded_minute"                                            AS int_expanded_minute,
        "second"                                                     AS int_second,
        "x"                                                          AS dec_x,
        "y"                                                          AS dec_y,
        "end_x"                                                      AS dec_end_x,
        "end_y"                                                      AS dec_end_y,
        "is_key_pass"                                                AS int_is_key_pass,
        "is_shot_assist"                                             AS int_is_shot_assist,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "is_progressive"                                             AS bool_is_progressive,
        "is_creative"                                                AS bool_is_creative,
        "is_buildup"                                                 AS bool_is_buildup
    FROM mdl_body
)

SELECT * FROM mdl_out
