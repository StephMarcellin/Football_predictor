{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_chain_id', 'str_player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        tags=['by_season'],
        alias='intermediate_threat_conceded_credits'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- intermediate_whoscored_event_qualifiers lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_events_qual AS NOT MATERIALIZED (
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
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
),

-- intermediate_possession_chains lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_possession_chains AS NOT MATERIALIZED (
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
    FROM {{ ref('intermediate_possession_chains') }}
),

-- intermediate_xg_chain_credits lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_xg_chain AS NOT MATERIALIZED (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(str_shot_event_id AS INTEGER)                           AS "shot_event_id",
        CAST(str_shot_type_id AS INTEGER)                            AS "shot_type_id",
        int_shot_minute                                              AS "shot_minute",
        dec_shot_outcome_score                                       AS "shot_outcome_score",
        bool_is_counter_attack                                       AS "is_counter_attack",
        int_position_in_chain                                        AS "position_in_chain",
        int_chain_length                                             AS "chain_length",
        dec_position_weight                                          AS "position_weight",
        bool_is_shooter                                              AS "is_shooter",
        bool_is_assister                                             AS "is_assister",
        dec_xgchain                                                  AS "xgchain",
        dec_xgchain_weighted                                         AS "xgchain_weighted",
        dec_xgbuildup                                                AS "xgbuildup",
        dec_xgbuildup_weighted                                       AS "xgbuildup_weighted"
    FROM {{ ref('intermediate_xg_chain_credits') }}
),

-- intermediate_whoscored_player_match_official lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_match_official AS NOT MATERIALIZED (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_position                                                 AS "position_name"
    FROM {{ ref('intermediate_whoscored_player_match_official') }}
),

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- ══════════════════════════════════════════════════════════════════════════════
{% if var('only_season', none) %}
-- Mode « une saison » (orchestrateur, boucle par saison) : matchs de la saison
-- pas encore présents dans la table. Remplace le critère habituel (scraped_at,
-- jointure sur {{ this }}) qui exclurait à tort une saison plus ancienne que
-- la dernière déjà chargée.
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_xg_chain
    WHERE match_id IN ({{ season_match_ids(this) }})
    {%- if is_incremental() %}
      AND match_id NOT IN (SELECT DISTINCT str_match_id FROM {{ this }})
    {%- endif %}
),
{% elif is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_xg_chain
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            str_chain_id                                                 AS "chain_id",
            CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
            CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            dec_shot_outcome_score                                   AS "shot_outcome_score",
            bool_is_penalty                                              AS "is_penalty",
            int_position_in_chain                                        AS "position_in_chain",
            int_chain_length                                             AS "chain_length",
            dec_position_weight                                          AS "position_weight",
            bool_is_keeper                                               AS "is_keeper",
            bool_has_error_leading_to_goal                               AS "has_error_leading_to_goal",
            dec_threat_conceded                                          AS "threat_conceded",
            dec_threat_conceded_weighted                                 AS "threat_conceded_weighted"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_xg_chain
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- CHAIN_SHOTS
-- Une ligne par chaîne avec tir.
-- is_penalty : le tir terminal est-il un penalty (qual 9) ?
--   → si oui, le gardien adverse n'est pas sanctionné
-- ══════════════════════════════════════════════════════════════════════════════
chain_shots AS (
    SELECT DISTINCT ON (xgc.match_id, xgc.chain_id)
        xgc.match_id,
        xgc.chain_id,
        xgc.chain_number,
        xgc.chain_team_id,
        xgc.shot_outcome_score,
        xgc.season,
        xgc.league_source,
        xgc.shot_event_id,
        CASE
            WHEN eq_pen.qual_type_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                                 AS is_penalty
    FROM in_player_xg_chain xgc
    LEFT JOIN in_events_qual eq_pen
        ON  eq_pen.match_id     = xgc.match_id
        AND eq_pen.event_id     = xgc.shot_event_id
        AND eq_pen.qual_type_id = 9  -- Penalty
    WHERE xgc.match_id IN (SELECT match_id FROM new_matches)
    ORDER BY xgc.match_id, xgc.chain_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- KEEPERS — etre gardien est une propriete du JOUEUR DANS LE MATCH
-- Refonte du 07/10/2026 (defaut 9). L'ancienne version reconnaissait un gardien
-- a ses actions DANS LA CHAINE, parmi les types 10/11/41/52. Deux erreurs :
--
--   1. Le type 10 Save n'est PAS exclusif au gardien : sur 34 252 evenements,
--      seuls 46,3 % sont le fait d'un gardien ; 16 779 sont des joueurs de
--      champ qui ont CONTRE un tir. Resultat : 18 125 lignes etaient marquees
--      is_keeper a tort (8 944 defenseurs centraux, 2 179 milieux...) et donc
--      exonerees de toute menace concedee.
--   2. Le perimetre par chaine inversait la regle : un gardien battu n'a pas
--      d'evenement Save dans la chaine, donc il n'etait PAS reconnu gardien et
--      etait facture comme un defenseur ordinaire -- 990 lignes de vrais
--      gardiens. Sur Chelsea-City, le gardien encaissait 1,0 a la 53e, et
--      Ederson 1,0 sur le penalty de la 101e alors que la regle 1 l'interdit.
--
-- Etre gardien est un fait du match, pas de la possession : le perimetre est
-- donc (match_id, player_id), sans chain_id.
--
-- Deux criteres, qui disent tous deux 'ce joueur est gardien dans ce match' :
--   a. le poste officiel de la composition (str_position = 'GK') : 5 198 lignes,
--      soit exactement 2 par match sur 2 599 matchs ;
--   b. une action reellement exclusive au gardien, pour rattraper le gardien
--      entre en cours de match, que la composition type 'Sub'. Les sept types
--      retenus sont a 98,9-99,7 % le fait d'un gardien : 11 Claim (99,5),
--      41 Punch (99,4), 52 KeeperPickup (99,5), 53 CrossNotClaimed (98,9),
--      54 Smother (99,1), 58 PenaltyFaced (98,9), 59 KeeperSweeper (99,7).
--      Le type 10 est exclu, c'est lui le coupable. Ce critere ne concerne que
--      56 paires (match, joueur) sur 2 599 matchs.
--
-- La lecture se fait depuis in_player_possession_chains et non depuis
-- events_enriched : les deux sources donnent le MEME ensemble de 5 246 paires
-- (0 difference dans les deux sens), donc on evite un amont de 4 M de lignes.
-- ══════════════════════════════════════════════════════════════════════════════
keepers AS (
    SELECT DISTINCT
        o.match_id,
        o.player_id
    FROM in_player_match_official o
    WHERE o.position_name = 'GK'
      AND o.match_id IN (SELECT match_id FROM new_matches)

    UNION

    SELECT DISTINCT
        pc.match_id,
        pc.player_id
    FROM in_player_possession_chains pc
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.type_id IN (11, 41, 52, 53, 54, 58, 59)
),

-- ══════════════════════════════════════════════════════════════════════════════
-- KEEPER_ERRORS
-- Identifie les gardiens qui ont commis une Error LeadingToGoal
-- dans la chaîne. Ces gardiens peuvent être sanctionnés malgré leur statut.
-- ══════════════════════════════════════════════════════════════════════════════
keeper_errors AS (
    SELECT DISTINCT
        pc.match_id,
        pc.chain_id,
        pc.player_id
    FROM in_player_possession_chains pc
    JOIN chain_shots cs
        ON  cs.match_id = pc.match_id
        AND cs.chain_id = pc.chain_id
    JOIN in_events_qual eq_err
        ON  eq_err.match_id     = pc.match_id
        AND eq_err.event_id     = pc.event_id
        AND eq_err.qual_type_id = 170  -- LeadingToGoal
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id  != pc.chain_team_id
      AND pc.type_id   = 51  -- Error
),

-- ══════════════════════════════════════════════════════════════════════════════
-- DEFENSIVE_PLAYERS_RAW
-- Joueurs adverses présents dans les chaînes avec tir.
-- ══════════════════════════════════════════════════════════════════════════════
defensive_players_raw AS (
    SELECT
        pc.match_id,
        pc.chain_id,
        pc.chain_team_id,
        pc.player_id,
        pc.team_id,
        pc.expanded_minute,
        pc.second,
        pc.row_num,
        ROW_NUMBER() OVER (
            PARTITION BY pc.match_id, pc.chain_id
            ORDER BY pc.expanded_minute, pc.second, pc.row_num
        ) AS position_in_chain,
        COUNT(*) OVER (
            PARTITION BY pc.match_id, pc.chain_id
        ) AS chain_length
    FROM in_player_possession_chains pc
    JOIN chain_shots cs
        ON  cs.match_id = pc.match_id
        AND cs.chain_id = pc.chain_id
    WHERE pc.match_id IN (SELECT match_id FROM new_matches)
      AND pc.team_id  != pc.chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- DEFENSIVE_PLAYERS
-- Déduplique + ajoute les flags is_keeper et has_error.
-- ══════════════════════════════════════════════════════════════════════════════
defensive_players AS (
    SELECT DISTINCT ON (dp.match_id, dp.chain_id, dp.player_id)
        dp.match_id,
        dp.chain_id,
        dp.chain_team_id,
        dp.player_id,
        dp.team_id,
        dp.position_in_chain,
        dp.chain_length,
        ROUND(
            CAST(dp.position_in_chain AS DOUBLE) / dp.chain_length,
            4
        )                                                   AS position_weight,
        CASE WHEN ka.player_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_keeper,
        CASE WHEN ke.player_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_error_leading_to_goal
    FROM defensive_players_raw dp
    -- par (match, joueur) : etre gardien ne depend pas de la chaine
    LEFT JOIN keepers ka
        ON  ka.match_id  = dp.match_id
        AND ka.player_id = dp.player_id
    LEFT JOIN keeper_errors ke
        ON  ke.match_id  = dp.match_id
        AND ke.chain_id  = dp.chain_id
        AND ke.player_id = dp.player_id
    ORDER BY dp.match_id, dp.chain_id, dp.player_id, dp.position_in_chain DESC
),

-- ══════════════════════════════════════════════════════════════════════════════
-- THREAT_COMPUTED
-- Calcul de intermediate_threat_conceded_credits avec règles de sanction du gardien :
--
-- Règle 1 — Penalty : le gardien n'est JAMAIS sanctionné
--   → threat_conceded_weighted = 0 pour le gardien si is_penalty = TRUE
--
-- Règle 2 — Erreur du gardien : sanction pleine si Error LeadingToGoal
--   → threat_conceded_weighted normal même si is_keeper = TRUE
--
-- Règle 3 — Défense trouée : le gardien n'est PAS sanctionné par défaut
--   → threat_conceded_weighted = 0 si is_keeper = TRUE sans erreur
--
-- Pour les défenseurs non-gardiens : sanction pondérée normale.
-- ══════════════════════════════════════════════════════════════════════════════
threat_computed AS (
    SELECT
        dp.match_id,
        dp.chain_id,
        cs.chain_number,
        dp.chain_team_id,
        dp.player_id,
        dp.team_id,
        cs.season,
        cs.league_source,
        cs.shot_outcome_score,
        cs.is_penalty,
        dp.position_in_chain,
        dp.chain_length,
        dp.position_weight,
        dp.is_keeper,
        dp.has_error_leading_to_goal,

        -- Sanction brute
        cs.shot_outcome_score                                         AS threat_conceded,

        -- Sanction pondérée avec règles gardien
        CASE
            -- Règle 1 : penalty → gardien non sanctionné
            WHEN dp.is_keeper = TRUE AND cs.is_penalty = TRUE
                THEN 0.0
            -- Règle 2 : erreur du gardien → sanction pleine
            WHEN dp.is_keeper = TRUE AND dp.has_error_leading_to_goal = TRUE
                THEN ROUND(cs.shot_outcome_score * dp.position_weight, 4)
            -- Règle 3 : défense trouée → gardien non sanctionné
            WHEN dp.is_keeper = TRUE
                THEN 0.0
            -- Défenseur non-gardien : sanction pondérée normale
            ELSE ROUND(cs.shot_outcome_score * dp.position_weight, 4)
        END                                                 AS threat_conceded_weighted

    FROM defensive_players dp

    JOIN chain_shots cs
        ON  cs.match_id = dp.match_id
        AND cs.chain_id = dp.chain_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM threat_computed
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "chain_id"                                                   AS str_chain_id,
        CAST(chain_number AS BIGINT)                                 AS int_chain_number,
        CAST(chain_team_id AS VARCHAR)                               AS str_chain_team_id,
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "shot_outcome_score"                                                   AS dec_shot_outcome_score,
        "is_penalty"                                                 AS bool_is_penalty,
        "position_in_chain"                                          AS int_position_in_chain,
        "chain_length"                                               AS int_chain_length,
        "position_weight"                                            AS dec_position_weight,
        "is_keeper"                                                  AS bool_is_keeper,
        "has_error_leading_to_goal"                                  AS bool_has_error_leading_to_goal,
        "threat_conceded"                                            AS dec_threat_conceded,
        "threat_conceded_weighted"                                   AS dec_threat_conceded_weighted
    FROM mdl_body
)

SELECT * FROM mdl_out
