{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_shot_row_num', 'int_sca_order'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_shot_creating_actions'
    )
}}

-- Actions créatrices de tir (SCA) et de but (GCA) — standard FBref/StatsBomb.
-- Pour chaque tir, on crédite les 2 actions offensives qui l'ont précédé DANS LA
-- MÊME CHAÎNE DE POSSESSION, côté équipe qui tire. Capte passeurs et pré-passeurs
-- que les passes décisives seules ratent.
--
-- Grain = un crédit SCA : (tir, sca_order 1|2). Format long → gold agrège
-- facilement : SCA/joueur = COUNT ; GCA/joueur = COUNT WHERE is_gca.
--
-- Source = player_possession_chains : donne le chain_id, l'ordre temporel et
-- l'équipe en possession. On restreint aux événements de l'équipe qui tire pour
-- exclure les actions adverses présentes dans la chaîne (ex. un dégagement contré).
--
-- Actions créatrices retenues (périmètre offensif standard) :
--   Pass (1), TakeOn/dribble (3), faute obtenue (4 outcome 1), tir précédent /
--   rebond (13/14/15/16). Pas d'actions défensives.
--
-- Comptes de référence (échantillon 300 matchs, validés read-only) :
--   0 doublon, creator jamais NULL, ~84 % de passes, SCA1 > SCA2 (tirs sans 2e
--   action avant), GCA = crédits dont le tir est un but.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

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

-- ── FILTRE INCRÉMENTAL (même patron que player_possession_chains) ──────────────
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            int_shot_row_num                                             AS "shot_row_num",
            int_sca_order                                                AS "sca_order",
            CAST(str_shot_event_id AS INTEGER)                           AS "shot_event_id",
            str_chain_id                                                 AS "chain_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
            CAST(str_attacking_team_id AS BIGINT)                        AS "attacking_team_id",
            CAST(str_shot_taker_player_id AS INTEGER)                    AS "shot_taker_player_id",
            CAST(str_creator_player_id AS INTEGER)                       AS "creator_player_id",
            str_action_type                                              AS "action_type",
            int_action_row_num                                           AS "action_row_num",
            bool_is_gca                                                  AS "is_gca"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM in_player_possession_chains
),
{% endif %}

-- ── Événements des chaînes, ordonnés dans la chaîne ───────────────────────────
chain_seq AS (
    SELECT
        c.match_id,
        c.chain_id,
        c.season,
        c.league_source,
        c.scraped_at,
        c.row_num,
        c.event_id,
        c.team_id,
        c.player_id,
        c.type_id,
        c.type_name,
        c.outcome_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.match_id, c.chain_id
            ORDER BY c.expanded_minute, c.second, c.row_num
        ) AS seq_in_chain
    FROM in_player_possession_chains c
    WHERE c.match_id IN (SELECT match_id FROM new_matches)
),

-- ── Les tirs (pivot) ──────────────────────────────────────────────────────────
shots AS (
    SELECT
        match_id,
        chain_id,
        season,
        league_source,
        scraped_at,
        row_num       AS shot_row_num,
        event_id      AS shot_event_id,
        team_id       AS attacking_team_id,
        player_id     AS shot_taker_player_id,
        (type_id = 16) AS is_goal,          -- Goal → les crédits seront des GCA
        seq_in_chain
    FROM chain_seq
    WHERE type_id IN (13, 14, 15, 16)
),

-- ── Actions créatrices candidates : même chaîne, même équipe, avant le tir ────
sca AS (
    SELECT
        s.match_id,
        s.shot_row_num,
        s.shot_event_id,
        s.chain_id,
        s.season,
        s.league_source,
        s.scraped_at,
        s.attacking_team_id,
        s.shot_taker_player_id,
        s.is_goal,
        c.player_id   AS creator_player_id,
        c.type_name   AS action_type,
        c.row_num     AS action_row_num,
        -- 1 = action juste avant le tir, 2 = l'action d'avant
        ROW_NUMBER() OVER (
            PARTITION BY s.match_id, s.shot_row_num
            ORDER BY c.seq_in_chain DESC
        ) AS sca_order
    FROM shots s
    JOIN chain_seq c
        ON  c.match_id     = s.match_id
        AND c.chain_id     = s.chain_id
        AND c.team_id      = s.attacking_team_id   -- côté équipe qui tire
        AND c.seq_in_chain < s.seq_in_chain        -- strictement avant le tir
        AND (
               c.type_id IN (1, 3)                 -- Pass, TakeOn
            OR (c.type_id = 4 AND c.outcome_id = 1) -- faute obtenue
            OR c.type_id IN (13, 14, 15, 16)       -- tir précédent (rebond)
        )
)

-- ── SÉLECTION : on ne garde que les 2 premières actions avant chaque tir ──────
SELECT
    match_id,
    shot_row_num,
    sca_order,                                   -- (match_id, shot_row_num, sca_order) = clé
    shot_event_id,
    chain_id,
    season,
    league_source,
    scraped_at,
    attacking_team_id,
    shot_taker_player_id,
    creator_player_id,
    action_type,
    action_row_num,
    is_goal      AS is_gca                       -- crédit aussi GCA si le tir est un but
FROM sca
WHERE sca_order <= 2
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "shot_row_num"                                               AS int_shot_row_num,
        "sca_order"                                                  AS int_sca_order,
        CAST(shot_event_id AS VARCHAR)                               AS str_shot_event_id,
        "chain_id"                                                   AS str_chain_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        CAST(attacking_team_id AS VARCHAR)                           AS str_attacking_team_id,
        CAST(shot_taker_player_id AS VARCHAR)                        AS str_shot_taker_player_id,
        CAST(creator_player_id AS VARCHAR)                           AS str_creator_player_id,
        "action_type"                                                AS str_action_type,
        "action_row_num"                                             AS int_action_row_num,
        "is_gca"                                                     AS bool_is_gca
    FROM mdl_body
)

SELECT * FROM mdl_out
