{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_shot_row_num', 'int_sca_order'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='intermediate_shot_creating_actions'
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
-- Source = intermediate_possession_chains : donne le chain_id, l'ordre temporel et
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

-- intermediate_possession_chains lu sous ses noms refondus, remappé vers les noms de travail du modèle
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
    FROM {{ ref('intermediate_possession_chains') }}
),

mdl_body AS (
WITH

-- ── FILTRE INCRÉMENTAL (même patron que intermediate_possession_chains) ──────────────
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
-- ══════════════════════════════════════════════════════════════════════════════
-- CHAINS_WITH_SHOT
-- Les chaines qui contiennent au moins un tir : les seules a pouvoir produire
-- une action creatrice de tir. Mesure du 05/10/2026 sur football_dev :
-- 62 548 chaines sur 786 542, soit 8,0 %.
--
-- Sans ce filtre, le ROW_NUMBER() de chain_seq numerote TOUTES les lignes de
-- TOUTES les chaines (3,95 M en dev, ~58 M en prod) et impose donc un tri
-- complet — alors que 92 % de ces chaines ne produiront jamais une ligne.
-- Le filtre est semantiquement neutre : pas de tir, pas de SCA.
-- ══════════════════════════════════════════════════════════════════════════════
chains_with_shot AS (
    SELECT DISTINCT match_id, chain_id
    FROM in_player_possession_chains
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND type_id IN (13, 14, 15, 16)
),

-- ── Evenements des chaines a tir, ordonnes dans la chaine ────────────────────
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
    JOIN chains_with_shot cws
        ON  cws.match_id = c.match_id
        AND cws.chain_id = c.chain_id
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

-- ══════════════════════════════════════════════════════════════════════════════
-- FREEKICK_ORIGIN_FOUL
-- La faute obtenue qui a provoque chaque chaine declenchee sur coup franc.
--
-- Pourquoi ce CTE existe : une faute obtenue TERMINE sa chaine (regle de
-- possession_group_increment dans intermediate_possession_chains). Elle vit
-- donc dans la chaine N, alors que le coup franc et le tir sont dans la N+1.
-- Le JOIN de sca_candidates, borne par chain_id, ne pouvait jamais les
-- rapprocher : mesure du 05/10/2026, 1,2 % seulement des chaines a tir
-- contenaient une faute obtenue, contre 86 % des chaines free_kick qui en ont
-- une juste avant leur debut. Resultat : la branche type_id=4 du filtre de
-- sca_candidates etait structurellement morte et ~5 550 actions creatrices
-- par saison manquaient.
--
-- La borne est deterministe, sans fenetre numerique : la DERNIERE faute
-- obtenue avant le premier row_num de la chaine, par l'equipe en possession.
-- chain_trigger = 'free_kick' garantit deja que la chaine demarre sur un coup
-- franc, donc la faute la plus proche en amont est celle qui l'a provoquee.
-- Un seuil fixe (« 3 lignes avant ») echouerait sur les cas ou un carton ou un
-- remplacement s'intercale — justement les fautes les plus significatives.
--
-- PARTITION BY (match_id, team_id) porte la condition d'equipe : la faute a
-- team_id = equipe qui l'obtient (outcome_id = 1), la cible a chain_team_id.
-- Les deux ne se rencontrent dans une meme partition que si elles coincident.
-- ══════════════════════════════════════════════════════════════════════════════
fk_origin_stream AS (
    -- Porteuses : les fautes obtenues.
    SELECT
        match_id,
        team_id,
        row_num,
        {'pid': player_id, 'tn': type_name, 'rn': row_num} AS foul,
        CAST(NULL AS VARCHAR)                              AS chain_id,
        0                                                  AS is_target
    FROM in_player_possession_chains
    WHERE match_id IN (SELECT match_id FROM new_matches)
      AND type_id    = 4
      AND outcome_id = 1

    UNION ALL

    -- Cibles : le premier evenement de chaque chaine declenchee sur coup franc.
    SELECT
        match_id,
        chain_team_id AS team_id,
        first_row     AS row_num,
        NULL          AS foul,
        chain_id,
        1             AS is_target
    FROM (
        -- Restreint aux chaines qui contiennent un tir : les autres ne seront
        -- de toute facon jamais jointes a shots.
        SELECT pc.match_id, pc.chain_id, pc.chain_team_id, MIN(pc.row_num) AS first_row
        FROM in_player_possession_chains pc
        JOIN chains_with_shot cws
            ON  cws.match_id = pc.match_id
            AND cws.chain_id = pc.chain_id
        WHERE pc.match_id IN (SELECT match_id FROM new_matches)
          AND pc.chain_trigger = 'free_kick'
        GROUP BY 1, 2, 3
    )
),

freekick_origin_foul AS (
    SELECT
        match_id,
        team_id,
        chain_id,
        prev_foul['pid'] AS player_id,
        prev_foul['tn']  AS type_name,
        prev_foul['rn']  AS row_num
    FROM (
        SELECT
            match_id, team_id, chain_id, is_target,
            LAG(foul IGNORE NULLS) OVER (
                PARTITION BY match_id, team_id
                ORDER BY row_num, is_target
            ) AS prev_foul
        FROM fk_origin_stream
    )
    WHERE is_target = 1
      AND prev_foul IS NOT NULL
),

-- ══════════════════════════════════════════════════════════════════════════════
-- SCA_CANDIDATES
-- Deux sources : les actions eligibles internes a la chaine du tir, et la faute
-- obtenue qui a provoque le coup franc (hors chaine, seq_in_chain = 0 pour la
-- placer avant tout le reste dans le classement ORDER BY seq_in_chain DESC).
--
-- La faute occupe un RANG NORMAL, conformement a la definition Opta/FBref ou
-- « fouls drawn » est un type d'action creatrice : si trois passes la separent
-- du tir, elle sort du top 2 et n'est pas creditee. Le grain de la table
-- (match_id, shot_row_num, sca_order <= 2) est donc preserve.
-- ══════════════════════════════════════════════════════════════════════════════
sca_candidates AS (
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
        c.seq_in_chain
    FROM shots s
    JOIN chain_seq c
        ON  c.match_id     = s.match_id
        AND c.chain_id     = s.chain_id
        AND c.team_id      = s.attacking_team_id   -- cote equipe qui tire
        AND c.seq_in_chain < s.seq_in_chain        -- strictement avant le tir
        AND (
               c.type_id IN (1, 3)                 -- Pass, TakeOn
            OR c.type_id IN (13, 14, 15, 16)       -- tir precedent (rebond)
        )
    -- NB : plus de branche type_id=4 ici. Une faute obtenue n'est jamais dans
    -- la meme chaine que le tir ; elle arrive par l'UNION ci-dessous.

    UNION ALL

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
        f.player_id   AS creator_player_id,
        f.type_name   AS action_type,
        f.row_num     AS action_row_num,
        0             AS seq_in_chain              -- avant le 1er evenement
    FROM shots s
    JOIN freekick_origin_foul f
        ON  f.match_id = s.match_id
        AND f.chain_id = s.chain_id
        AND f.team_id  = s.attacking_team_id
),

sca AS (
    SELECT
        *,
        -- 1 = action juste avant le tir, 2 = l'action d'avant
        ROW_NUMBER() OVER (
            PARTITION BY match_id, shot_row_num
            ORDER BY seq_in_chain DESC
        ) AS sca_order
    FROM sca_candidates
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
