{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'shot_row_num', 'sca_order'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_shot_creating_actions'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

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

WITH

-- ── FILTRE INCRÉMENTAL (même patron que player_possession_chains) ──────────────
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM {{ ref('player_possession_chains') }}
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
    FROM {{ ref('player_possession_chains') }} c
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
