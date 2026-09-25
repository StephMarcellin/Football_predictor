{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='team_network_features'
    )
}}

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- player_network_centrality lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_network_centrality AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        int_degree_out                                               AS "degree_out",
        int_degree_in                                                AS "degree_in",
        CAST(int_weighted_degree_out AS HUGEINT)                     AS "weighted_degree_out",
        CAST(int_weighted_degree_in AS HUGEINT)                      AS "weighted_degree_in",
        CAST(int_n_creative_out AS HUGEINT)                          AS "n_creative_out",
        CAST(int_n_progressive_out AS HUGEINT)                       AS "n_progressive_out",
        dec_pass_share                                               AS "pass_share",
        dec_creative_rate                                            AS "creative_rate",
        dec_betweenness_proxy                                        AS "betweenness_proxy"
    FROM {{ ref('player_network_centrality') }}
),

-- player_passes_raw lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_passes_raw AS (
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
    FROM {{ ref('player_passes_raw') }}
),

mdl_body AS (
WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- Source de référence : player_network_centrality.
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_network_centrality
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            int_n_players                                                AS "n_players",
            CAST(int_n_edges AS HUGEINT)                                 AS "n_edges",
            dec_network_density                                          AS "network_density",
            dec_top_creator_share                                        AS "top_creator_share",
            dec_avg_betweenness                                          AS "avg_betweenness",
            dec_network_entropy                                          AS "network_entropy",
            dec_centroid_x                                               AS "centroid_x",
            dec_centroid_y                                               AS "centroid_y",
            dec_centroid_x_progressive                                   AS "centroid_x_progressive",
            dec_centroid_y_progressive                                   AS "centroid_y_progressive"
        FROM {{ this }}
    ))
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_player_network_centrality
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- NETWORK_AGG
-- Agrégation des métriques joueur → équipe depuis player_network_centrality.
--
-- n_players         : nombre de joueurs actifs dans le réseau de passes
-- n_edges           : somme des degree_out = nombre total d'arêtes dirigées
--                     existantes dans le graphe de l'équipe
-- top_creator_share : pass_share du joueur dominant — mesure la dépendance
--                     de l'équipe à un seul joueur
-- avg_betweenness   : moyenne des betweenness_proxy — indique si l'équipe
--                     a plusieurs joueurs-ponts ou un seul
-- network_entropy   : formule de Shannon normalisée par LN(n_players)
--                     → 0 = tout passe par un seul joueur
--                     → 1 = distribution parfaitement uniforme
-- ══════════════════════════════════════════════════════════════════════════════
network_agg AS (
    SELECT
        match_id,
        team_id,
        season,
        league_source,
        COUNT(*)                                                        AS n_players,
        SUM(degree_out)                                                 AS n_edges,
        MAX(pass_share)                                                 AS top_creator_share,
        ROUND(AVG(betweenness_proxy), 4)                               AS avg_betweenness,
        ROUND(
            - SUM(pass_share * LN(NULLIF(pass_share, 0)))
            / NULLIF(LN(COUNT(*)), 0)
        , 4)                                                            AS network_entropy
    FROM in_player_network_centrality
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id, season, league_source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- SPATIAL_AGG
-- Centroïde des passes depuis player_passes_raw.
-- On moyenne les coordonnées de départ (x, y) de toutes les passes réussies.
-- centroid_x proche de 100 = équipe qui joue haut sur le terrain
-- centroid_x proche de 0   = équipe qui joue bas / subit
-- centroid_y proche de 50  = jeu axial
-- centroid_y < 50 ou > 50  = jeu décalé sur un côté
-- ══════════════════════════════════════════════════════════════════════════════
spatial_agg AS (
    SELECT
        match_id,
        team_id,
        ROUND(AVG(x), 2)                                               AS centroid_x,
        ROUND(AVG(y), 2)                                               AS centroid_y,
        ROUND(AVG(CASE WHEN is_progressive THEN x END), 2)            AS centroid_x_progressive,
        ROUND(AVG(CASE WHEN is_progressive THEN y END), 2)            AS centroid_y_progressive
        
    FROM in_player_passes_raw
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FINAL
-- Joint network_agg et spatial_agg.
-- Calcule network_density = n_edges / (n_players * (n_players - 1))
-- qui représente le ratio arêtes existantes / arêtes possibles dans
-- un graphe dirigé complet à n_players nœuds.
-- ══════════════════════════════════════════════════════════════════════════════
final AS (
    SELECT
        n.match_id,
        n.team_id,
        n.season,
        n.league_source,

        -- Métriques structurelles du réseau
        n.n_players,
        n.n_edges,
        ROUND(
            n.n_edges
            / NULLIF(n.n_players * (n.n_players - 1), 0)
        , 4)                                                            AS network_density,

        -- Métriques de concentration
        ROUND(n.top_creator_share, 4)                                  AS top_creator_share,
        n.avg_betweenness,

        -- Entropie normalisée
        n.network_entropy,

        -- Centroïde spatial
        s.centroid_x,
        s.centroid_y,

        -- Centroïde spatial — passes progressives uniquement
        s.centroid_x_progressive,
        s.centroid_y_progressive

    FROM network_agg n
    LEFT JOIN spatial_agg s
        ON  s.match_id = n.match_id
        AND s.team_id  = n.team_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM final
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        "n_players"                                                  AS int_n_players,
        CAST(n_edges AS BIGINT)                                      AS int_n_edges,
        "network_density"                                            AS dec_network_density,
        "top_creator_share"                                          AS dec_top_creator_share,
        "avg_betweenness"                                            AS dec_avg_betweenness,
        "network_entropy"                                            AS dec_network_entropy,
        "centroid_x"                                                 AS dec_centroid_x,
        "centroid_y"                                                 AS dec_centroid_y,
        "centroid_x_progressive"                                     AS dec_centroid_x_progressive,
        "centroid_y_progressive"                                     AS dec_centroid_y_progressive
    FROM mdl_body
)

SELECT * FROM mdl_out
