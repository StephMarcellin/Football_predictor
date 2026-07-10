{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='team_network_features'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- Source de référence : player_network_centrality.
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_network_centrality') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_network_centrality') }}
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
    FROM {{ ref('player_network_centrality') }}
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
        
    FROM {{ ref('player_passes_raw') }}
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