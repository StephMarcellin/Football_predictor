{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='player_network_centrality'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- FILTRE INCRÉMENTAL
-- On détecte les nouveaux matchs depuis player_network_passes,
-- qui est la source unique de cette table.
-- ══════════════════════════════════════════════════════════════════════════════
{% if is_incremental() %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_network_passes') }}
    WHERE match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_network_passes') }}
),
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- OUT_STATS
-- Métriques du joueur en tant qu'émetteur de passes.
-- Une ligne par (match_id, team_id, passer_id).
-- degree_out        : nombre de destinataires distincts
-- weighted_degree_out : volume total de passes émises (toutes arêtes confondues)
-- n_creative_out    : passes menant directement à un tir
-- n_progressive_out : passes avançant le ballon vers le but adverse
-- ══════════════════════════════════════════════════════════════════════════════
out_stats AS (
    SELECT
        match_id,
        team_id,
        passer_id                       AS player_id,
        season,
        league_source,
        COUNT(DISTINCT receiver_id)     AS degree_out,
        SUM(n_passes)                   AS weighted_degree_out,
        SUM(n_creative)                 AS n_creative_out,
        SUM(n_progressive)              AS n_progressive_out
    FROM {{ ref('player_network_passes') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id, passer_id, season, league_source
),

-- ══════════════════════════════════════════════════════════════════════════════
-- IN_STATS
-- Métriques du joueur en tant que récepteur de passes.
-- Une ligne par (match_id, team_id, receiver_id).
-- degree_in         : nombre de sources distinctes
-- weighted_degree_in : volume total de passes reçues
-- ══════════════════════════════════════════════════════════════════════════════
in_stats AS (
    SELECT
        match_id,
        team_id,
        receiver_id                     AS player_id,
        COUNT(DISTINCT passer_id)       AS degree_in,
        SUM(n_passes)                   AS weighted_degree_in
    FROM {{ ref('player_network_passes') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id, receiver_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- TEAM_STATS
-- Métriques globales de l'équipe dans ce match.
-- n_players         : nombre de joueurs actifs dans le réseau (ont passé au moins une fois)
-- total_passes_team : volume total de passes de l'équipe — dénominateur de pass_share
-- NULLIF sur (n_players * (n_players - 1)) protège la division du betweenness_proxy
-- quand n_players <= 1 (cas théoriquement impossible mais défensif).
-- ══════════════════════════════════════════════════════════════════════════════
team_stats AS (
    SELECT
        match_id,
        team_id,
        COUNT(DISTINCT passer_id)       AS n_players,
        SUM(n_passes)                   AS total_passes_team
    FROM {{ ref('player_network_passes') }}
    WHERE match_id IN (SELECT match_id FROM new_matches)
    GROUP BY match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FINAL
-- Joint out_stats, in_stats et team_stats.
-- Calcule les trois métriques dérivées :
--
-- pass_share        : part des passes de l'équipe émises par ce joueur
--                     = weighted_degree_out / total_passes_team
--
-- creative_rate     : ratio passes créatives sur total passes émises
--                     = n_creative_out / weighted_degree_out
--
-- betweenness_proxy : approximation de la centralité de passage
--                     = (degree_in * degree_out) / (n_players * (n_players - 1))
--                     Un joueur qui reçoit de sources variées ET redistribue
--                     vers des cibles variées est probablement un pont dans le réseau.
--
-- Les joueurs présents uniquement comme receiver (jamais comme passer) auront
-- degree_out = NULL après le LEFT JOIN — COALESCE les ramène à 0.
-- ══════════════════════════════════════════════════════════════════════════════
final AS (
    SELECT
        o.match_id,
        o.team_id,
        o.player_id,
        o.season,
        o.league_source,

        -- Degrés bruts
        COALESCE(o.degree_out, 0)                                       AS degree_out,
        COALESCE(i.degree_in,  0)                                       AS degree_in,

        -- Volumes pondérés
        COALESCE(o.weighted_degree_out, 0)                              AS weighted_degree_out,
        COALESCE(i.weighted_degree_in,  0)                              AS weighted_degree_in,

        -- Métriques de contribution offensive
        COALESCE(o.n_creative_out,    0)                                AS n_creative_out,
        COALESCE(o.n_progressive_out, 0)                                AS n_progressive_out,

        -- Part des passes de l'équipe portées par ce joueur
        ROUND(
            COALESCE(o.weighted_degree_out, 0)
            / NULLIF(t.total_passes_team, 0)
        , 4)                                                            AS pass_share,

        -- Taux de passes créatives parmi les passes émises
        ROUND(
            COALESCE(o.n_creative_out, 0)
            / NULLIF(o.weighted_degree_out, 0)
        , 4)                                                            AS creative_rate,

        -- Proxy de betweenness : diversité entrante × diversité sortante / paires possibles
        ROUND(
            (COALESCE(o.degree_out, 0) * COALESCE(i.degree_in, 0))
            / NULLIF(t.n_players * (t.n_players - 1), 0)
        , 4)                                                            AS betweenness_proxy

    FROM out_stats o
    LEFT JOIN in_stats i
        ON  i.match_id  = o.match_id
        AND i.team_id   = o.team_id
        AND i.player_id = o.player_id
    LEFT JOIN team_stats t
        ON  t.match_id = o.match_id
        AND t.team_id  = o.team_id
)

-- ══════════════════════════════════════════════════════════════════════════════
-- SELECT FINAL
-- ══════════════════════════════════════════════════════════════════════════════
SELECT * FROM final