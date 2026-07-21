{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id', 'player_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='features_players'
    )
}}

{% if execute %}
    {% do run_query("SET temp_directory='C:/Users/marce/AppData/Local/Temp/duckdb_dbt'") %}
{% endif %}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.features_players — Étape 6.1
-- Profil de forme d'un joueur AVANT chaque match : moyenne de ses métriques
-- sur ses 5 apparitions précédentes de la saison (le match courant est exclu).
-- Grain : 1 ligne par (match_id, team_id, player_id).
-- Anti-leakage : fenêtre ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING.
-- Périmètre : uniquement les 4 championnats couverts par WhoScored ; NULL ailleurs.
-- ══════════════════════════════════════════════════════════════════════════════

{% set w = 5 %}
-- Centres des 5 tranches de terrain (bornes 0-20-40-60-80-100 → centres)
{% set centers = [10, 30, 50, 70, 90] %}

WITH

-- ── 1. xG-chain agrégé au niveau joueur-match ────────────────────────────────
-- player_xg_chain est au grain (match_id, chain_id, player_id) et ne contient
-- que les chaînes se terminant par un tir. On somme le crédit uniforme `xgchain`
-- (= xG du tir terminal) sur toutes les chaînes où le joueur est impliqué.
xg_chain_agg AS (
    SELECT
        match_id,
        chain_team_id       AS team_id,
        player_id,
        SUM(xgchain)        AS xg_chain_match
    FROM {{ ref('player_xg_chain') }}
    GROUP BY match_id, chain_team_id, player_id
),

-- ── 2. Une ligne par joueur-match, métriques brutes prêtes à rouler ──────────
-- Spine = player_match_stats (grain joueur-match complet). On y greffe la
-- centralité réseau et le xG-chain. La position moyenne (zone_x/zone_y) est
-- approximée depuis les 25 pourcentages de zones, pondérés par le centre de
-- chaque cellule.
per_match AS (
    SELECT
        p.match_id,
        p.team_id,
        p.player_id,
        p.date,
        p.season,
        p.league_source,

        -- Contribution offensive
        COALESCE(x.xg_chain_match, 0)   AS xg_chain,
        p.xg_contribution,
        p.n_progressive_passes,
        p.n_key_passes,
        p.n_shots,

        -- Rôle dans le réseau de passes
        nc.pass_share,
        nc.betweenness_proxy,
        nc.creative_rate,

        -- Duels (aérien et sol traités séparément)
        p.aerial_win_rate,
        CASE WHEN p.n_tackles > 0
             THEN CAST(p.n_tackles_won AS DOUBLE) / p.n_tackles
        END                             AS tackle_win_rate,

        -- Activité défensive
        (p.n_tackles_won + p.n_interceptions
         + p.n_clearances + p.n_ball_recoveries) AS defensive_actions,
        p.n_clearances,

        -- Volume
        p.n_touches,

        -- Position moyenne approximée depuis les 25 zones (axe longueur = x)
        (
        {%- for i in range(5) %}
          {%- for j in range(5) %}
            {%- if not (i == 0 and j == 0) %} + {% endif -%}
            COALESCE(p.pct_z{{ i + 1 }}_c{{ j + 1 }}, 0) * {{ centers[i] }}
          {%- endfor %}
        {%- endfor %}
        )                               AS zone_x,

        -- Position moyenne approximée depuis les 25 zones (axe largeur = y)
        (
        {%- for i in range(5) %}
          {%- for j in range(5) %}
            {%- if not (i == 0 and j == 0) %} + {% endif -%}
            COALESCE(p.pct_z{{ i + 1 }}_c{{ j + 1 }}, 0) * {{ centers[j] }}
          {%- endfor %}
        {%- endfor %}
        )                               AS zone_y

    FROM {{ ref('player_match_stats') }} p

    LEFT JOIN {{ ref('player_network_centrality') }} nc
        ON  nc.match_id  = p.match_id
        AND nc.team_id   = p.team_id
        AND nc.player_id = p.player_id

    LEFT JOIN xg_chain_agg x
        ON  x.match_id  = p.match_id
        AND x.team_id   = p.team_id
        AND x.player_id = p.player_id
),

-- ── 3. Fenêtres rolling 5, anti-leakage strict ───────────────────────────────
-- La fenêtre part du joueur et se réinitialise à chaque saison.
-- ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING : on ne regarde QUE les 5 matchs
-- d'avant, jamais le match courant. n_matchs_window compte les matchs réellement
-- disponibles (0 à 5) — 0 = première apparition de la saison → toutes les
-- moyennes sont NULL.
rolled AS (
    SELECT
        match_id,
        team_id,
        player_id,
        date,
        season,
        league_source,

        {% set win %}PARTITION BY player_id, season ORDER BY date ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING{% endset %}

        AVG(xg_chain)              OVER ({{ win }}) AS avg_xg_chain_5,
        AVG(xg_contribution)       OVER ({{ win }}) AS avg_xg_contribution_5,
        AVG(n_progressive_passes)  OVER ({{ win }}) AS avg_progressive_passes_5,
        AVG(n_key_passes)          OVER ({{ win }}) AS avg_key_passes_5,
        AVG(n_shots)               OVER ({{ win }}) AS avg_shots_5,

        AVG(pass_share)            OVER ({{ win }}) AS avg_pass_share_5,
        AVG(betweenness_proxy)     OVER ({{ win }}) AS avg_betweenness_5,
        AVG(creative_rate)         OVER ({{ win }}) AS avg_creative_rate_5,

        AVG(aerial_win_rate)       OVER ({{ win }}) AS avg_aerial_win_rate_5,
        AVG(tackle_win_rate)       OVER ({{ win }}) AS avg_tackle_win_rate_5,

        AVG(defensive_actions)     OVER ({{ win }}) AS avg_defensive_actions_5,
        AVG(n_clearances)          OVER ({{ win }}) AS avg_clearances_5,

        AVG(zone_x)                OVER ({{ win }}) AS avg_zone_x_5,
        AVG(zone_y)                OVER ({{ win }}) AS avg_zone_y_5,
        AVG(n_touches)             OVER ({{ win }}) AS avg_touches_5,

        COUNT(*)                   OVER ({{ win }}) AS n_matchs_window

    FROM per_match
)

SELECT * FROM rolled

{% if is_incremental() %}
WHERE (match_id::VARCHAR || '_' || team_id || '_' || player_id) NOT IN (
    SELECT (match_id::VARCHAR || '_' || team_id || '_' || player_id)
    FROM {{ this }}
)
{% endif %}
