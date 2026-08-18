{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='equipe_adversaire_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.equipe_adversaire_match — grain (match_id, team_id, opponent_id)
-- Famille CDC : 1 (confrontation directe H2H). Classe temporelle : H2H-CUTOFF.
--
-- h2h_history porte déjà les cumuls ANTÉRIEURS au match (vérif H3 : le match
-- courant n'est jamais compté). On se contente d'en DÉRIVER les taux.
--
-- Garde-fous issus de la vérif H3 :
--   • 14,8 % des lignes ont h2h_played NULL = première confrontation.
--   • Un TAUX sur zéro confrontation n'existe pas → on le laisse NULL
--     (« inconnu »), on ne le force PAS à 0 (0 % ≠ inconnu). L'imputation
--     famille 11 / la sélection de features gèrent le NULL en aval.
--   • Un COMPTE de confrontations, lui, vaut bien 0 quand il n'y en a pas
--     → COALESCE(..., 0) sur les compteurs uniquement.
-- ══════════════════════════════════════════════════════════════════════════════

WITH h2h AS (
    SELECT
        match_id, team_id, opponent_id, date, season, league_source,

        -- Compteurs de confiance (0 = pas d'historique, pas NULL)
        COALESCE(h2h_played, 0)      AS h2h_played,
        COALESCE(h2h_home_played, 0) AS h2h_home_played,

        -- Taux cumulés avant le match (NULL si aucune confrontation)
        h2h_wins::DOUBLE   / NULLIF(h2h_played, 0)      AS h2h_win_rate_cutoff,
        h2h_draws::DOUBLE  / NULLIF(h2h_played, 0)      AS h2h_draw_rate_cutoff,
        h2h_losses::DOUBLE / NULLIF(h2h_played, 0)      AS h2h_loss_rate_cutoff,
        h2h_home_wins::DOUBLE / NULLIF(h2h_home_played, 0) AS h2h_home_win_rate_cutoff,

        -- Moyennes sur les 10 dernières confrontations (déjà matérialisées)
        h2h_avg_gf_10,
        h2h_avg_ga_10,
        h2h_avg_xg_diff_10

    FROM {{ ref('h2h_history') }}
)

SELECT * FROM h2h

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
