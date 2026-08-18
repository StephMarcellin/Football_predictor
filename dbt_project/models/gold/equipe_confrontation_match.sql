{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='equipe_confrontation_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.equipe_confrontation_match — grain (match_id, team_id) directionnel (A→B)
-- Famille CDC 7, feature 56 : matchup_high_press_vs_buildup. Croise le PRESSING de
-- l'adversaire B avec la CAPACITÉ DE RELANCE SOUS PRESSION de l'équipe A, à partir
-- des deux mesures PPDA déjà roulées dans equipe_match (aucune brique amont neuve).
--
-- Rappel PPDA (passes allowed per defensive action) :
--   • ppda_rolling      = pressing de l'équipe elle-même. PLUS BAS = presse plus fort.
--   • ppda_allowed      = à quel point l'équipe SE FAIT presser. PLUS HAUT = relance
--                         librement (fait beaucoup de passes avant l'action adverse).
--
-- Confrontation (repère de A) :
--   • opp_press_ppda            = ppda_rolling de B  → intensité du pressing subi.
--   • self_buildup_resistance   = ppda_allowed de A  → capacité de A à relancer.
--   • matchup_high_press_vs_buildup = self_buildup_resistance / opp_press_ppda.
--       >1 → A relance au-dessus du pressing de B ; <1 → le pressing de B étouffe
--       la relance de A. ⚠️ SENS À VALIDER empiriquement (signe/importance dans le
--       modèle) — la sémantique du croisement ppda × ppda_allowed est subtile.
--
-- Composante xgbuildup du CDC INCLUSE (self_team_xgbuildup_lag) : somme du
-- off_xgbuildup_per90_lag des 11 titulaires (convention team_corridor_profile —
-- on somme les contributions des joueurs présents ; per-90 intègre déjà
-- l'implication). Qualité de construction du onze, complémentaire du ratio ppda.
-- Classe temporelle : DÉRIVÉ (rolling + SEASON-LAG déjà anti-leakage). Dénominateur
-- ppda jamais nul (min ≈ 2,5) → pas de garde.
-- ══════════════════════════════════════════════════════════════════════════════

WITH team_buildup AS (
    SELECT l.match_id, l.team_id,
        SUM(COALESCE(js.off_xgbuildup_per90_lag, 0)) AS team_xgbuildup_lag
    FROM {{ ref('int_whoscored_lineup') }} l
    JOIN {{ ref('joueur_saison') }} js
        ON  js.match_id  = l.match_id
        AND js.team_id   = l.team_id
        AND js.player_id = l.player_id
    WHERE l.start_minute = 0 AND l.match_id IS NOT NULL
    GROUP BY l.match_id, l.team_id
)

SELECT
    a.match_id,
    a.team_id      AS team_id,
    a.opponent_id  AS opponent_id,

    {% for w in [3, 5, 10] %}
    b.ppda_rolling_{{ w }}         AS opp_press_ppda_rolling_{{ w }},
    a.ppda_allowed_rolling_{{ w }} AS self_buildup_resistance_rolling_{{ w }},
    a.ppda_allowed_rolling_{{ w }} / NULLIF(b.ppda_rolling_{{ w }}, 0)
        AS matchup_high_press_vs_buildup_rolling_{{ w }},
    {% endfor %}

    -- Qualité de construction du onze (feature 56, composante xgbuildup).
    tba.team_xgbuildup_lag AS self_team_xgbuildup_lag,
    tbb.team_xgbuildup_lag AS opp_team_xgbuildup_lag

FROM {{ ref('equipe_match') }} a
LEFT JOIN {{ ref('equipe_match') }} b
    ON  b.match_id = a.match_id
    AND b.team_id  = a.opponent_id
LEFT JOIN team_buildup tba
    ON  tba.match_id = a.match_id AND tba.team_id = a.team_id
LEFT JOIN team_buildup tbb
    ON  tbb.match_id = a.match_id AND tbb.team_id = a.opponent_id

-- Exclut les lignes à clé nulle d'equipe_match (2 lignes orphelines match_id/team_id
-- NULL → défaut de source à corriger en amont). Sans confrontation valide possible.
WHERE a.match_id IS NOT NULL AND a.team_id IS NOT NULL

{% if is_incremental() %}
AND a.match_id IN (
    SELECT match_id FROM {{ ref('equipe_match') }}
    EXCEPT SELECT match_id FROM {{ this }}
)
{% endif %}
