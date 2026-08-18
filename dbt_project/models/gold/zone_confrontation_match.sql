{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'attacking_team_id', 'attack_corridor'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='zone_confrontation_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.zone_confrontation_match — grain (match_id, attacking_team_id, attack_corridor)
-- Famille CDC 7 : croisement menace offensive de A × vulnérabilité défensive de B,
-- PAR COULOIR (3×3 regroupé), une fois les compositions connues. Deux sens par
-- match (A→B et B→A), trois couloirs → 6 lignes par match.
--
-- MIROIR (crucial) : quand A attaque à gauche, il affronte la DROITE de B (les
-- deux équipes se font face). L'appariement est donc gauche↔droit, axe↔axe.
--
-- Score (décision actée) : PRODUIT menace × vulnérabilité —
--   matchup_danger = off_strength(A, couloir) × (1 − def_solidity(B, couloir miroir)).
-- NULL si la solidité adverse est inconnue (aucun duel) → imputation famille 11.
-- Features dérivées (matchup_cross/dribble/central, high_press) : passes suivantes.
-- ══════════════════════════════════════════════════════════════════════════════

SELECT
    a.match_id,
    a.team_id      AS attacking_team_id,
    a.opponent_id  AS defending_team_id,
    a.corridor     AS attack_corridor,
    a.off_strength,
    b.def_solidity AS opp_def_solidity,
    a.off_strength * (1 - b.def_solidity) AS matchup_danger_by_corridor,
    -- [Feature 53] matchup_cross_threat — VERSION RÉDUITE : capacité de centre de A
    -- dans le couloir × fragilité défensive de B dans le couloir miroir. La
    -- composante aérienne du CDC (def_aerial_win_rate des cibles / def_cross_conceded)
    -- n'est PAS encore incluse : features famille 6 zonales absentes → TODO.
    a.off_cross_strength   * (1 - b.def_solidity) AS matchup_cross_threat,
    -- [Feature 54] matchup_dribble_threat — dribbleurs/progression de A dans le
    -- couloir × vulnérabilité au duel de B (couloir miroir). Complet.
    a.off_dribble_strength * (1 - b.def_solidity) AS matchup_dribble_threat,
    -- [Feature 55] matchup_central_control — UNIQUEMENT sur la ligne 'axe' (axe↔axe).
    -- Progression centrale de A rapportée à la densité défensive centrale de B.
    -- >1 → A progresse dans l'axe plus que B n'y défend. ⚠️ SENS À VALIDER
    -- empiriquement. Composante def_threat_conceded_by_zone du CDC ABSENTE
    -- (threat_conceded sans coordonnées) → densité = actions défensives seules (TODO).
    CASE WHEN a.corridor = 'axe'
         THEN a.off_central_progression / NULLIF(b.def_central_density, 0)
         ELSE NULL END AS matchup_central_control,
    -- Composantes brutes crossées (exposées pour laisser le modèle apprendre).
    CASE WHEN a.corridor = 'axe' THEN a.off_central_progression ELSE NULL END AS self_central_progression,
    CASE WHEN a.corridor = 'axe' THEN a.off_central_touch       ELSE NULL END AS self_central_touch,
    CASE WHEN a.corridor = 'axe' THEN b.def_central_density      ELSE NULL END AS opp_central_def_density
FROM {{ ref('team_corridor_profile') }} a
JOIN {{ ref('team_corridor_profile') }} b
    ON  b.match_id = a.match_id
    AND b.team_id  = a.opponent_id
    AND b.corridor = CASE a.corridor
                        WHEN 'gauche' THEN 'droit'
                        WHEN 'droit'  THEN 'gauche'
                        ELSE 'axe'
                     END

{% if is_incremental() %}
WHERE a.match_id IN (
    SELECT match_id FROM {{ ref('team_corridor_profile') }}
    EXCEPT SELECT match_id FROM {{ this }}
)
{% endif %}
