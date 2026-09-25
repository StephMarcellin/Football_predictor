{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_attacking_team_id', 'str_attack_corridor'],
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
--
-- Refonte nommage : team_corridor_profile est lu sous ses nouveaux noms et remappé
-- vers les anciens (CTE corridor_in) — les sorties de ce modèle ne changent pas
-- (types compris : attacking/defending_team_id restent INTEGER).
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
WITH corridor_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        CAST(str_opponent_id AS BIGINT)   AS opponent_id,
        str_corridor                       AS corridor,
        dec_off_strength                   AS off_strength,
        dec_off_cross_strength             AS off_cross_strength,
        dec_off_dribble_strength           AS off_dribble_strength,
        dec_off_central_progression        AS off_central_progression,
        dec_off_central_touch              AS off_central_touch,
        dec_def_central_density            AS def_central_density,
        dec_def_solidity                   AS def_solidity
    FROM {{ ref('team_corridor_profile') }}
)

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
FROM corridor_in a
JOIN corridor_in b
    ON  b.match_id = a.match_id
    AND b.team_id  = a.opponent_id
    AND b.corridor = CASE a.corridor
                        WHEN 'gauche' THEN 'droit'
                        WHEN 'droit'  THEN 'gauche'
                        ELSE 'axe'
                     END

{% if is_incremental() %}
WHERE a.match_id IN (
    SELECT str_match_id FROM {{ ref('team_corridor_profile') }}
    EXCEPT SELECT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_attacking_team_id AS BIGINT)                        AS "attacking_team_id",
            CAST(str_defending_team_id AS BIGINT)                        AS "defending_team_id",
            str_attack_corridor                                          AS "attack_corridor",
            dec_off_strength                                             AS "off_strength",
            dec_opp_def_solidity                                         AS "opp_def_solidity",
            dec_matchup_danger_by_corridor                               AS "matchup_danger_by_corridor",
            dec_matchup_cross_threat                                     AS "matchup_cross_threat",
            dec_matchup_dribble_threat                                   AS "matchup_dribble_threat",
            dec_matchup_central_control                                  AS "matchup_central_control",
            dec_self_central_progression                                 AS "self_central_progression",
            dec_self_central_touch                                       AS "self_central_touch",
            dec_opp_central_def_density                                  AS "opp_central_def_density"
        FROM {{ this }}
    )
)
{% endif %}
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(attacking_team_id AS VARCHAR)                           AS str_attacking_team_id,
        CAST(defending_team_id AS VARCHAR)                           AS str_defending_team_id,
        "attack_corridor"                                            AS str_attack_corridor,
        "off_strength"                                               AS dec_off_strength,
        "opp_def_solidity"                                           AS dec_opp_def_solidity,
        "matchup_danger_by_corridor"                                 AS dec_matchup_danger_by_corridor,
        "matchup_cross_threat"                                       AS dec_matchup_cross_threat,
        "matchup_dribble_threat"                                     AS dec_matchup_dribble_threat,
        "matchup_central_control"                                    AS dec_matchup_central_control,
        "self_central_progression"                                   AS dec_self_central_progression,
        "self_central_touch"                                         AS dec_self_central_touch,
        "opp_central_def_density"                                    AS dec_opp_central_def_density
    FROM mdl_body
)

SELECT * FROM mdl_out
