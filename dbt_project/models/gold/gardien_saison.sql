{{
    config(
        materialized='incremental',
        unique_key=['keeper_id', 'season'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='gardien_saison'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.gardien_saison — grain (keeper_id, season)
-- Famille CDC 9 (profil gardien). Classe temporelle : SEASON-LAG.
--
-- Contrainte data actée : int_keeper_psxg est DÉJÀ au grain (keeper, saison) —
-- il n'existe pas de PSxG au niveau match. Le SEASON-LAG est donc « saison
-- précédente » (pas de snapshot as-of-date possible faute de grain match).
-- Toutes les saisons 2017→2025 sont présentes (pas de trou).
--
-- Chaque ligne (keeper, saison S) porte le profil de la saison S-1 → aucune
-- info de la saison courante, anti-leakage garanti par construction.
--
-- Un gardien à faible volume la saison N-1 a un PSxG bruité : keeper_shots_faced_lag
-- est exposé comme signal de confiance (seuil de bascule = calibration famille 11).
-- keeper_distribution_lag (feature 67, À CONSTRUIRE depuis int_whoscored_player_match)
-- sera ajoutée dans une passe ultérieure.
-- ══════════════════════════════════════════════════════════════════════════════

WITH

-- 1) Agrégation au grain (keeper, saison) : un gardien peut avoir 2 championnats
--    dans une saison (transfert / promotion) → on somme les volumes.
keeper_season AS (
    SELECT
        keeper_id,
        season,
        SUM(shots_faced)     AS shots_faced,
        SUM(saves)           AS saves,
        SUM(goals_conceded)  AS goals_conceded,
        SUM(psxg_faced)      AS psxg_faced,
        SUM(psxg_plus_minus) AS psxg_plus_minus
    FROM {{ ref('int_keeper_psxg') }}
    GROUP BY keeper_id, season
),

-- 2) Profil = valeurs de la saison N-1, rattachées par jointure explicite sur
--    la saison calendaire précédente. NULL si le gardien n'a pas de saison N-1
--    dans les données (recrue / 1re saison) → imputation famille 11 en aval.
lagged AS (
    SELECT
        cur.keeper_id,
        cur.season,
        prev.psxg_plus_minus                              AS keeper_psxg_plus_minus_lag,
        prev.psxg_plus_minus / NULLIF(prev.shots_faced, 0) AS keeper_psxg_per_shot_lag,
        prev.saves::DOUBLE   / NULLIF(prev.shots_faced, 0) AS keeper_save_pct_lag,
        prev.shots_faced                                  AS keeper_shots_faced_lag
    FROM keeper_season cur
    LEFT JOIN keeper_season prev
        ON prev.keeper_id = cur.keeper_id
       AND prev.season = (CAST(LEFT(cur.season, 4) AS INTEGER) - 1)::VARCHAR || '-' || LEFT(cur.season, 4)
)

SELECT *,
    -- [Famille 11, feature 79] Fiabilité du profil PSxG selon le volume de tirs
    -- affrontés la saison N-1 (seuils par défaut, à calibrer). 'none' → imputation.
    CASE WHEN keeper_shots_faced_lag IS NULL THEN 'none'
         WHEN keeper_shots_faced_lag >= 50   THEN 'high'
         WHEN keeper_shots_faced_lag >= 15   THEN 'medium'
         ELSE 'low' END AS profile_confidence_flag
FROM lagged

{% if is_incremental() %}
WHERE season > (SELECT MAX(season) FROM {{ this }})
{% endif %}
