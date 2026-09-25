{{
    config(
        materialized='incremental',
        unique_key=['str_player_id', 'str_zone_5x5', 'str_season'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='joueur_zone_saison'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.joueur_zone_saison — grain (player_id, zone_5x5, season) — format tall 5×5
-- Familles CDC 5 (profils offensifs zonaux) et 6 (défensifs). Classe SEASON-LAG.
--   Offensif : off_touch_share_by_zone (feature 34), off_shot_volume_by_zone
--     (feature 36). Défensif : def_duel_win_rate_by_zone (feature 45, coords
--     retournées vers la cage du défenseur). Source : int_player_zone_season.
--   RESTE À FAIRE : off_danger_by_zone (event_values — séparer off/def) ;
--     def_threat_conceded (threat_conceded, sans coords → plutôt non-zonal).
--
-- Chaque ligne (joueur, cellule, saison S) porte le profil de la saison S-1
-- (repli saison précédente : par cellule 5×5, l'échantillon d'une seule saison
-- est déjà mince). Anti-leakage par construction. n_matches_prev = compteur
-- d'observations, base de l'imputation famille 11.
-- Convention cellule : z = profondeur (x), c = couloir (y), bornes tous les 20.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_player_zone_season lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_player_zone_season AS (
    SELECT
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        str_zone_5x5                                                 AS "zone_5x5",
        str_season                                                   AS "season",
        dec_avg_touch_share                                          AS "avg_touch_share",
        int_n_matches                                                AS "n_matches",
        CAST(int_total_shots AS HUGEINT)                             AS "total_shots",
        int_n_duels                                                  AS "n_duels",
        CAST(int_duels_won AS HUGEINT)                               AS "duels_won",
        dec_total_danger                                             AS "total_danger",
        CAST(int_total_progressive AS HUGEINT)                       AS "total_progressive",
        CAST(int_total_crosses AS HUGEINT)                           AS "total_crosses",
        int_total_def_actions                                        AS "total_def_actions"
    FROM {{ ref('int_player_zone_season') }}
),

mdl_body AS (
SELECT
    cur.player_id,
    cur.zone_5x5,
    cur.season,
    prev.avg_touch_share                          AS off_touch_share_by_zone_lag,
    prev.total_shots / NULLIF(prev.n_matches, 0)  AS off_shot_volume_by_zone_lag,
    prev.total_danger / NULLIF(prev.n_matches, 0) AS off_danger_by_zone_lag,
    -- [Famille 5] feature 41 : actions progressives par cellule et par match.
    prev.total_progressive / NULLIF(prev.n_matches, 0) AS off_progressive_actions_by_zone_lag,
    -- [Famille 5] feature 37 : volume de centres (définition géométrique) par cellule.
    prev.total_crosses / NULLIF(prev.n_matches, 0)     AS off_cross_volume_by_zone_lag,
    prev.duels_won  / NULLIF(prev.n_duels, 0)     AS def_duel_win_rate_by_zone_lag,
    -- [Famille 6] densité d'actions défensives par cellule et par match (feature 55).
    prev.total_def_actions / NULLIF(prev.n_matches, 0) AS def_actions_by_zone_lag,
    prev.n_duels                                  AS n_duels_prev,
    prev.n_matches                                AS n_matches_prev,
    -- [Famille 11, feature 79] Fiabilité du profil selon le volume d'observation
    -- (seuils par défaut, à calibrer). 'none' → cible d'imputation KNN.
    CASE WHEN prev.n_matches IS NULL THEN 'none'
         WHEN prev.n_matches >= 10   THEN 'high'
         WHEN prev.n_matches >= 3    THEN 'medium'
         ELSE 'low' END                           AS profile_confidence_flag
FROM in_int_player_zone_season cur
LEFT JOIN in_int_player_zone_season prev
    ON  prev.player_id = cur.player_id
    AND prev.zone_5x5  = cur.zone_5x5
    AND prev.season = (CAST(LEFT(cur.season, 4) AS INTEGER) - 1)::VARCHAR || '-' || LEFT(cur.season, 4)

{% if is_incremental() %}
WHERE cur.season > (SELECT MAX(season) FROM (
    SELECT
            CAST(str_player_id AS INTEGER)                               AS "player_id",
            str_zone_5x5                                                 AS "zone_5x5",
            str_season                                                   AS "season",
            dec_off_touch_share_by_zone_lag                              AS "off_touch_share_by_zone_lag",
            dec_off_shot_volume_by_zone_lag                              AS "off_shot_volume_by_zone_lag",
            dec_off_danger_by_zone_lag                                   AS "off_danger_by_zone_lag",
            dec_off_progressive_actions_by_zone_lag                      AS "off_progressive_actions_by_zone_lag",
            dec_off_cross_volume_by_zone_lag                             AS "off_cross_volume_by_zone_lag",
            dec_def_duel_win_rate_by_zone_lag                            AS "def_duel_win_rate_by_zone_lag",
            dec_def_actions_by_zone_lag                                  AS "def_actions_by_zone_lag",
            int_n_duels_prev                                             AS "n_duels_prev",
            int_n_matches_prev                                           AS "n_matches_prev",
            str_profile_confidence_flag                                  AS "profile_confidence_flag"
        FROM {{ this }}
    ))
{% endif %}
),

mdl_out AS (
    SELECT
        CAST(player_id AS VARCHAR)                                   AS str_player_id,
        "zone_5x5"                                                   AS str_zone_5x5,
        "season"                                                     AS str_season,
        "off_touch_share_by_zone_lag"                                AS dec_off_touch_share_by_zone_lag,
        "off_shot_volume_by_zone_lag"                                AS dec_off_shot_volume_by_zone_lag,
        "off_danger_by_zone_lag"                                     AS dec_off_danger_by_zone_lag,
        "off_progressive_actions_by_zone_lag"                        AS dec_off_progressive_actions_by_zone_lag,
        "off_cross_volume_by_zone_lag"                               AS dec_off_cross_volume_by_zone_lag,
        "def_duel_win_rate_by_zone_lag"                              AS dec_def_duel_win_rate_by_zone_lag,
        "def_actions_by_zone_lag"                                    AS dec_def_actions_by_zone_lag,
        "n_duels_prev"                                               AS int_n_duels_prev,
        "n_matches_prev"                                             AS int_n_matches_prev,
        "profile_confidence_flag"                                    AS str_profile_confidence_flag
    FROM mdl_body
)

SELECT * FROM mdl_out
