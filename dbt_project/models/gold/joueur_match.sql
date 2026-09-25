{{
    config(
        materialized='table',
        schema='gold',
        alias='joueur_match'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- gold.joueur_match — grain (match_id, team_id, player_id) — 7e table de grain
-- Table d'assemblage du modèle BUTEURS (une ligne = un joueur du XI dans un match).
-- Réunit les features scorer SEASON-LAG (joueur_saison) et la feature propre 75 :
--   scorer_context_vs_opponent_style = Σ_couloir [ tirs du joueur dans le couloir
--   × vulnérabilité de l'adversaire dans le couloir MIROIR (1 − def_solidity) ].
--   C'est la version personnalisée de la confrontation (famille 7) pour le tireur.
--
-- Candidats = XI de départ (compo connue ~1 h avant). Pas d'imputation de la
-- solidité adverse manquante (la famille 11 comblera) → contexte NULL/partiel
-- quand le profil adverse manque.
--
-- Refonte nommage : backbone et team_corridor_profile lus sous leurs nouveaux noms
-- (CTE backbone_in, corridor_in), sorties renommées selon
-- docs/proposition_nommage_definitif.csv dans la CTE finale renamed.
-- ══════════════════════════════════════════════════════════════════════════════

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- int_whoscored_lineup lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_lineup AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        int_formation_seq                                            AS "formation_seq",
        CAST(str_formation_id AS INTEGER)                            AS "formation_id",
        int_period                                                   AS "period",
        int_start_minute                                             AS "start_minute",
        int_end_minute                                               AS "end_minute",
        CAST(str_player_id AS BIGINT)                                AS "player_id",
        int_slot                                                     AS "slot",
        dec_grid_vertical                                            AS "grid_vertical",
        dec_grid_horizontal                                          AS "grid_horizontal",
        bool_is_captain                                              AS "is_captain"
    FROM {{ ref('int_whoscored_lineup') }}
),

-- joueur_saison lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_joueur_saison AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dt_date                                                      AS "date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        int_n_apps_lag                                               AS "n_apps_lag",
        CAST(int_minutes_lag AS HUGEINT)                             AS "minutes_lag",
        dec_scorer_xg_per90_lag                                      AS "scorer_xg_per90_lag",
        dec_scorer_shots_per90_lag                                   AS "scorer_shots_per90_lag",
        dec_off_chances_created_per90_lag                            AS "off_chances_created_per90_lag",
        dec_off_key_passes_per90_lag                                 AS "off_key_passes_per90_lag",
        dec_off_xg_per_shot_lag                                      AS "off_xg_per_shot_lag",
        dec_def_aerial_win_rate_lag                                  AS "def_aerial_win_rate_lag",
        dec_def_actions_per90_lag                                    AS "def_actions_per90_lag",
        dec_def_errors_per90_lag                                     AS "def_errors_per90_lag",
        dec_player_card_propensity_lag                               AS "player_card_propensity_lag",
        dec_off_xgchain_per90_lag                                    AS "off_xgchain_per90_lag",
        dec_off_xgbuildup_per90_lag                                  AS "off_xgbuildup_per90_lag",
        dec_scorer_team_shot_share_lag                               AS "scorer_team_shot_share_lag",
        CAST(int_scorer_penalty_taker_lag AS HUGEINT)                AS "scorer_penalty_taker_lag",
        CAST(int_scorer_freekick_taker_lag AS HUGEINT)               AS "scorer_freekick_taker_lag",
        dec_def_threat_conceded_per90_lag                            AS "def_threat_conceded_per90_lag",
        dec_scorer_xgot_overperformance_lag                          AS "scorer_xgot_overperformance_lag",
        str_profile_confidence_flag                                  AS "profile_confidence_flag"
    FROM {{ ref('joueur_saison') }}
),

mdl_body AS (
WITH backbone_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        CAST(str_opponent_id AS BIGINT)   AS opponent_id,
        str_season                         AS season,
        str_venue                          AS venue
    FROM {{ ref('backbone') }}
),

corridor_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        str_corridor                       AS corridor,
        dec_def_solidity                   AS def_solidity
    FROM {{ ref('team_corridor_profile') }}
),

xi AS (
    SELECT DISTINCT
        l.match_id, l.team_id, b.opponent_id, l.player_id, b.season, b.venue
    FROM in_int_whoscored_lineup l
    JOIN backbone_in b
        ON b.match_id = l.match_id AND b.team_id = l.team_id
    WHERE l.start_minute = 0 AND l.match_id IS NOT NULL
),

-- Tirs du joueur par couloir (repère attaquant : c1,c2=gauche, c3=axe, c4,c5=droit ;
-- cellules attaquantes z4,z5), depuis son profil zonal N-1.
player_corridor_shots AS (
    SELECT CAST(str_player_id AS BIGINT) AS player_id, str_season AS season,
        CASE WHEN CAST(substr(str_zone_5x5, 5, 1) AS INTEGER) IN (1, 2) THEN 'gauche'
             WHEN CAST(substr(str_zone_5x5, 5, 1) AS INTEGER) = 3 THEN 'axe'
             ELSE 'droit' END AS corridor,
        SUM(dec_off_shot_volume_by_zone_lag) AS shot_vol
    FROM {{ source('machine_learning', 'zonal_profiles_imputed') }}
    WHERE CAST(substr(str_zone_5x5, 2, 1) AS INTEGER) IN (4, 5)
    GROUP BY 1, 2, 3
),

-- Feature 75 : croisement tirs du joueur × vulnérabilité adverse (couloir miroir).
context AS (
    SELECT x.match_id, x.team_id, x.player_id,
        SUM(pcs.shot_vol * (1 - tcp.def_solidity)) AS scorer_context_vs_opponent_style
    FROM xi x
    JOIN player_corridor_shots pcs
        ON pcs.player_id = x.player_id AND pcs.season = x.season
    LEFT JOIN corridor_in tcp
        ON tcp.match_id = x.match_id
        AND tcp.team_id = x.opponent_id
        AND tcp.corridor = CASE pcs.corridor
                              WHEN 'gauche' THEN 'droit'
                              WHEN 'droit'  THEN 'gauche'
                              ELSE 'axe'
                           END
    GROUP BY x.match_id, x.team_id, x.player_id
),

final AS (
SELECT
    x.match_id,
    x.team_id,
    x.opponent_id,
    x.player_id,
    CASE WHEN x.venue = 'Home' THEN 1 ELSE 0 END AS is_home,

    -- Features Buteurs (SEASON-LAG, depuis joueur_saison)
    js.scorer_xg_per90_lag,
    js.scorer_shots_per90_lag,
    js.scorer_team_shot_share_lag,
    js.scorer_penalty_taker_lag,
    js.scorer_freekick_taker_lag,
    js.off_xg_per_shot_lag,

    -- Feature propre 75
    c.scorer_context_vs_opponent_style
FROM xi x
LEFT JOIN in_joueur_saison js
    ON js.match_id = x.match_id AND js.team_id = x.team_id AND js.player_id = x.player_id
LEFT JOIN context c
    ON c.match_id = x.match_id AND c.team_id = x.team_id AND c.player_id = x.player_id
),

-- Renommage final (docs/proposition_nommage_definitif.csv)
renamed AS (
    SELECT
        match_id                                             AS str_match_id,
        CAST(team_id AS VARCHAR)                             AS str_team_id,
        CAST(opponent_id AS VARCHAR)                         AS str_opponent_id,
        CAST(player_id AS VARCHAR)                           AS str_player_id,
        is_home                                              AS int_is_home,
        scorer_xg_per90_lag                                  AS dec_scorer_xg_per90_lag,
        scorer_shots_per90_lag                               AS dec_scorer_shots_per90_lag,
        scorer_team_shot_share_lag                           AS dec_scorer_team_shot_share_lag,
        scorer_penalty_taker_lag                             AS int_scorer_penalty_taker_lag,
        scorer_freekick_taker_lag                            AS int_scorer_freekick_taker_lag,
        off_xg_per_shot_lag                                  AS dec_off_xg_per_shot_lag,
        scorer_context_vs_opponent_style                     AS dec_scorer_context_vs_opponent_style
    FROM final
)

SELECT * FROM renamed
),

mdl_out AS (
    SELECT
        "str_match_id"                                               AS str_match_id,
        "str_team_id"                                                AS str_team_id,
        "str_opponent_id"                                            AS str_opponent_id,
        "str_player_id"                                              AS str_player_id,
        "int_is_home"                                                AS int_is_home,
        "dec_scorer_xg_per90_lag"                                    AS dec_scorer_xg_per90_lag,
        "dec_scorer_shots_per90_lag"                                 AS dec_scorer_shots_per90_lag,
        "dec_scorer_team_shot_share_lag"                             AS dec_scorer_team_shot_share_lag,
        CAST(int_scorer_penalty_taker_lag AS BIGINT)                 AS int_scorer_penalty_taker_lag,
        CAST(int_scorer_freekick_taker_lag AS BIGINT)                AS int_scorer_freekick_taker_lag,
        "dec_off_xg_per_shot_lag"                                    AS dec_off_xg_per_shot_lag,
        "dec_scorer_context_vs_opponent_style"                       AS dec_scorer_context_vs_opponent_style
    FROM mdl_body
)

SELECT * FROM mdl_out
