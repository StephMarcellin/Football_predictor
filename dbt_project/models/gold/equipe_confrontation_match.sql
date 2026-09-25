{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
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

-- Refonte nommage : equipe_match est lu sous ses nouveaux noms et remappé vers les
-- anciens (CTE equipe_match_in) — les sorties de ce modèle ne changent pas (types
-- compris : team_id / opponent_id restent INTEGER).

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
WITH equipe_match_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        CAST(str_opponent_id AS BIGINT)   AS opponent_id,
        {% for w in [3, 5, 10] %}
        dec_ppda_rolling_{{ w }}           AS ppda_rolling_{{ w }},
        dec_ppda_allowed_rolling_{{ w }}   AS ppda_allowed_rolling_{{ w }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ ref('equipe_match') }}
),

team_buildup AS (
    SELECT l.match_id, l.team_id,
        SUM(COALESCE(js.off_xgbuildup_per90_lag, 0)) AS team_xgbuildup_lag
    FROM in_int_whoscored_lineup l
    JOIN in_joueur_saison js
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

FROM equipe_match_in a
LEFT JOIN equipe_match_in b
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
    SELECT str_match_id FROM {{ ref('equipe_match') }}
    EXCEPT SELECT match_id FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            CAST(str_team_id AS BIGINT)                                  AS "team_id",
            CAST(str_opponent_id AS BIGINT)                              AS "opponent_id",
            dec_opp_press_ppda_rolling_3                                 AS "opp_press_ppda_rolling_3",
            dec_self_buildup_resistance_rolling_3                        AS "self_buildup_resistance_rolling_3",
            dec_matchup_high_press_vs_buildup_rolling_3                  AS "matchup_high_press_vs_buildup_rolling_3",
            dec_opp_press_ppda_rolling_5                                 AS "opp_press_ppda_rolling_5",
            dec_self_buildup_resistance_rolling_5                        AS "self_buildup_resistance_rolling_5",
            dec_matchup_high_press_vs_buildup_rolling_5                  AS "matchup_high_press_vs_buildup_rolling_5",
            dec_opp_press_ppda_rolling_10                                AS "opp_press_ppda_rolling_10",
            dec_self_buildup_resistance_rolling_10                       AS "self_buildup_resistance_rolling_10",
            dec_matchup_high_press_vs_buildup_rolling_10                 AS "matchup_high_press_vs_buildup_rolling_10",
            dec_self_team_xgbuildup_lag                                  AS "self_team_xgbuildup_lag",
            dec_opp_team_xgbuildup_lag                                   AS "opp_team_xgbuildup_lag"
        FROM {{ this }}
    )
)
{% endif %}
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        CAST(team_id AS VARCHAR)                                     AS str_team_id,
        CAST(opponent_id AS VARCHAR)                                 AS str_opponent_id,
        "opp_press_ppda_rolling_3"                                   AS dec_opp_press_ppda_rolling_3,
        "self_buildup_resistance_rolling_3"                          AS dec_self_buildup_resistance_rolling_3,
        "matchup_high_press_vs_buildup_rolling_3"                    AS dec_matchup_high_press_vs_buildup_rolling_3,
        "opp_press_ppda_rolling_5"                                   AS dec_opp_press_ppda_rolling_5,
        "self_buildup_resistance_rolling_5"                          AS dec_self_buildup_resistance_rolling_5,
        "matchup_high_press_vs_buildup_rolling_5"                    AS dec_matchup_high_press_vs_buildup_rolling_5,
        "opp_press_ppda_rolling_10"                                  AS dec_opp_press_ppda_rolling_10,
        "self_buildup_resistance_rolling_10"                         AS dec_self_buildup_resistance_rolling_10,
        "matchup_high_press_vs_buildup_rolling_10"                   AS dec_matchup_high_press_vs_buildup_rolling_10,
        "self_team_xgbuildup_lag"                                    AS dec_self_team_xgbuildup_lag,
        "opp_team_xgbuildup_lag"                                     AS dec_opp_team_xgbuildup_lag
    FROM mdl_body
)

SELECT * FROM mdl_out
