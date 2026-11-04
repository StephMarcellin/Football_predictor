{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema="intermediate",
        alias='intermediate_team_match_backbone'
    )
}}

-- Grain : 1 ligne par (match, équipe). Base = intermediate_fbref_schedule (toutes compétitions,
-- matchs à venir inclus), enrichie par les autres int_* sur (str_match_id, str_team_id).
-- Ne contient QUE des faits au grain match. Aucune cote ici : elles vivent dans
-- gold.gold_odds_pinnacle (grain match) et ne sont jamais des features. Les agrégats de saison (intermediate_whoscored_team_season,
-- grain équipe × saison) n'ont rien à faire ici : ils sont joints en gold, décalés en N-1
-- (cf. gold.gold_team_match), sinon fuite de données (agrégat de la saison en cours).
-- Noms de sortie conformes à docs/proposition_nommage_definitif.csv (préfixes de type).
-- ⚠ Changement de schéma de sortie : premier run en --full-refresh obligatoire.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
WITH
fbref_base AS (
    SELECT
        str_match_id,
        str_team_id, str_opponent_id,

        dt_date, str_formation,
        str_venue, str_season, str_league_source, str_comp_category,
        str_result_1n2,
        int_gf,
        int_ga,
        int_poss
    FROM {{ ref('intermediate_fbref_schedule') }}
),

fbref_keeper_cte AS (
    SELECT
        str_match_id,
        str_team_id,

        int_sota                                      AS int_shots_on_target_faced,
        int_saves,
        dec_save_pct,
        int_cs                      AS int_clean_sheet
    FROM {{ ref('intermediate_fbref_keeper') }}
),

fbref_shooting_cte AS (
    SELECT
        str_match_id,
        str_team_id,

        int_standard_sh                               AS int_shots_total,
        int_standard_sot                              AS int_shots_on_target,
        dec_standard_g_sh                             AS dec_goals_per_shot
    FROM {{ ref('intermediate_fbref_shooting') }}
),

fbref_misc_cte AS (
    SELECT
        str_match_id,
        str_team_id,

        int_crdy                                      AS int_yellow_cards,
        int_crdr                                      AS int_red_cards,
        int_crdy2                                     AS int_second_yellow_cards,
        int_fls                                       AS int_fouls_committed,
        int_int                                       AS int_interceptions,
        int_tklw                                      AS int_tackles_won
    FROM {{ ref('intermediate_fbref_misc') }}
),

-- Understat : 1 ligne par match (colonnes home_/away_), rattachée au match_id unifié
-- directement dans intermediate_understat_match_stats (plus besoin de repasser par le calendrier).
understat_base AS (
    SELECT
        str_match_id,
        dec_home_np_xg, dec_away_np_xg,
        dec_home_ppda,  dec_away_ppda,
        dec_home_np_xg_diff, dec_away_np_xg_diff
    FROM {{ ref('intermediate_understat_match_stats') }}
    WHERE str_match_id IS NOT NULL
),

fbref_merged AS (
    SELECT
        b.*,
        k.int_shots_on_target_faced, k.int_saves, k.dec_save_pct, k.int_clean_sheet,
        s.int_shots_total, s.int_shots_on_target, s.dec_goals_per_shot,
        m.int_yellow_cards, m.int_red_cards, m.int_second_yellow_cards,
        m.int_fouls_committed, m.int_interceptions, m.int_tackles_won
    FROM fbref_base b
    LEFT JOIN fbref_keeper_cte   k
        ON b.str_match_id = k.str_match_id AND b.str_team_id = k.str_team_id
    LEFT JOIN fbref_shooting_cte s
        ON b.str_match_id = s.str_match_id AND b.str_team_id = s.str_team_id
    LEFT JOIN fbref_misc_cte     m
        ON b.str_match_id = m.str_match_id AND b.str_team_id = m.str_team_id
),

-- Équipe « domicile » du calendrier (listée en premier) : sert à orienter les matchs sur terrain neutre
registry_home AS (
    SELECT match_id AS str_match_id, CAST(home_team_id AS VARCHAR) AS str_listed_home_team_id
    FROM {{ source('intermediate', 'match_registry') }}
),

-- Orientation : is_home_side = la ligne est du côté « domicile » du match.
-- Home → vrai ; Away → faux ; Neutral → vrai pour l'équipe listée en premier.
-- Label 1N2 : le label est fixé par rapport au côté domicile. Sur terrain neutre, la source
-- l'encode du point de vue de la ligne (H = la ligne gagne) : on l'inverse (H<->A) sur la ligne
-- de l'équipe NON listée en premier, pour que les deux lignes disent la même chose.
fbref_sided AS (
    SELECT
        f.* EXCLUDE (str_result_1n2),
        CASE WHEN f.str_venue = 'Home' THEN TRUE
             WHEN f.str_venue = 'Neutral' AND f.str_team_id = r.str_listed_home_team_id THEN TRUE
             ELSE FALSE END AS is_home_side,
        CASE WHEN f.str_venue = 'Neutral'
                  AND r.str_listed_home_team_id IS NOT NULL
                  AND f.str_team_id <> r.str_listed_home_team_id
             THEN CASE f.str_result_1n2 WHEN 'H' THEN 'A' WHEN 'A' THEN 'H' ELSE f.str_result_1n2 END
             ELSE f.str_result_1n2 END AS str_result_1n2
    FROM fbref_merged f
    LEFT JOIN registry_home r ON r.str_match_id = f.str_match_id
),

-- Pivot Understat home/away → point de vue de l'équipe (venue)
fbref_understat AS (
    SELECT
        f.*,
        CASE WHEN f.is_home_side THEN u.dec_home_np_xg      ELSE u.dec_away_np_xg      END AS dec_np_xg,
        CASE WHEN f.is_home_side THEN u.dec_away_np_xg      ELSE u.dec_home_np_xg      END AS dec_np_xg_conceded,
        CASE WHEN f.is_home_side THEN u.dec_home_ppda       ELSE u.dec_away_ppda       END AS dec_ppda,
        CASE WHEN f.is_home_side THEN u.dec_away_ppda       ELSE u.dec_home_ppda       END AS dec_ppda_allowed,
        CASE WHEN f.is_home_side THEN u.dec_home_np_xg_diff ELSE u.dec_away_np_xg_diff END AS dec_np_xg_diff_match
    FROM fbref_sided f
    LEFT JOIN understat_base u
        ON f.str_match_id = u.str_match_id
),

final AS (
    SELECT
        -- Identifiants
        f.str_match_id,
        f.str_team_id, f.str_opponent_id,

        f.dt_date,
        f.str_venue, f.str_season, f.str_league_source, f.str_comp_category,
        f.str_result_1n2,
        f.str_formation, f.int_gf, f.int_ga, f.int_poss,

        -- FBref
        f.int_shots_on_target_faced, f.int_saves, f.dec_save_pct, f.int_clean_sheet,
        f.int_shots_total, f.int_shots_on_target, f.dec_goals_per_shot,
        f.int_yellow_cards, f.int_second_yellow_cards, f.int_red_cards,
        f.int_fouls_committed, f.int_interceptions, f.int_tackles_won,

        -- Understat
        f.dec_np_xg, f.dec_np_xg_conceded, f.dec_ppda, f.dec_ppda_allowed, f.dec_np_xg_diff_match

    FROM fbref_understat f
)

SELECT * FROM final
-- Filtre COVID Ligue 1 / Ligue 2 — matchs suspendus
WHERE NOT (
    str_season = '2019-2020'
    AND str_league_source IN ('Ligue 1', 'Ligue 2')
    AND dt_date >= '2020-03-08'
)

{% if is_incremental() %}
AND (str_match_id || '_' || str_team_id) NOT IN (
    SELECT (str_match_id || '_' || str_team_id)
    FROM {{ this }}
)
{% endif %}
)

SELECT * FROM mdl_body
