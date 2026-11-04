{{ config(materialized='view', schema='prediction', alias='match_evaluated') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- prediction.match_evaluated — prédictions (prediction.match_predicted) confrontées au résultat réel et à Pinnacle.
-- Une ligne par match prédit ET par (modèle, version de modèle). Aucune agrégation ici : on agrège à la lecture
-- (moyenne du log-loss, taux de réussite, ROI…), voir les requêtes de contrôle dans la passation.
--
--   Résultat réel : label du backbone sur la ligne de l'équipe à domicile (H = le domicile gagne) ; NULL si le
--                   match n'est pas encore joué ou pas ingéré → les métriques restent NULL (jamais 0).
--   Log-loss      : -ln(probabilité donnée à l'issue réelle) ; plus bas = mieux. Brier : somme des carrés des écarts
--                   sur les 3 issues ; plus bas = mieux.
--   Pinnacle      : probabilités et cotes d'OUVERTURE de gold_odds_pinnacle (sans marge) ; NULL si le match n'a pas de cotes.
--   Edge          : probabilité du modèle moins probabilité de Pinnacle, par issue.
-- ══════════════════════════════════════════════════════════════════════════════
WITH pred AS (
    SELECT *
    FROM {{ source('prediction', 'match_predicted') }}
    WHERE dec_p_home IS NOT NULL            -- uniquement les matchs réellement prédits
),

actual AS (
    SELECT str_match_id, str_team_id, str_result_1n2
    FROM {{ ref('intermediate_team_match_backbone') }}
),

odds AS (
    SELECT
        str_match_id,
        dec_prob_open_home, dec_prob_open_draw, dec_prob_open_away,
        dec_odds_open_home, dec_odds_open_draw, dec_odds_open_away
    FROM {{ ref('gold_odds_pinnacle') }}
),

joined AS (
    SELECT
        p.dt_date,
        p.str_home_team_name,
        p.str_away_team_name,
        p.str_match_id,
        p.str_model_key,
        p.str_model_version,
        p.dt_predicted_at,
        p.dec_p_home,
        p.dec_p_draw,
        p.dec_p_away,
        p.str_predicted_outcome,
        a.str_result_1n2 AS str_actual_outcome,
        o.dec_prob_open_home AS dec_pinnacle_p_home,
        o.dec_prob_open_draw AS dec_pinnacle_p_draw,
        o.dec_prob_open_away AS dec_pinnacle_p_away,
        o.dec_odds_open_home, o.dec_odds_open_draw, o.dec_odds_open_away
    FROM pred p
    LEFT JOIN actual a
           ON a.str_match_id = p.str_match_id AND a.str_team_id = p.str_home_team_id
    LEFT JOIN odds o
           ON o.str_match_id = p.str_match_id
)

SELECT
    *,
    CASE WHEN str_actual_outcome IS NULL THEN NULL
         WHEN str_predicted_outcome = str_actual_outcome THEN 1 ELSE 0 END AS int_is_correct,

    CASE str_actual_outcome
        WHEN 'H' THEN -ln(greatest(dec_p_home, 1e-15))
        WHEN 'D' THEN -ln(greatest(dec_p_draw, 1e-15))
        WHEN 'A' THEN -ln(greatest(dec_p_away, 1e-15)) END AS dec_log_loss,

    CASE WHEN str_actual_outcome IS NULL THEN NULL
         ELSE power(dec_p_home - (str_actual_outcome = 'H')::INTEGER, 2)
            + power(dec_p_draw - (str_actual_outcome = 'D')::INTEGER, 2)
            + power(dec_p_away - (str_actual_outcome = 'A')::INTEGER, 2) END AS dec_brier,

    CASE str_actual_outcome
        WHEN 'H' THEN -ln(greatest(dec_pinnacle_p_home, 1e-15))
        WHEN 'D' THEN -ln(greatest(dec_pinnacle_p_draw, 1e-15))
        WHEN 'A' THEN -ln(greatest(dec_pinnacle_p_away, 1e-15)) END AS dec_pinnacle_log_loss,

    CASE WHEN str_actual_outcome IS NULL OR dec_pinnacle_p_home IS NULL THEN NULL
         ELSE power(dec_pinnacle_p_home - (str_actual_outcome = 'H')::INTEGER, 2)
            + power(dec_pinnacle_p_draw - (str_actual_outcome = 'D')::INTEGER, 2)
            + power(dec_pinnacle_p_away - (str_actual_outcome = 'A')::INTEGER, 2) END AS dec_pinnacle_brier,

    dec_p_home - dec_pinnacle_p_home AS dec_edge_home,
    dec_p_draw - dec_pinnacle_p_draw AS dec_edge_draw,
    dec_p_away - dec_pinnacle_p_away AS dec_edge_away,

    CASE WHEN dec_pinnacle_p_home IS NULL THEN 0 ELSE 1 END AS int_has_odds
FROM joined
