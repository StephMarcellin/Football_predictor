{{
    config(
        materialized='table',
        schema='intermediate',
        alias='intermediate_matchs_sans_score'
    )
}}

-- ════════════════════════════════════════════════════════════════════════════
-- intermediate.intermediate_matchs_sans_score — grain : 1 ligne par MATCH
--
-- Liste de contrôle des matchs du backbone pour lesquels AUCUN score n'est connu :
--   • 'passe_sans_score' : date passée et pas de score → résultat non ingéré, match
--                          annulé ou non joué (ex. Covid 2019-2020), ou donnée absente de la source.
--   • 'a_venir'          : date aujourd'hui ou future → normal, pas encore joué.
--
-- Sert à décider quoi ré-ingérer. Ce n'est PAS une feature et rien ne le consomme.
-- ⚠ Dépend de la date du jour (CURRENT_DATE) : le statut est celui du dernier `dbt run`.
--
-- Un match est « sans score » si int_gf est NULL sur toutes ses lignes dans le backbone
-- (le backbone a 2 lignes par match, une par équipe).
-- Home / away = équipe listée à domicile / à l'extérieur dans match_registry.
-- str_ws_match_id = identifiant WhoScored du match (via intermediate_whoscored_match_bridge).
-- NULL = match absent de l'index WhoScored (pas encore listé, ou compétition non scrapée).
-- ════════════════════════════════════════════════════════════════════════════

WITH matchs_sans_score AS (
    SELECT
        str_match_id
    FROM {{ ref('intermediate_team_match_backbone') }}
    GROUP BY str_match_id
    HAVING MAX(int_gf) IS NULL
),

noms_equipes AS (
    -- Un nom canonique par équipe (club_name), le mapping étant au grain alias.
    SELECT team_id, MIN(club_name) AS club_name
    FROM {{ source('referentiel', 'team_mapping') }}
    GROUP BY team_id
),

registre AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

pont_whoscored AS (
    SELECT str_match_id, str_ws_match_id
    FROM {{ ref('intermediate_whoscored_match_bridge') }}
)

SELECT
    r.match_id                                  AS str_match_id,
    w.str_ws_match_id                           AS str_ws_match_id,
    r.home_team_id                              AS str_home_team_id,
    r.away_team_id                              AS str_away_team_id,
    nh.club_name                                AS str_home_team_name,
    na.club_name                                AS str_away_team_name,
    r.match_date                                AS dt_match_date,
    r.season                                    AS str_season,
    r.league_source                             AS str_league_source,
    CASE WHEN r.match_date < CURRENT_DATE
         THEN 'passe_sans_score' ELSE 'a_venir' END AS str_statut
FROM matchs_sans_score m
JOIN registre r             ON r.match_id = m.str_match_id
LEFT JOIN noms_equipes nh   ON nh.team_id = r.home_team_id
LEFT JOIN noms_equipes na   ON na.team_id = r.away_team_id
LEFT JOIN pont_whoscored w  ON w.str_match_id = r.match_id
