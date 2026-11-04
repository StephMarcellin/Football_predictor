{{ config(materialized='view', schema='prediction', alias='match_resolved') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- prediction.match_resolved — résolution de prediction.match_to_predict : noms d'équipes → team_id,
-- puis (date, équipes) → str_match_id du backbone. UNE ligne par MATCH demandé (les 2 lignes
-- « une par équipe » de match_to_predict sont regroupées).
--
-- Pourquoi on résout contre le BACKBONE et non contre match_registry : le modèle lit des lignes du
-- mart, qui dérivent du backbone. Un match_id absent du backbone n'aurait aucune ligne à prédire.
-- (Le registre contient des doublons d'un même match décalés d'un jour : 77 paires, toutes ligues et
-- saisons confondues ; le backbone n'a, pour la Ligue 1 2025-26, que 306 match_id pour 306 matchs.)
--
-- Statuts (str_status), du plus bloquant au moins bloquant :
--   equipe_inconnue         un nom ne correspond à aucun alias / club_name de team_mapping
--   incoherent              les lignes d'un même match se contredisent (deux domiciles, Home+Neutral,
--                           ou l'équipe contre elle-même)
--   match_inconnu           aucun match du backbone pour ces deux équipes à ± 1 jour
--   ambigu                  plusieurs match_id possibles à égale distance de la date
--   domicile_contradictoire le côté déclaré (Home/Away) contredit l'équipe domicile du backbone
--   resolu                  prêt à être prédit
-- ══════════════════════════════════════════════════════════════════════════════
WITH

-- 1) Noms → team_id : alias ET club_name (comparaison sans casse ni espaces de bord).
names AS (
    SELECT lower(trim(alias)) AS nm, team_id FROM {{ source('referentiel', 'team_mapping') }}
    UNION
    SELECT lower(trim(club_name)) AS nm, team_id FROM {{ source('referentiel', 'team_mapping') }}
),

-- Un nom canonique d'affichage par team_id.
display_names AS (
    SELECT team_id, MIN(club_name) AS club_name FROM {{ source('referentiel', 'team_mapping') }} GROUP BY team_id
),

-- 2) Lignes saisies + team_id des deux noms (LEFT JOIN : un nom inconnu reste visible, id NULL).
input AS (
    SELECT
        i.dt_date,
        i.str_team_name,
        i.str_opponent_name,
        i.str_venue,
        lower(trim(i.str_team_name))     AS ln_team,
        lower(trim(i.str_opponent_name)) AS ln_opp,
        nt.team_id                       AS team_id,
        nopp.team_id                     AS opp_id,
        COALESCE(CAST(nt.team_id   AS VARCHAR), 'nom:' || lower(trim(i.str_team_name)))     AS k_team,
        COALESCE(CAST(nopp.team_id AS VARCHAR), 'nom:' || lower(trim(i.str_opponent_name))) AS k_opp
    FROM {{ ref('match_to_predict') }} i
    LEFT JOIN names nt   ON nt.nm   = lower(trim(i.str_team_name))
    LEFT JOIN names nopp ON nopp.nm = lower(trim(i.str_opponent_name))
),

-- 3) Une ligne par match demandé : clé = (date, paire d'équipes triée). On regroupe par team_id quand
--    il est connu (« Paris SG » et « Paris Saint-Germain » = la même équipe), sinon par nom normalisé.
--    key_a / key_b = les deux clés triées ; id_a / id_b leurs team_id (NULL si nom inconnu) ;
--    declared_home_k = clé de l'équipe domicile déclarée (via Home/Away), NULL si Neutral seulement.
asked AS (
    SELECT
        dt_date,
        least(k_team, k_opp)    AS key_a,
        greatest(k_team, k_opp) AS key_b,
        max(CASE WHEN k_team <= k_opp THEN str_team_name     ELSE str_opponent_name END) AS name_a,
        max(CASE WHEN k_team <= k_opp THEN str_opponent_name ELSE str_team_name     END) AS name_b,
        max(CASE WHEN k_team <= k_opp THEN team_id ELSE opp_id  END) AS id_a,
        max(CASE WHEN k_team <= k_opp THEN opp_id  ELSE team_id END) AS id_b,
        count(*)                                                   AS n_rows,
        count(DISTINCT CASE str_venue WHEN 'Home' THEN k_team WHEN 'Away' THEN k_opp END) AS n_declared_homes,
        count(*) FILTER (WHERE str_venue = 'Neutral')              AS n_neutral_rows,
        count(*) FILTER (WHERE str_venue IN ('Home', 'Away'))      AS n_sided_rows,
        max(CASE str_venue WHEN 'Home' THEN k_team WHEN 'Away' THEN k_opp END) AS declared_home_k
    FROM input
    GROUP BY dt_date, least(k_team, k_opp), greatest(k_team, k_opp)
),
asked_k AS (
    SELECT row_number() OVER (ORDER BY dt_date, key_a, key_b) AS gk, * FROM asked
),

-- 4) Matchs du backbone : (match_id, date, paire d'ids) et équipe domicile officielle.
--    Home → l'équipe de la ligne Home ; Neutral → équipe listée en premier du registre.
bb_rows AS (
    SELECT DISTINCT
        str_match_id,
        dt_date,
        CAST(str_team_id     AS BIGINT) AS team_id,
        CAST(str_opponent_id AS BIGINT) AS opp_id,
        str_venue
    FROM {{ ref('intermediate_team_match_backbone') }}
),
bb_matches AS (
    SELECT DISTINCT
        b.str_match_id,
        b.dt_date,
        least(b.team_id, b.opp_id)    AS id_a,
        greatest(b.team_id, b.opp_id) AS id_b,
        COALESCE(h.home_id, r.home_team_id) AS official_home_id
    FROM bb_rows b
    LEFT JOIN (SELECT str_match_id, max(team_id) AS home_id FROM bb_rows WHERE str_venue = 'Home' GROUP BY 1) h
           ON h.str_match_id = b.str_match_id
    LEFT JOIN {{ source('intermediate', 'match_registry') }} r ON r.match_id = b.str_match_id
),

-- 5) Candidats : même paire d'équipes, date à ± 1 jour ; on garde l'écart minimal.
cand AS (
    SELECT a.gk, m.str_match_id, m.official_home_id,
           abs(date_diff('day', m.dt_date, a.dt_date)) AS day_gap
    FROM asked_k a
    JOIN bb_matches m
      ON m.id_a = least(a.id_a, a.id_b) AND m.id_b = greatest(a.id_a, a.id_b)
     AND abs(date_diff('day', m.dt_date, a.dt_date)) <= 1
),
best AS (
    SELECT c.gk,
           count(*)                  AS n_cand,
           max(c.str_match_id)       AS str_match_id,
           max(c.official_home_id)   AS official_home_id
    FROM cand c
    JOIN (SELECT gk, min(day_gap) AS mg FROM cand GROUP BY gk) g
      ON g.gk = c.gk AND g.mg = c.day_gap
    GROUP BY c.gk
),

-- 6) Statut.
resolved AS (
    SELECT
        a.*,
        b.n_cand, b.str_match_id, b.official_home_id,
        CASE WHEN a.declared_home_k IS NULL THEN NULL
             WHEN a.declared_home_k = a.key_a THEN a.id_a
             ELSE a.id_b END AS declared_home_id,
        CASE
            WHEN a.id_a IS NULL OR a.id_b IS NULL                               THEN 'equipe_inconnue'
            WHEN a.id_a = a.id_b OR a.n_declared_homes > 1
                 OR (a.n_neutral_rows > 0 AND a.n_sided_rows > 0)               THEN 'incoherent'
            WHEN b.gk IS NULL                                                    THEN 'match_inconnu'
            WHEN b.n_cand > 1                                                    THEN 'ambigu'
            WHEN a.declared_home_k IS NOT NULL
                 AND b.official_home_id IS NOT NULL
                 AND (CASE WHEN a.declared_home_k = a.key_a THEN a.id_a ELSE a.id_b END) <> b.official_home_id
                                                                                 THEN 'domicile_contradictoire'
            ELSE 'resolu'
        END AS str_status
    FROM asked_k a
    LEFT JOIN best b ON b.gk = a.gk
)

SELECT
    r.dt_date,
    -- Équipes affichées : domicile officiel si le match est résolu, sinon l'ordre déclaré, sinon alphabétique.
    COALESCE(dh.club_name, CASE WHEN r.declared_home_k = r.key_b THEN r.name_b ELSE r.name_a END) AS str_home_team_name,
    COALESCE(da.club_name, CASE WHEN r.declared_home_k = r.key_b THEN r.name_a ELSE r.name_b END) AS str_away_team_name,
    CASE WHEN r.str_status = 'resolu' THEN r.str_match_id END                                      AS str_match_id,
    CASE WHEN r.str_status = 'resolu' THEN CAST(r.official_home_id AS VARCHAR) END                 AS str_home_team_id,
    CASE WHEN r.str_status = 'resolu'
         THEN CAST(CASE WHEN r.official_home_id = r.id_a THEN r.id_b ELSE r.id_a END AS VARCHAR) END AS str_away_team_id,
    r.str_status,
    CASE r.str_status
        WHEN 'equipe_inconnue' THEN 'Nom non reconnu dans team_mapping : ' ||
             CASE WHEN r.id_a IS NULL THEN r.name_a ELSE '' END ||
             CASE WHEN r.id_a IS NULL AND r.id_b IS NULL THEN ' / ' ELSE '' END ||
             CASE WHEN r.id_b IS NULL THEN r.name_b ELSE '' END
        WHEN 'incoherent' THEN 'Les lignes de ce match se contredisent (domiciles, Neutral, ou même équipe)'
        WHEN 'match_inconnu' THEN 'Aucun match du backbone pour ces équipes à ± 1 jour de cette date'
        WHEN 'ambigu' THEN CAST(r.n_cand AS VARCHAR) || ' matchs possibles à égale distance de la date'
        WHEN 'domicile_contradictoire' THEN 'Le côté déclaré contredit l''équipe domicile du backbone'
        ELSE NULL
    END AS str_message,
    r.n_rows AS int_n_input_rows
FROM resolved r
LEFT JOIN display_names dh ON dh.team_id = CASE WHEN r.str_status IN ('resolu', 'domicile_contradictoire') THEN r.official_home_id END
LEFT JOIN display_names da ON da.team_id = CASE WHEN r.str_status IN ('resolu', 'domicile_contradictoire')
                                                THEN CASE WHEN r.official_home_id = r.id_a THEN r.id_b ELSE r.id_a END END
ORDER BY r.dt_date, str_home_team_name
