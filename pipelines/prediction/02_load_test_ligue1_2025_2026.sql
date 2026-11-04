-- ══════════════════════════════════════════════════════════════════════════════
-- 02_load_test_ligue1_2025_2026.sql — JEU DE TEST : toute la Ligue 1 2025-2026.
-- Prérequis : la table existe (créée par `dbt run --select match_to_predict`).
-- ATTENTION : vide d'abord match_to_predict (DELETE) puis la remplit. À ne lancer que pour
-- repartir du jeu de test, jamais sur une table contenant des matchs saisis à la main.
--
-- Source : intermediate_fbref_schedule (une ligne par équipe et par match → 2 × 306 = 612).
-- Noms : un seul nom canonique par team_id (MIN(club_name) de referentiel.team_mapping).
-- ══════════════════════════════════════════════════════════════════════════════
DELETE FROM prediction.match_to_predict;

INSERT INTO prediction.match_to_predict
WITH names AS (
    SELECT team_id, MIN(club_name) AS club_name
    FROM referentiel.team_mapping
    GROUP BY team_id
)
SELECT
    s.dt_date,
    nt.club_name  AS str_team_name,
    no_.club_name AS str_opponent_name,
    s.str_venue
FROM intermediate.intermediate_fbref_schedule s
JOIN names nt  ON nt.team_id  = CAST(s.str_team_id     AS BIGINT)
JOIN names no_ ON no_.team_id = CAST(s.str_opponent_id AS BIGINT)
WHERE s.str_season = '2025-2026'
  AND s.str_league_source = 'Ligue 1'
ORDER BY s.dt_date, s.str_match_id, s.str_venue DESC;
