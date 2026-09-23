{{ config(materialized='view', schema='audits') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- scraping_targets_int_fbref_shooting
--
-- Liste des (team, season) à rescraper : lignes de silver.fbref_shooting où
-- une des deux violations physiques A-011 est présente :
--   1) standard_gls > standard_sot — buts sur tir supérieurs aux tirs cadrés
--      (1 475 lignes empiriques — bug scraping FBref confirmé côté silver).
--   2) standard_gls > gf — buts sur tir supérieurs aux buts totaux
--      (5 lignes empiriques — anomalie mineure du même bug).
--
-- Source : silver.fbref_shooting (couche brute alimentée par le scraper —
--   JAMAIS partir de intermediate, cf. anti-pattern n°5 de la charte).
-- Materialization : view — se met à jour à chaque `dbt run`, permet de suivre
-- la dette de rescraping descendre au fil du temps.
--
-- Consommation côté scraper :
--   SELECT team, season FROM audits.scraping_targets_int_fbref_shooting;
-- ══════════════════════════════════════════════════════════════════════════════

SELECT DISTINCT
    team,
    season
FROM {{ source('silver', 'fbref_shooting') }}
WHERE standard_gls > standard_sot
   OR standard_gls > gf
ORDER BY season DESC, team
