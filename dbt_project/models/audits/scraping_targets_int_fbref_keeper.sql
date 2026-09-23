{{ config(materialized='view', schema='audits') }}

-- ══════════════════════════════════════════════════════════════════════════════
-- scraping_targets_int_fbref_keeper
--
-- Liste des (team, season) à rescraper : lignes de silver.fbref_keeper où
-- ga_keeper > sota (violation physique — buts encaissés supérieurs aux tirs
-- cadrés subis).
--
-- Source : silver.fbref_keeper (couche brute alimentée par le scraper).
-- Materialization : view — se met à jour à chaque `dbt run`, permet de suivre
-- la dette de rescraping descendre au fil du temps.
--
-- Consommation côté scraper :
--   SELECT team, season FROM audits.scraping_targets_int_fbref_keeper;
-- ══════════════════════════════════════════════════════════════════════════════

SELECT DISTINCT
    team,
    season
FROM {{ source('silver', 'fbref_keeper') }}
WHERE ga_keeper > sota
ORDER BY season DESC, team
