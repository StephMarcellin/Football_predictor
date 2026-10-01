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

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

mdl_body AS (
SELECT DISTINCT
    team,
    season
FROM {{ source('silver', 'fbref_keeper') }}
WHERE ga_keeper > sota
ORDER BY season DESC, team
),

mdl_out AS (
    SELECT
        "team"                                                       AS str_team,
        "season"                                                     AS str_season
    FROM mdl_body
)

SELECT * FROM mdl_out
