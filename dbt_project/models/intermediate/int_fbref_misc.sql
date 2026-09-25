{{
    config(
        materialized='table',
        schema='intermediate',
        alias='int_fbref_misc'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('silver', 'fbref_misc') }}
),

typed AS (
    SELECT
        TRY_CAST(date AS DATE)                                AS dt_date,
        TRY_CAST(time AS VARCHAR)                             AS str_time,
        TRY_CAST(league_source AS VARCHAR)                    AS str_league_source,
        TRY_CAST(day AS VARCHAR)                              AS str_day,
        TRY_CAST(venue AS VARCHAR)                            AS str_venue,
        TRY_CAST(result AS VARCHAR)                           AS str_result,
        TRY_CAST(NULLIF(TRIM(gf), '')         AS INTEGER)     AS int_gf,
        TRY_CAST(NULLIF(TRIM(ga), '')         AS INTEGER)     AS int_ga,
        TRY_CAST(opponent AS VARCHAR)                         AS str_opponent,
        TRY_CAST(NULLIF(TRIM(crdy), '')       AS INTEGER)     AS int_crdy,
        TRY_CAST(NULLIF(TRIM(crdr), '')       AS INTEGER)     AS int_crdr,
        TRY_CAST(NULLIF(TRIM(crdy2), '')      AS INTEGER)     AS int_crdy2,
        TRY_CAST(NULLIF(TRIM(fls), '')        AS INTEGER)     AS int_fls,
        TRY_CAST(NULLIF(TRIM(fld), '')        AS INTEGER)     AS int_fld,
        TRY_CAST(NULLIF(TRIM("off"), '')      AS INTEGER)     AS int_off,
        TRY_CAST(NULLIF(TRIM(crosses), '')    AS INTEGER)     AS int_crosses,
        TRY_CAST(NULLIF(TRIM("int"), '')      AS INTEGER)     AS int_int,
        TRY_CAST(NULLIF(TRIM(tklw), '')       AS INTEGER)     AS int_tklw,
        TRY_CAST(NULLIF(TRIM(pkwon), '')      AS INTEGER)     AS int_pkwon,
        TRY_CAST(NULLIF(TRIM(pkcon), '')      AS INTEGER)     AS int_pkcon,
        TRY_CAST(NULLIF(TRIM(og), '')         AS INTEGER)     AS int_og,
        TRY_CAST(team AS VARCHAR)                             AS str_team,
        TRY_CAST(season AS VARCHAR)                           AS str_season,
        TRY_CAST(source AS VARCHAR)                           AS str_source,
        TRY_CAST(NULLIF(TRIM(scraped_at), '') AS TIMESTAMP)   AS dt_scraped_at,
        TRY_CAST(comp_category AS VARCHAR)                    AS str_comp_category,
        TRY_CAST(raw_team AS VARCHAR)                         AS str_raw_team,
        TRY_CAST(raw_opponent AS VARCHAR)                     AS str_raw_opponent,
        TRY_CAST(result_1n2 AS VARCHAR)                       AS str_result_1n2
    FROM source
),

team_mapping AS (
    SELECT DISTINCT club_name, team_id
    FROM {{ source('referentiel', 'team_mapping') }}
),

registry AS (
    SELECT * FROM {{ source('intermediate', 'match_registry') }}
),

-- 1. Assemblage et résolution des identifiants
--    Jointure contrainte sur l'adversaire : 0 fan-out (86 396 lignes in = out, vs 86 645 avant).
enriched AS (
    SELECT
        r.match_id                           AS str_match_id,
        TRY_CAST(tm_team.team_id AS VARCHAR) AS str_team_id,
        TRY_CAST(tm_opp.team_id  AS VARCHAR) AS str_opponent_id,
        s.* EXCLUDE (str_team, str_opponent, str_raw_team, str_raw_opponent)
    FROM typed s
    LEFT JOIN team_mapping tm_team ON s.str_team     = tm_team.club_name
    LEFT JOIN team_mapping tm_opp  ON s.str_opponent = tm_opp.club_name
    LEFT JOIN registry r
        ON  s.dt_date           = r.match_date
        AND s.str_league_source = r.league_source
        AND s.str_season        = r.season
        AND (
            (s.str_venue = 'Home'    AND tm_team.team_id = r.home_team_id AND tm_opp.team_id = r.away_team_id)
            OR
            (s.str_venue = 'Away'    AND tm_team.team_id = r.away_team_id AND tm_opp.team_id = r.home_team_id)
            OR
            (s.str_venue = 'Neutral' AND (
                (tm_team.team_id = r.home_team_id AND tm_opp.team_id = r.away_team_id)
                OR
                (tm_team.team_id = r.away_team_id AND tm_opp.team_id = r.home_team_id)
            ))
        )
),

-- 2. Détection des blocs de faux zéros (donnée manquante codée 0 par FBref)
--    Bloc complet : fls = fld = crosses = 0 ensemble (616 lignes, surtout D2 2017-18 et coupes) —
--    off, int et tklw y valent aussi 0 dans 100 % des cas. 0 faute est quasi impossible
--    (fls = 0 : 623 lignes vs fls = 1 : 15).
--    Sous-bloc défensif : int = tklw = 0 ensemble hors bloc complet (coupes surtout).
flagged AS (
    SELECT
        *,
        COALESCE(int_fls = 0 AND int_fld = 0 AND int_crosses = 0, FALSE)   AS is_missing_block,
        COALESCE(int_int = 0 AND int_tklw = 0, FALSE)                      AS is_missing_defense
    FROM enriched
),

-- 3. Neutralisation des faux zéros → NULL (jamais de GREATEST : GREATEST(NULL, 0) = 0)
corrected AS (
    SELECT
        * EXCLUDE (
            int_fls, int_fld, int_off, int_crosses, int_int, int_tklw,
            is_missing_block, is_missing_defense
        ),
        CASE WHEN is_missing_block THEN NULL ELSE int_fls     END AS int_fls,
        CASE WHEN is_missing_block THEN NULL ELSE int_fld     END AS int_fld,
        CASE WHEN is_missing_block THEN NULL ELSE int_off     END AS int_off,
        CASE WHEN is_missing_block THEN NULL ELSE int_crosses END AS int_crosses,
        CASE WHEN is_missing_block OR is_missing_defense THEN NULL ELSE int_int  END AS int_int,
        CASE WHEN is_missing_block OR is_missing_defense THEN NULL ELSE int_tklw END AS int_tklw
    FROM flagged
)

SELECT * FROM corrected