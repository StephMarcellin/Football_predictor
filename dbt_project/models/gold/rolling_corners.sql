{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='rolling_corners'
    )
}}

WITH

-- ══════════════════════════════════════════════════════════════════════════════
-- CORNER_MATCH_AGG
-- Agrégation de corner_profiles au grain match + équipe (team_id = équipe qui
-- TIRE le corner). Au-delà du volume/danger/but déjà présents, on ajoute :
--   - sum_danger            : magnitude continue du danger généré (chain_danger_total),
--                             équivalent "xG par corner" utilisé par Opta/StatsPerform
--   - n_headed_shots/goals  : profil aérien offensif (tirs et buts de la tête)
--   - n_short               : tendance tactique corner court vs centré
--   - n_clearances / _bad / _headed : qualité du dégagement adverse pendant la séquence
--     (NB : ces 3 champs décrivent l'action de l'équipe DÉFENSEUSE, donc l'ADVERSAIRE
--     de team_id dans cette ligne — voir commentaire dans BASE plus bas)
-- ══════════════════════════════════════════════════════════════════════════════
corner_match_agg AS (
    SELECT
        match_id,
        team_id,
        COUNT(*)                                                                   AS n_corners,
        COUNT(*) FILTER (WHERE outcome IN ('goal','shot_saved','shot_off_target')) AS n_dangerous,
        COUNT(*) FILTER (WHERE outcome = 'goal')                                   AS n_goals,
        SUM(chain_danger_total)                                                    AS sum_danger,
        COUNT(*) FILTER (WHERE shot_body_part = 'head')                           AS n_headed_shots,
        COUNT(*) FILTER (WHERE outcome = 'goal' AND shot_body_part = 'head')      AS n_headed_goals,
        COUNT(*) FILTER (WHERE landing_zone = 'short')                            AS n_short,
        COUNT(*) FILTER (WHERE clearance_quality IS NOT NULL)                     AS n_clearances,
        COUNT(*) FILTER (WHERE clearance_quality IN ('poor', 'failed'))           AS n_clearances_bad,
        COUNT(*) FILTER (WHERE is_headed_clearance)                              AS n_clearances_headed,
        -- Dénominateur DÉDIÉ pour le taux de tête : is_headed_clearance peut être
        -- renseigné (TRUE/FALSE) même quand clearance_quality est NULL (cas où
        -- aucune possession certaine suivante n'a été trouvée, ~0.3% des dégagements
        -- selon corner_profiles.sql). Utiliser n_clearances comme dénominateur ici
        -- produisait des taux > 1 (numérateur pas un sous-ensemble du dénominateur).
        COUNT(*) FILTER (WHERE is_headed_clearance IS NOT NULL)                  AS n_clearances_bodypart_known
    FROM {{ ref('corner_profiles') }}
    GROUP BY match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- MATCH_COVERAGE
-- Un match a-t-il des données WhoScored du tout ? Sert à distinguer "0 corner
-- réel" de "pas de donnée" — corner_profiles ne couvre que 4 compétitions
-- sur 28 (Serie A, Premier League, Ligue 1, Bundesliga).
-- ══════════════════════════════════════════════════════════════════════════════
match_coverage AS (
    SELECT DISTINCT match_id
    FROM {{ ref('player_possession_chains') }}
),

-- ══════════════════════════════════════════════════════════════════════════════
-- BASE
-- Jointure sur backbone : "for" via team_id, "against" via opponent_id.
--
-- ATTENTION SÉMANTIQUE CLÉ (dégagements) :
-- corner_match_agg est groupé par team_id = équipe qui TIRE le corner. Les
-- champs de dégagement (n_clearances*) y décrivent donc l'action de l'équipe
-- QUI DÉFEND, c'est-à-dire l'ADVERSAIRE de team_id.
--   - Côté "for"  (cf, team_id = bb.team_id attaque)      -> ces champs décrivent
--     comment l'ADVERSAIRE dégage face aux corners de bb.team_id. Utile comme
--     mesure de la capacité de bb.team_id à provoquer de mauvais dégagements
--     (second ballon) -> "forced_bad_clearance_rate_for".
--   - Côté "against" (ca, team_id = bb.opponent_id attaque) -> ces champs
--     décrivent comment bb.team_id (qui défend) dégage réellement ses propres
--     corners concédés -> "clearance_fail_rate_against" / "headed_clearance_rate_against".
-- ══════════════════════════════════════════════════════════════════════════════
base AS (
    SELECT
        bb.match_id,
        bb.team_id,
        bb.date,
        bb.season,
        bb.league_source,

        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_corners, 0)             ELSE NULL END AS n_corners_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_dangerous, 0)           ELSE NULL END AS n_dangerous_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_goals, 0)               ELSE NULL END AS n_goals_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.sum_danger, 0)            ELSE NULL END AS sum_danger_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_headed_shots, 0)        ELSE NULL END AS n_headed_shots_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_headed_goals, 0)        ELSE NULL END AS n_headed_goals_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_short, 0)               ELSE NULL END AS n_short_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances, 0)          ELSE NULL END AS n_clearances_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances_bad, 0)      ELSE NULL END AS n_clearances_bad_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances_headed, 0)   ELSE NULL END AS n_clearances_headed_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances_bodypart_known, 0) ELSE NULL END AS n_clearances_bodypart_known_for,

        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_corners, 0)             ELSE NULL END AS n_corners_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_dangerous, 0)           ELSE NULL END AS n_dangerous_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_goals, 0)               ELSE NULL END AS n_goals_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.sum_danger, 0)            ELSE NULL END AS sum_danger_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_headed_shots, 0)        ELSE NULL END AS n_headed_shots_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_headed_goals, 0)        ELSE NULL END AS n_headed_goals_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_short, 0)               ELSE NULL END AS n_short_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances, 0)          ELSE NULL END AS n_clearances_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances_bad, 0)      ELSE NULL END AS n_clearances_bad_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances_headed, 0)   ELSE NULL END AS n_clearances_headed_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances_bodypart_known, 0) ELSE NULL END AS n_clearances_bodypart_known_against

    FROM {{ ref('backbone') }} bb
    LEFT JOIN match_coverage mc
        ON mc.match_id = bb.match_id
    LEFT JOIN corner_match_agg cf
        ON cf.match_id = bb.match_id AND cf.team_id = bb.team_id
    LEFT JOIN corner_match_agg ca
        ON ca.match_id = bb.match_id AND ca.team_id = bb.opponent_id
)

SELECT
    match_id, team_id, date, season, league_source,

    {% for w in [3, 5, 10] %}
    {% set fg %}PARTITION BY team_id, season, league_source ORDER BY date ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING{% endset %}

    -- Volume, taux de conversion en action dangereuse, taux de conversion en but
    SUM(n_corners_for)   OVER ({{ fg }})                                                    AS corners_for_{{ w }},
    SUM(n_dangerous_for) OVER ({{ fg }}) / NULLIF(SUM(n_corners_for)   OVER ({{ fg }}), 0)   AS corner_danger_rate_for_{{ w }},
    SUM(n_goals_for)     OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_for) OVER ({{ fg }}), 0)   AS corner_conversion_rate_for_{{ w }},

    SUM(n_corners_against)   OVER ({{ fg }})                                                          AS corners_against_{{ w }},
    SUM(n_dangerous_against) OVER ({{ fg }}) / NULLIF(SUM(n_corners_against)   OVER ({{ fg }}), 0)     AS corner_danger_rate_against_{{ w }},
    SUM(n_goals_against)     OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_against) OVER ({{ fg }}), 0)     AS corner_conversion_rate_against_{{ w }},

    -- Intensité de danger continue (xG-par-corner) : magnitude du danger généré,
    -- pas seulement binaire dangereux/pas dangereux.
    SUM(sum_danger_for)     OVER ({{ fg }}) / NULLIF(SUM(n_corners_for)     OVER ({{ fg }}), 0)  AS corner_danger_intensity_for_{{ w }},
    SUM(sum_danger_against) OVER ({{ fg }}) / NULLIF(SUM(n_corners_against) OVER ({{ fg }}), 0)  AS corner_danger_intensity_against_{{ w }},

    -- Profil aérien : part des tirs / des buts issus de corners qui sont des têtes.
    SUM(n_headed_shots_for)     OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_for)     OVER ({{ fg }}), 0) AS corner_header_share_for_{{ w }},
    SUM(n_headed_shots_against) OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_against) OVER ({{ fg }}), 0) AS corner_header_share_against_{{ w }},
    SUM(n_headed_goals_for)     OVER ({{ fg }}) / NULLIF(SUM(n_goals_for)     OVER ({{ fg }}), 0)     AS corner_header_goal_share_for_{{ w }},
    SUM(n_headed_goals_against) OVER ({{ fg }}) / NULLIF(SUM(n_goals_against) OVER ({{ fg }}), 0)     AS corner_header_goal_share_against_{{ w }},

    -- Tendance tactique : part des corners joués courts plutôt que centrés.
    SUM(n_short_for)     OVER ({{ fg }}) / NULLIF(SUM(n_corners_for)     OVER ({{ fg }}), 0) AS corner_short_rate_for_{{ w }},
    SUM(n_short_against) OVER ({{ fg }}) / NULLIF(SUM(n_corners_against) OVER ({{ fg }}), 0) AS corner_short_rate_against_{{ w }},

    -- Bataille du dégagement (voir commentaire BASE pour la sémantique for/against) :
    --   forced_bad_clearance_rate_for : on force l'adversaire à mal dégager nos corners
    --   clearance_fail_rate_against    : on dégage mal nos propres corners concédés
    --   headed_clearance_rate_against  : part de nos dégagements défensifs faits de la tête
    SUM(n_clearances_bad_for)      OVER ({{ fg }}) / NULLIF(SUM(n_clearances_for)      OVER ({{ fg }}), 0) AS corner_forced_bad_clearance_rate_for_{{ w }},
    SUM(n_clearances_bad_against)  OVER ({{ fg }}) / NULLIF(SUM(n_clearances_against)  OVER ({{ fg }}), 0) AS corner_clearance_fail_rate_against_{{ w }},
    SUM(n_clearances_headed_against) OVER ({{ fg }}) / NULLIF(SUM(n_clearances_bodypart_known_against) OVER ({{ fg }}), 0) AS corner_headed_clearance_rate_against_{{ w }},
    {% endfor %}

FROM base

{% if is_incremental() %}
WHERE (match_id::VARCHAR || '_' || team_id) NOT IN (
    SELECT (match_id::VARCHAR || '_' || team_id) FROM {{ this }}
)
{% endif %}
