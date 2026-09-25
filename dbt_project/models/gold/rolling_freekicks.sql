{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id'],
        on_schema_change='sync_all_columns',
        schema='gold',
        alias='rolling_freekicks'
    )
}}

-- Refonte nommage : backbone lu sous ses nouveaux noms (CTE backbone_in), sorties
-- renommées selon docs/proposition_nommage_definitif.csv (CTE renamed). Le filtre
-- incrémental s'applique désormais APRÈS le calcul des fenêtres : avant, il filtrait
-- base avant les fenêtres, qui perdaient alors l'historique en run incrémental.

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- freekick_profiles lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_freekick_profiles AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_freekick_taker_id AS INTEGER)                       AS "freekick_taker_id",
        int_row_num                                                  AS "row_num",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        str_fk_type                                                  AS "fk_type",
        bool_is_offside                                              AS "is_offside",
        str_fk_zone_type                                             AS "fk_zone_type",
        str_outcome                                                  AS "outcome",
        dec_chain_danger_total                                       AS "chain_danger_total",
        dec_chain_danger_momentum                                    AS "chain_danger_momentum",
        str_shot_body_part                                           AS "shot_body_part",
        CAST(str_clearance_player_id AS INTEGER)                     AS "clearance_player_id",
        str_clearance_quality                                        AS "clearance_quality",
        bool_is_headed_clearance                                     AS "is_headed_clearance",
        dec_x_m                                                      AS "x_m",
        dec_y_m                                                      AS "y_m",
        dec_distance_to_goal                                         AS "distance_to_goal",
        dec_angle                                                    AS "angle"
    FROM {{ ref('freekick_profiles') }}
),

-- player_possession_chains lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_player_possession_chains AS (
    SELECT
        str_match_id                                                 AS "match_id",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        str_chain_id                                                 AS "chain_id",
        CAST(int_chain_number AS HUGEINT)                            AS "chain_number",
        CAST(str_chain_team_id AS BIGINT)                            AS "chain_team_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_row_num                                                  AS "row_num",
        int_expanded_minute                                          AS "expanded_minute",
        int_second                                                   AS "second",
        int_period                                                   AS "period",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_shot                                                 AS "is_shot",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        int_is_rupture                                               AS "is_rupture",
        str_chain_trigger                                            AS "chain_trigger",
        int_certain_possessor                                        AS "certain_possessor",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at"
    FROM {{ ref('player_possession_chains') }}
),

mdl_body AS (
WITH

backbone_in AS (
    SELECT
        str_match_id                       AS match_id,
        CAST(str_team_id AS BIGINT)       AS team_id,
        CAST(str_opponent_id AS BIGINT)   AS opponent_id,
        dt_date                            AS date,
        str_season                         AS season,
        str_league_source                  AS league_source
    FROM {{ ref('backbone') }}
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FREEKICK_ZONE_AGG
-- Agrégation NON filtrée de freekick_profiles : répartition tactique de TOUS
-- les coups francs obtenus par zone (direct/crossed/own_box/too_far). Sert à
-- mesurer le profil offensif d'une équipe (joue-t-elle beaucoup de coups francs
-- directs ou centrés ?), indépendamment de leur dangerosité.
-- ══════════════════════════════════════════════════════════════════════════════
freekick_zone_agg AS (
    SELECT
        match_id,
        chain_team_id                                                  AS team_id,
        COUNT(*)                                                       AS n_total,
        COUNT(*) FILTER (WHERE fk_zone_type = 'direct_shot')           AS n_direct,
        COUNT(*) FILTER (WHERE fk_zone_type = 'crossed')               AS n_crossed,
        COUNT(*) FILTER (WHERE fk_zone_type = 'own_box')               AS n_own_box,
        COUNT(*) FILTER (WHERE fk_zone_type = 'too_far')               AS n_too_far
    FROM in_freekick_profiles
    GROUP BY match_id, chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- FREEKICK_MATCH_AGG
-- Agrégation de freekick_profiles au grain match + équipe.
-- Filtré sur fk_zone_type IN ('crossed','direct_shot') : le reste (too_far,
-- own_box) n'a pas de danger réel et diluerait le signal.
-- Ajouts : intensité de danger continue, profil aérien (têtes), qualité des
-- dégagements (uniquement peuplée pour 'crossed', cf. freekick_profiles.sql),
-- et distance/angle moyens des coups francs directs (proxy de la qualité de
-- position — cf. littérature xG : distance/angle sont les features de base).
-- ══════════════════════════════════════════════════════════════════════════════
freekick_match_agg AS (
    SELECT
        match_id,
        chain_team_id                                                              AS team_id,
        COUNT(*)                                                                   AS n_freekicks,
        COUNT(*) FILTER (WHERE outcome IN ('goal','shot_saved','shot_off_target')) AS n_dangerous,
        COUNT(*) FILTER (WHERE outcome = 'goal')                                   AS n_goals,
        SUM(chain_danger_total)                                                    AS sum_danger,
        COUNT(*) FILTER (WHERE shot_body_part = 'head')                           AS n_headed_shots,
        COUNT(*) FILTER (WHERE outcome = 'goal' AND shot_body_part = 'head')      AS n_headed_goals,
        COUNT(*) FILTER (WHERE clearance_quality IS NOT NULL)                     AS n_clearances,
        COUNT(*) FILTER (WHERE clearance_quality IN ('poor', 'failed'))           AS n_clearances_bad,
        COUNT(*) FILTER (WHERE is_headed_clearance)                              AS n_clearances_headed,
        -- Dénominateur dédié pour le taux de tête (voir rolling_corners.sql pour
        -- le raisonnement : is_headed_clearance peut être connu même quand
        -- clearance_quality est NULL, ce qui cassait le ratio si on réutilisait
        -- n_clearances comme dénominateur).
        COUNT(*) FILTER (WHERE is_headed_clearance IS NOT NULL)                  AS n_clearances_bodypart_known,
        COUNT(*) FILTER (WHERE fk_zone_type = 'direct_shot')                     AS n_direct,
        SUM(distance_to_goal) FILTER (WHERE fk_zone_type = 'direct_shot')        AS sum_direct_distance,
        SUM(angle)            FILTER (WHERE fk_zone_type = 'direct_shot')        AS sum_direct_angle
    FROM in_freekick_profiles
    WHERE fk_zone_type IN ('crossed', 'direct_shot')
    GROUP BY match_id, chain_team_id
),

-- ══════════════════════════════════════════════════════════════════════════════
-- MATCH_COVERAGE
-- Un match a-t-il des données WhoScored du tout ? Sert à distinguer "0 corner
-- réel" de "pas de donnée" — freekick_profiles ne couvre que 4 compétitions
-- sur 28 (Serie A, Premier League, Ligue 1, Bundesliga).
-- ══════════════════════════════════════════════════════════════════════════════
match_coverage AS (
    SELECT DISTINCT match_id
    FROM in_player_possession_chains
),

-- ══════════════════════════════════════════════════════════════════════════════
-- BASE
-- Jointure sur backbone : "for" via team_id, "against" via opponent_id.
-- Même remarque que rolling_corners.sql pour la sémantique des champs de
-- dégagement (n_clearances*) : ils décrivent l'action de l'équipe QUI DÉFEND,
-- donc l'adversaire de team_id dans freekick_match_agg. Voir corner_profiles
-- pour le détail du raisonnement "for" (on force l'adversaire à mal dégager)
-- vs "against" (on dégage mal nos propres corners/coups francs concédés).
-- ══════════════════════════════════════════════════════════════════════════════
base AS (
    SELECT
        bb.match_id,
        bb.team_id,
        bb.date,
        bb.season,
        bb.league_source,

        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_freekicks, 0)          ELSE NULL END AS n_freekicks_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_dangerous, 0)          ELSE NULL END AS n_dangerous_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_goals, 0)              ELSE NULL END AS n_goals_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.sum_danger, 0)           ELSE NULL END AS sum_danger_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_headed_shots, 0)       ELSE NULL END AS n_headed_shots_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_headed_goals, 0)       ELSE NULL END AS n_headed_goals_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances, 0)         ELSE NULL END AS n_clearances_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances_bad, 0)     ELSE NULL END AS n_clearances_bad_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances_headed, 0)  ELSE NULL END AS n_clearances_headed_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_clearances_bodypart_known, 0) ELSE NULL END AS n_clearances_bodypart_known_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.n_direct, 0)             ELSE NULL END AS n_direct_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.sum_direct_distance, 0)  ELSE NULL END AS sum_direct_distance_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(cf.sum_direct_angle, 0)     ELSE NULL END AS sum_direct_angle_for,

        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_freekicks, 0)          ELSE NULL END AS n_freekicks_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_dangerous, 0)          ELSE NULL END AS n_dangerous_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_goals, 0)              ELSE NULL END AS n_goals_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.sum_danger, 0)           ELSE NULL END AS sum_danger_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_headed_shots, 0)       ELSE NULL END AS n_headed_shots_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_headed_goals, 0)       ELSE NULL END AS n_headed_goals_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances, 0)         ELSE NULL END AS n_clearances_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances_bad, 0)     ELSE NULL END AS n_clearances_bad_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances_headed, 0)  ELSE NULL END AS n_clearances_headed_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_clearances_bodypart_known, 0) ELSE NULL END AS n_clearances_bodypart_known_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.n_direct, 0)             ELSE NULL END AS n_direct_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.sum_direct_distance, 0)  ELSE NULL END AS sum_direct_distance_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(ca.sum_direct_angle, 0)     ELSE NULL END AS sum_direct_angle_against,

        -- Répartition tactique (non filtrée) : tous les coups francs obtenus/concédés
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(zf.n_total, 0)              ELSE NULL END AS n_total_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(zf.n_direct, 0)             ELSE NULL END AS n_zone_direct_for,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(zf.n_crossed, 0)            ELSE NULL END AS n_zone_crossed_for,

        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(za.n_total, 0)              ELSE NULL END AS n_total_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(za.n_direct, 0)             ELSE NULL END AS n_zone_direct_against,
        CASE WHEN mc.match_id IS NOT NULL THEN COALESCE(za.n_crossed, 0)            ELSE NULL END AS n_zone_crossed_against

    FROM backbone_in bb
    LEFT JOIN match_coverage mc
        ON mc.match_id = bb.match_id
    LEFT JOIN freekick_match_agg cf
        ON cf.match_id = bb.match_id AND cf.team_id = bb.team_id
    LEFT JOIN freekick_match_agg ca
        ON ca.match_id = bb.match_id AND ca.team_id = bb.opponent_id
    LEFT JOIN freekick_zone_agg zf
        ON zf.match_id = bb.match_id AND zf.team_id = bb.team_id
    LEFT JOIN freekick_zone_agg za
        ON za.match_id = bb.match_id AND za.team_id = bb.opponent_id
),

rolled AS (
SELECT
    match_id, team_id, date, season, league_source,

    {% for w in [3, 5, 10] %}
    {% set fg %}PARTITION BY team_id, season, league_source ORDER BY date ROWS BETWEEN {{ w }} PRECEDING AND 1 PRECEDING{% endset %}

    SUM(n_freekicks_for) OVER ({{ fg }})                                                  AS freekicks_for_{{ w }},
    SUM(n_dangerous_for) OVER ({{ fg }}) / NULLIF(SUM(n_freekicks_for) OVER ({{ fg }}), 0) AS freekick_danger_rate_for_{{ w }},
    SUM(n_goals_for)     OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_for) OVER ({{ fg }}), 0) AS freekick_conversion_rate_for_{{ w }},

    SUM(n_freekicks_against) OVER ({{ fg }})                                                        AS freekicks_against_{{ w }},
    SUM(n_dangerous_against) OVER ({{ fg }}) / NULLIF(SUM(n_freekicks_against) OVER ({{ fg }}), 0)   AS freekick_danger_rate_against_{{ w }},
    SUM(n_goals_against)     OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_against) OVER ({{ fg }}), 0)    AS freekick_conversion_rate_against_{{ w }},

    -- Intensité de danger continue (xG-par-coup-franc)
    SUM(sum_danger_for)     OVER ({{ fg }}) / NULLIF(SUM(n_freekicks_for)     OVER ({{ fg }}), 0) AS freekick_danger_intensity_for_{{ w }},
    SUM(sum_danger_against) OVER ({{ fg }}) / NULLIF(SUM(n_freekicks_against) OVER ({{ fg }}), 0) AS freekick_danger_intensity_against_{{ w }},

    -- Profil aérien (uniquement pertinent sur les coups francs centrés)
    SUM(n_headed_shots_for)     OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_for)     OVER ({{ fg }}), 0) AS freekick_header_share_for_{{ w }},
    SUM(n_headed_shots_against) OVER ({{ fg }}) / NULLIF(SUM(n_dangerous_against) OVER ({{ fg }}), 0) AS freekick_header_share_against_{{ w }},
    SUM(n_headed_goals_for)     OVER ({{ fg }}) / NULLIF(SUM(n_goals_for)     OVER ({{ fg }}), 0)     AS freekick_header_goal_share_for_{{ w }},
    SUM(n_headed_goals_against) OVER ({{ fg }}) / NULLIF(SUM(n_goals_against) OVER ({{ fg }}), 0)     AS freekick_header_goal_share_against_{{ w }},

    -- Bataille du dégagement (coups francs centrés uniquement, cf. freekick_profiles.sql)
    SUM(n_clearances_bad_for)        OVER ({{ fg }}) / NULLIF(SUM(n_clearances_for)       OVER ({{ fg }}), 0) AS freekick_forced_bad_clearance_rate_for_{{ w }},
    SUM(n_clearances_bad_against)    OVER ({{ fg }}) / NULLIF(SUM(n_clearances_against)   OVER ({{ fg }}), 0) AS freekick_clearance_fail_rate_against_{{ w }},
    SUM(n_clearances_headed_against) OVER ({{ fg }}) / NULLIF(SUM(n_clearances_bodypart_known_against) OVER ({{ fg }}), 0) AS freekick_headed_clearance_rate_against_{{ w }},

    -- Qualité de position des coups francs directs obtenus/concédés (proxy xG :
    -- distance en mètres, angle en radians vers le but). "against" = discipline
    -- défensive : concède-t-on des fautes proches et en face du but ?
    SUM(sum_direct_distance_for)     OVER ({{ fg }}) / NULLIF(SUM(n_direct_for)     OVER ({{ fg }}), 0) AS freekick_direct_distance_avg_for_{{ w }},
    SUM(sum_direct_distance_against) OVER ({{ fg }}) / NULLIF(SUM(n_direct_against) OVER ({{ fg }}), 0) AS freekick_direct_distance_avg_against_{{ w }},
    SUM(sum_direct_angle_for)        OVER ({{ fg }}) / NULLIF(SUM(n_direct_for)     OVER ({{ fg }}), 0) AS freekick_direct_angle_avg_for_{{ w }},
    SUM(sum_direct_angle_against)    OVER ({{ fg }}) / NULLIF(SUM(n_direct_against) OVER ({{ fg }}), 0) AS freekick_direct_angle_avg_against_{{ w }},

    -- Profil tactique : part des coups francs obtenus/concédés joués en direct vs centrés
    SUM(n_zone_direct_for)      OVER ({{ fg }}) / NULLIF(SUM(n_total_for)     OVER ({{ fg }}), 0) AS freekick_zone_direct_rate_for_{{ w }},
    SUM(n_zone_direct_against)  OVER ({{ fg }}) / NULLIF(SUM(n_total_against) OVER ({{ fg }}), 0) AS freekick_zone_direct_rate_against_{{ w }},
    SUM(n_zone_crossed_for)     OVER ({{ fg }}) / NULLIF(SUM(n_total_for)     OVER ({{ fg }}), 0) AS freekick_zone_crossed_rate_for_{{ w }},
    SUM(n_zone_crossed_against) OVER ({{ fg }}) / NULLIF(SUM(n_total_against) OVER ({{ fg }}), 0) AS freekick_zone_crossed_rate_against_{{ w }},
    {% endfor %}

FROM base
),

-- Renommage final (docs/proposition_nommage_definitif.csv)
renamed AS (
    SELECT
        match_id                                             AS str_match_id,
        CAST(team_id AS VARCHAR)                             AS str_team_id,
        date                                                 AS dt_date,
        season                                               AS str_season,
        league_source                                        AS str_league_source,
        freekicks_for_3                                      AS int_freekicks_for_3,
        freekick_danger_rate_for_3                           AS dec_freekick_danger_rate_for_3,
        freekick_conversion_rate_for_3                       AS dec_freekick_conversion_rate_for_3,
        freekicks_against_3                                  AS int_freekicks_against_3,
        freekick_danger_rate_against_3                       AS dec_freekick_danger_rate_against_3,
        freekick_conversion_rate_against_3                   AS dec_freekick_conversion_rate_against_3,
        freekick_danger_intensity_for_3                      AS dec_freekick_danger_intensity_for_3,
        freekick_danger_intensity_against_3                  AS dec_freekick_danger_intensity_against_3,
        freekick_header_share_for_3                          AS dec_freekick_header_share_for_3,
        freekick_header_share_against_3                      AS dec_freekick_header_share_against_3,
        freekick_header_goal_share_for_3                     AS dec_freekick_header_goal_share_for_3,
        freekick_header_goal_share_against_3                 AS dec_freekick_header_goal_share_against_3,
        freekick_forced_bad_clearance_rate_for_3             AS dec_freekick_forced_bad_clearance_rate_for_3,
        freekick_clearance_fail_rate_against_3               AS dec_freekick_clearance_fail_rate_against_3,
        freekick_headed_clearance_rate_against_3             AS dec_freekick_headed_clearance_rate_against_3,
        freekick_direct_distance_avg_for_3                   AS dec_freekick_direct_distance_avg_for_3,
        freekick_direct_distance_avg_against_3               AS dec_freekick_direct_distance_avg_against_3,
        freekick_direct_angle_avg_for_3                      AS dec_freekick_direct_angle_avg_for_3,
        freekick_direct_angle_avg_against_3                  AS dec_freekick_direct_angle_avg_against_3,
        freekick_zone_direct_rate_for_3                      AS dec_freekick_zone_direct_rate_for_3,
        freekick_zone_direct_rate_against_3                  AS dec_freekick_zone_direct_rate_against_3,
        freekick_zone_crossed_rate_for_3                     AS dec_freekick_zone_crossed_rate_for_3,
        freekick_zone_crossed_rate_against_3                 AS dec_freekick_zone_crossed_rate_against_3,
        freekicks_for_5                                      AS int_freekicks_for_5,
        freekick_danger_rate_for_5                           AS dec_freekick_danger_rate_for_5,
        freekick_conversion_rate_for_5                       AS dec_freekick_conversion_rate_for_5,
        freekicks_against_5                                  AS int_freekicks_against_5,
        freekick_danger_rate_against_5                       AS dec_freekick_danger_rate_against_5,
        freekick_conversion_rate_against_5                   AS dec_freekick_conversion_rate_against_5,
        freekick_danger_intensity_for_5                      AS dec_freekick_danger_intensity_for_5,
        freekick_danger_intensity_against_5                  AS dec_freekick_danger_intensity_against_5,
        freekick_header_share_for_5                          AS dec_freekick_header_share_for_5,
        freekick_header_share_against_5                      AS dec_freekick_header_share_against_5,
        freekick_header_goal_share_for_5                     AS dec_freekick_header_goal_share_for_5,
        freekick_header_goal_share_against_5                 AS dec_freekick_header_goal_share_against_5,
        freekick_forced_bad_clearance_rate_for_5             AS dec_freekick_forced_bad_clearance_rate_for_5,
        freekick_clearance_fail_rate_against_5               AS dec_freekick_clearance_fail_rate_against_5,
        freekick_headed_clearance_rate_against_5             AS dec_freekick_headed_clearance_rate_against_5,
        freekick_direct_distance_avg_for_5                   AS dec_freekick_direct_distance_avg_for_5,
        freekick_direct_distance_avg_against_5               AS dec_freekick_direct_distance_avg_against_5,
        freekick_direct_angle_avg_for_5                      AS dec_freekick_direct_angle_avg_for_5,
        freekick_direct_angle_avg_against_5                  AS dec_freekick_direct_angle_avg_against_5,
        freekick_zone_direct_rate_for_5                      AS dec_freekick_zone_direct_rate_for_5,
        freekick_zone_direct_rate_against_5                  AS dec_freekick_zone_direct_rate_against_5,
        freekick_zone_crossed_rate_for_5                     AS dec_freekick_zone_crossed_rate_for_5,
        freekick_zone_crossed_rate_against_5                 AS dec_freekick_zone_crossed_rate_against_5,
        freekicks_for_10                                     AS int_freekicks_for_10,
        freekick_danger_rate_for_10                          AS dec_freekick_danger_rate_for_10,
        freekick_conversion_rate_for_10                      AS dec_freekick_conversion_rate_for_10,
        freekicks_against_10                                 AS int_freekicks_against_10,
        freekick_danger_rate_against_10                      AS dec_freekick_danger_rate_against_10,
        freekick_conversion_rate_against_10                  AS dec_freekick_conversion_rate_against_10,
        freekick_danger_intensity_for_10                     AS dec_freekick_danger_intensity_for_10,
        freekick_danger_intensity_against_10                 AS dec_freekick_danger_intensity_against_10,
        freekick_header_share_for_10                         AS dec_freekick_header_share_for_10,
        freekick_header_share_against_10                     AS dec_freekick_header_share_against_10,
        freekick_header_goal_share_for_10                    AS dec_freekick_header_goal_share_for_10,
        freekick_header_goal_share_against_10                AS dec_freekick_header_goal_share_against_10,
        freekick_forced_bad_clearance_rate_for_10            AS dec_freekick_forced_bad_clearance_rate_for_10,
        freekick_clearance_fail_rate_against_10              AS dec_freekick_clearance_fail_rate_against_10,
        freekick_headed_clearance_rate_against_10            AS dec_freekick_headed_clearance_rate_against_10,
        freekick_direct_distance_avg_for_10                  AS dec_freekick_direct_distance_avg_for_10,
        freekick_direct_distance_avg_against_10              AS dec_freekick_direct_distance_avg_against_10,
        freekick_direct_angle_avg_for_10                     AS dec_freekick_direct_angle_avg_for_10,
        freekick_direct_angle_avg_against_10                 AS dec_freekick_direct_angle_avg_against_10,
        freekick_zone_direct_rate_for_10                     AS dec_freekick_zone_direct_rate_for_10,
        freekick_zone_direct_rate_against_10                 AS dec_freekick_zone_direct_rate_against_10,
        freekick_zone_crossed_rate_for_10                    AS dec_freekick_zone_crossed_rate_for_10,
        freekick_zone_crossed_rate_against_10                AS dec_freekick_zone_crossed_rate_against_10
    FROM rolled
)

SELECT * FROM renamed

{% if is_incremental() %}
WHERE (str_match_id || '_' || str_team_id) NOT IN (
    SELECT (str_match_id || '_' || str_team_id) FROM (
    SELECT
            str_match_id                                                 AS "str_match_id",
            str_team_id                                                  AS "str_team_id",
            dt_date                                                      AS "dt_date",
            str_season                                                   AS "str_season",
            str_league_source                                            AS "str_league_source",
            CAST(int_freekicks_for_3 AS HUGEINT)                         AS "int_freekicks_for_3",
            dec_freekick_danger_rate_for_3                               AS "dec_freekick_danger_rate_for_3",
            dec_freekick_conversion_rate_for_3                           AS "dec_freekick_conversion_rate_for_3",
            CAST(int_freekicks_against_3 AS HUGEINT)                     AS "int_freekicks_against_3",
            dec_freekick_danger_rate_against_3                           AS "dec_freekick_danger_rate_against_3",
            dec_freekick_conversion_rate_against_3                       AS "dec_freekick_conversion_rate_against_3",
            dec_freekick_danger_intensity_for_3                          AS "dec_freekick_danger_intensity_for_3",
            dec_freekick_danger_intensity_against_3                      AS "dec_freekick_danger_intensity_against_3",
            dec_freekick_header_share_for_3                              AS "dec_freekick_header_share_for_3",
            dec_freekick_header_share_against_3                          AS "dec_freekick_header_share_against_3",
            dec_freekick_header_goal_share_for_3                         AS "dec_freekick_header_goal_share_for_3",
            dec_freekick_header_goal_share_against_3                     AS "dec_freekick_header_goal_share_against_3",
            dec_freekick_forced_bad_clearance_rate_for_3                 AS "dec_freekick_forced_bad_clearance_rate_for_3",
            dec_freekick_clearance_fail_rate_against_3                   AS "dec_freekick_clearance_fail_rate_against_3",
            dec_freekick_headed_clearance_rate_against_3                 AS "dec_freekick_headed_clearance_rate_against_3",
            dec_freekick_direct_distance_avg_for_3                       AS "dec_freekick_direct_distance_avg_for_3",
            dec_freekick_direct_distance_avg_against_3                   AS "dec_freekick_direct_distance_avg_against_3",
            dec_freekick_direct_angle_avg_for_3                          AS "dec_freekick_direct_angle_avg_for_3",
            dec_freekick_direct_angle_avg_against_3                      AS "dec_freekick_direct_angle_avg_against_3",
            dec_freekick_zone_direct_rate_for_3                          AS "dec_freekick_zone_direct_rate_for_3",
            dec_freekick_zone_direct_rate_against_3                      AS "dec_freekick_zone_direct_rate_against_3",
            dec_freekick_zone_crossed_rate_for_3                         AS "dec_freekick_zone_crossed_rate_for_3",
            dec_freekick_zone_crossed_rate_against_3                     AS "dec_freekick_zone_crossed_rate_against_3",
            CAST(int_freekicks_for_5 AS HUGEINT)                         AS "int_freekicks_for_5",
            dec_freekick_danger_rate_for_5                               AS "dec_freekick_danger_rate_for_5",
            dec_freekick_conversion_rate_for_5                           AS "dec_freekick_conversion_rate_for_5",
            CAST(int_freekicks_against_5 AS HUGEINT)                     AS "int_freekicks_against_5",
            dec_freekick_danger_rate_against_5                           AS "dec_freekick_danger_rate_against_5",
            dec_freekick_conversion_rate_against_5                       AS "dec_freekick_conversion_rate_against_5",
            dec_freekick_danger_intensity_for_5                          AS "dec_freekick_danger_intensity_for_5",
            dec_freekick_danger_intensity_against_5                      AS "dec_freekick_danger_intensity_against_5",
            dec_freekick_header_share_for_5                              AS "dec_freekick_header_share_for_5",
            dec_freekick_header_share_against_5                          AS "dec_freekick_header_share_against_5",
            dec_freekick_header_goal_share_for_5                         AS "dec_freekick_header_goal_share_for_5",
            dec_freekick_header_goal_share_against_5                     AS "dec_freekick_header_goal_share_against_5",
            dec_freekick_forced_bad_clearance_rate_for_5                 AS "dec_freekick_forced_bad_clearance_rate_for_5",
            dec_freekick_clearance_fail_rate_against_5                   AS "dec_freekick_clearance_fail_rate_against_5",
            dec_freekick_headed_clearance_rate_against_5                 AS "dec_freekick_headed_clearance_rate_against_5",
            dec_freekick_direct_distance_avg_for_5                       AS "dec_freekick_direct_distance_avg_for_5",
            dec_freekick_direct_distance_avg_against_5                   AS "dec_freekick_direct_distance_avg_against_5",
            dec_freekick_direct_angle_avg_for_5                          AS "dec_freekick_direct_angle_avg_for_5",
            dec_freekick_direct_angle_avg_against_5                      AS "dec_freekick_direct_angle_avg_against_5",
            dec_freekick_zone_direct_rate_for_5                          AS "dec_freekick_zone_direct_rate_for_5",
            dec_freekick_zone_direct_rate_against_5                      AS "dec_freekick_zone_direct_rate_against_5",
            dec_freekick_zone_crossed_rate_for_5                         AS "dec_freekick_zone_crossed_rate_for_5",
            dec_freekick_zone_crossed_rate_against_5                     AS "dec_freekick_zone_crossed_rate_against_5",
            CAST(int_freekicks_for_10 AS HUGEINT)                        AS "int_freekicks_for_10",
            dec_freekick_danger_rate_for_10                              AS "dec_freekick_danger_rate_for_10",
            dec_freekick_conversion_rate_for_10                          AS "dec_freekick_conversion_rate_for_10",
            CAST(int_freekicks_against_10 AS HUGEINT)                    AS "int_freekicks_against_10",
            dec_freekick_danger_rate_against_10                          AS "dec_freekick_danger_rate_against_10",
            dec_freekick_conversion_rate_against_10                      AS "dec_freekick_conversion_rate_against_10",
            dec_freekick_danger_intensity_for_10                         AS "dec_freekick_danger_intensity_for_10",
            dec_freekick_danger_intensity_against_10                     AS "dec_freekick_danger_intensity_against_10",
            dec_freekick_header_share_for_10                             AS "dec_freekick_header_share_for_10",
            dec_freekick_header_share_against_10                         AS "dec_freekick_header_share_against_10",
            dec_freekick_header_goal_share_for_10                        AS "dec_freekick_header_goal_share_for_10",
            dec_freekick_header_goal_share_against_10                    AS "dec_freekick_header_goal_share_against_10",
            dec_freekick_forced_bad_clearance_rate_for_10                AS "dec_freekick_forced_bad_clearance_rate_for_10",
            dec_freekick_clearance_fail_rate_against_10                  AS "dec_freekick_clearance_fail_rate_against_10",
            dec_freekick_headed_clearance_rate_against_10                AS "dec_freekick_headed_clearance_rate_against_10",
            dec_freekick_direct_distance_avg_for_10                      AS "dec_freekick_direct_distance_avg_for_10",
            dec_freekick_direct_distance_avg_against_10                  AS "dec_freekick_direct_distance_avg_against_10",
            dec_freekick_direct_angle_avg_for_10                         AS "dec_freekick_direct_angle_avg_for_10",
            dec_freekick_direct_angle_avg_against_10                     AS "dec_freekick_direct_angle_avg_against_10",
            dec_freekick_zone_direct_rate_for_10                         AS "dec_freekick_zone_direct_rate_for_10",
            dec_freekick_zone_direct_rate_against_10                     AS "dec_freekick_zone_direct_rate_against_10",
            dec_freekick_zone_crossed_rate_for_10                        AS "dec_freekick_zone_crossed_rate_for_10",
            dec_freekick_zone_crossed_rate_against_10                    AS "dec_freekick_zone_crossed_rate_against_10"
        FROM {{ this }}
    )
)
{% endif %}
),

mdl_out AS (
    SELECT
        "str_match_id"                                               AS str_match_id,
        "str_team_id"                                                AS str_team_id,
        "dt_date"                                                    AS dt_date,
        "str_season"                                                 AS str_season,
        "str_league_source"                                          AS str_league_source,
        CAST(int_freekicks_for_3 AS BIGINT)                          AS int_freekicks_for_3,
        "dec_freekick_danger_rate_for_3"                             AS dec_freekick_danger_rate_for_3,
        "dec_freekick_conversion_rate_for_3"                         AS dec_freekick_conversion_rate_for_3,
        CAST(int_freekicks_against_3 AS BIGINT)                      AS int_freekicks_against_3,
        "dec_freekick_danger_rate_against_3"                         AS dec_freekick_danger_rate_against_3,
        "dec_freekick_conversion_rate_against_3"                     AS dec_freekick_conversion_rate_against_3,
        "dec_freekick_danger_intensity_for_3"                        AS dec_freekick_danger_intensity_for_3,
        "dec_freekick_danger_intensity_against_3"                    AS dec_freekick_danger_intensity_against_3,
        "dec_freekick_header_share_for_3"                            AS dec_freekick_header_share_for_3,
        "dec_freekick_header_share_against_3"                        AS dec_freekick_header_share_against_3,
        "dec_freekick_header_goal_share_for_3"                       AS dec_freekick_header_goal_share_for_3,
        "dec_freekick_header_goal_share_against_3"                   AS dec_freekick_header_goal_share_against_3,
        "dec_freekick_forced_bad_clearance_rate_for_3"               AS dec_freekick_forced_bad_clearance_rate_for_3,
        "dec_freekick_clearance_fail_rate_against_3"                 AS dec_freekick_clearance_fail_rate_against_3,
        "dec_freekick_headed_clearance_rate_against_3"               AS dec_freekick_headed_clearance_rate_against_3,
        "dec_freekick_direct_distance_avg_for_3"                     AS dec_freekick_direct_distance_avg_for_3,
        "dec_freekick_direct_distance_avg_against_3"                 AS dec_freekick_direct_distance_avg_against_3,
        "dec_freekick_direct_angle_avg_for_3"                        AS dec_freekick_direct_angle_avg_for_3,
        "dec_freekick_direct_angle_avg_against_3"                    AS dec_freekick_direct_angle_avg_against_3,
        "dec_freekick_zone_direct_rate_for_3"                        AS dec_freekick_zone_direct_rate_for_3,
        "dec_freekick_zone_direct_rate_against_3"                    AS dec_freekick_zone_direct_rate_against_3,
        "dec_freekick_zone_crossed_rate_for_3"                       AS dec_freekick_zone_crossed_rate_for_3,
        "dec_freekick_zone_crossed_rate_against_3"                   AS dec_freekick_zone_crossed_rate_against_3,
        CAST(int_freekicks_for_5 AS BIGINT)                          AS int_freekicks_for_5,
        "dec_freekick_danger_rate_for_5"                             AS dec_freekick_danger_rate_for_5,
        "dec_freekick_conversion_rate_for_5"                         AS dec_freekick_conversion_rate_for_5,
        CAST(int_freekicks_against_5 AS BIGINT)                      AS int_freekicks_against_5,
        "dec_freekick_danger_rate_against_5"                         AS dec_freekick_danger_rate_against_5,
        "dec_freekick_conversion_rate_against_5"                     AS dec_freekick_conversion_rate_against_5,
        "dec_freekick_danger_intensity_for_5"                        AS dec_freekick_danger_intensity_for_5,
        "dec_freekick_danger_intensity_against_5"                    AS dec_freekick_danger_intensity_against_5,
        "dec_freekick_header_share_for_5"                            AS dec_freekick_header_share_for_5,
        "dec_freekick_header_share_against_5"                        AS dec_freekick_header_share_against_5,
        "dec_freekick_header_goal_share_for_5"                       AS dec_freekick_header_goal_share_for_5,
        "dec_freekick_header_goal_share_against_5"                   AS dec_freekick_header_goal_share_against_5,
        "dec_freekick_forced_bad_clearance_rate_for_5"               AS dec_freekick_forced_bad_clearance_rate_for_5,
        "dec_freekick_clearance_fail_rate_against_5"                 AS dec_freekick_clearance_fail_rate_against_5,
        "dec_freekick_headed_clearance_rate_against_5"               AS dec_freekick_headed_clearance_rate_against_5,
        "dec_freekick_direct_distance_avg_for_5"                     AS dec_freekick_direct_distance_avg_for_5,
        "dec_freekick_direct_distance_avg_against_5"                 AS dec_freekick_direct_distance_avg_against_5,
        "dec_freekick_direct_angle_avg_for_5"                        AS dec_freekick_direct_angle_avg_for_5,
        "dec_freekick_direct_angle_avg_against_5"                    AS dec_freekick_direct_angle_avg_against_5,
        "dec_freekick_zone_direct_rate_for_5"                        AS dec_freekick_zone_direct_rate_for_5,
        "dec_freekick_zone_direct_rate_against_5"                    AS dec_freekick_zone_direct_rate_against_5,
        "dec_freekick_zone_crossed_rate_for_5"                       AS dec_freekick_zone_crossed_rate_for_5,
        "dec_freekick_zone_crossed_rate_against_5"                   AS dec_freekick_zone_crossed_rate_against_5,
        CAST(int_freekicks_for_10 AS BIGINT)                         AS int_freekicks_for_10,
        "dec_freekick_danger_rate_for_10"                            AS dec_freekick_danger_rate_for_10,
        "dec_freekick_conversion_rate_for_10"                        AS dec_freekick_conversion_rate_for_10,
        CAST(int_freekicks_against_10 AS BIGINT)                     AS int_freekicks_against_10,
        "dec_freekick_danger_rate_against_10"                        AS dec_freekick_danger_rate_against_10,
        "dec_freekick_conversion_rate_against_10"                    AS dec_freekick_conversion_rate_against_10,
        "dec_freekick_danger_intensity_for_10"                       AS dec_freekick_danger_intensity_for_10,
        "dec_freekick_danger_intensity_against_10"                   AS dec_freekick_danger_intensity_against_10,
        "dec_freekick_header_share_for_10"                           AS dec_freekick_header_share_for_10,
        "dec_freekick_header_share_against_10"                       AS dec_freekick_header_share_against_10,
        "dec_freekick_header_goal_share_for_10"                      AS dec_freekick_header_goal_share_for_10,
        "dec_freekick_header_goal_share_against_10"                  AS dec_freekick_header_goal_share_against_10,
        "dec_freekick_forced_bad_clearance_rate_for_10"              AS dec_freekick_forced_bad_clearance_rate_for_10,
        "dec_freekick_clearance_fail_rate_against_10"                AS dec_freekick_clearance_fail_rate_against_10,
        "dec_freekick_headed_clearance_rate_against_10"              AS dec_freekick_headed_clearance_rate_against_10,
        "dec_freekick_direct_distance_avg_for_10"                    AS dec_freekick_direct_distance_avg_for_10,
        "dec_freekick_direct_distance_avg_against_10"                AS dec_freekick_direct_distance_avg_against_10,
        "dec_freekick_direct_angle_avg_for_10"                       AS dec_freekick_direct_angle_avg_for_10,
        "dec_freekick_direct_angle_avg_against_10"                   AS dec_freekick_direct_angle_avg_against_10,
        "dec_freekick_zone_direct_rate_for_10"                       AS dec_freekick_zone_direct_rate_for_10,
        "dec_freekick_zone_direct_rate_against_10"                   AS dec_freekick_zone_direct_rate_against_10,
        "dec_freekick_zone_crossed_rate_for_10"                      AS dec_freekick_zone_crossed_rate_for_10,
        "dec_freekick_zone_crossed_rate_against_10"                  AS dec_freekick_zone_crossed_rate_against_10
    FROM mdl_body
)

SELECT * FROM mdl_out
