{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'int_row_num'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        alias='int_penalties'
    )
}}

-- Penaltys au grain « un penalty tiré » — base pour les features discipline /
-- finition sous pression / profil gardien en gold.
--
-- Un penalty laisse plusieurs traces dans WhoScored. On les relie par le
-- qualifier 233 « OppositeRelatedEvent » (déterministe, 100 % de couverture) :
--   • la FRAPPE  (type 13/14/15/16 portant qual 9)  → tireur, résultat, placement
--   • PenaltyFaced (type 58)                         → équipe défense + gardien
--       lien : PenaltyFaced.qual233 = frappe.event_id
--   • le FOUL en double (type 4 + qual 9)            → obtient / concède
--       - outcome_id=1 : côté « obtient »  (drawer, souvent NULL à la source)
--       - outcome_id=0 : côté « concède »  (conceder, toujours renseigné)
--       lien : foul_défensif.qual233 = foul_offensif.event_id
--
-- PIVOT = la frappe : couvre TOUS les penaltys tirés, résultat lisible via type_id.
-- Défense/gardien rattachés en LEFT JOIN déterministe (qual 233). Le foul est
-- rattaché à la frappe par RANG (aucun lien 233 entre grappe-foul et grappe-frappe,
-- et aucune fenêtre de temps) : les penaltys d'une équipe arrivent dans l'ordre du
-- row_num et #tirs == #fautes par équipe (vérifié), donc le k-ième tir d'une équipe
-- correspond à sa k-ième faute. Robuste aux penaltys successifs.
--
-- Comptes de référence (backfill partiel, validés read-only) :
--   3469 penaltys, 79 % convertis, défense/gardien 100 %, concédeur 100 %,
--   obtenteur 77 % (limite données : côté « obtient » NULL à la source WhoScored).

-- ══ Refonte nommage (préfixe de type en tête de nom : str_, int_, dec_, dt_, bool_) ══
-- Entrées : les modèles amont refondus sont relus via des CTE in_<modèle> qui les
-- remappent vers les noms/types de travail utilisés par la logique ci-dessous
-- (inchangée). Sortie : CTE mdl_out, renommage + cast selon le type logique.

WITH

-- events_qual lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_events_qual AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        int_row_num                                                  AS "row_num",
        CAST(str_qual_type_id AS INTEGER)                            AS "qual_type_id",
        str_qual_type_name                                           AS "qual_type_name",
        str_qual_value                                               AS "qual_value"
    FROM {{ ref('events_qual') }}
),

-- int_event_enriched lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_event_enriched AS (
    SELECT
        str_match_id                                                 AS "match_id",
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
        bool_is_touch                                                AS "is_touch",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y",
        dt_match_date                                                AS "match_date",
        str_season                                                   AS "season",
        str_league_source                                            AS "league_source",
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_is_leading_to_goal                                       AS "is_leading_to_goal",
        int_is_intentional_goal_assist                               AS "is_intentional_goal_assist",
        int_is_intentional_assist                                    AS "is_intentional_assist",
        int_is_big_chance_created                                    AS "is_big_chance_created",
        int_is_key_pass                                              AS "is_key_pass",
        int_is_shot_assist                                           AS "is_shot_assist",
        int_is_leading_to_attempt                                    AS "is_leading_to_attempt",
        int_has_defensive_qual                                       AS "has_defensive_qual",
        int_has_offensive_qual                                       AS "has_offensive_qual",
        int_has_opposite_event                                       AS "has_opposite_event",
        int_team_score                                               AS "team_score",
        int_opp_score                                                AS "opp_score"
    FROM {{ ref('int_event_enriched') }}
),

-- int_whoscored_events lu sous ses noms refondus, remappé vers les noms de travail du modèle
in_int_whoscored_events AS (
    SELECT
        str_match_id                                                 AS "match_id",
        CAST(str_team_id AS BIGINT)                                  AS "team_id",
        CAST(str_event_id AS INTEGER)                                AS "event_id",
        str_league_source                                            AS "league_source",
        str_season                                                   AS "season",
        int_minute                                                   AS "minute",
        int_second                                                   AS "second",
        int_expanded_minute                                          AS "expanded_minute",
        int_period                                                   AS "period",
        CAST(str_player_id AS INTEGER)                               AS "player_id",
        dec_x                                                        AS "x",
        dec_y                                                        AS "y",
        dec_end_x                                                    AS "end_x",
        dec_end_y                                                    AS "end_y",
        CAST(str_type_id AS INTEGER)                                 AS "type_id",
        str_type_name                                                AS "type_name",
        CAST(str_outcome_id AS INTEGER)                              AS "outcome_id",
        str_outcome_name                                             AS "outcome_name",
        bool_is_touch                                                AS "is_touch",
        bool_is_shot                                                 AS "is_shot",
        -- qualifiers_json non lu : absent de la sortie Spark (retiré par spark_events.py)
        CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
        int_row_num                                                  AS "row_num",
        bool_is_goal                                                 AS "is_goal",
        bool_is_own_goal                                             AS "is_own_goal",
        CAST(str_related_event_id AS INTEGER)                        AS "related_event_id",
        CAST(str_related_player_id AS INTEGER)                       AS "related_player_id",
        str_card_type                                                AS "card_type",
        dec_goal_mouth_y                                             AS "goal_mouth_y",
        dec_goal_mouth_z                                             AS "goal_mouth_z",
        dec_blocked_x                                                AS "blocked_x",
        dec_blocked_y                                                AS "blocked_y"
    FROM {{ ref('int_whoscored_events') }}
),

mdl_body AS (
WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
-- Même patron que int_event_enriched : on ne traite que les matchs dont le
-- scraped_at dépasse le dernier déjà présent dans la table cible. Penalty = grain
-- append-only (les penaltys passés ne changent pas) → incrémental correct.
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM (
    SELECT
            str_match_id                                                 AS "match_id",
            int_row_num                                                  AS "row_num",
            str_season                                                   AS "season",
            str_league_source                                            AS "league_source",
            CAST(dt_scraped_at AS VARCHAR)                               AS "scraped_at",
            int_expanded_minute                                          AS "expanded_minute",
            CAST(str_attacking_team_id AS BIGINT)                        AS "attacking_team_id",
            CAST(str_taker_player_id AS INTEGER)                         AS "taker_player_id",
            CAST(str_defending_team_id AS BIGINT)                        AS "defending_team_id",
            CAST(str_gk_player_id AS INTEGER)                            AS "gk_player_id",
            int_player_drew                                              AS "player_drew",
            int_player_conceded                                          AS "player_conceded",
            bool_is_goal                                                 AS "is_goal",
            bool_is_saved                                                AS "is_saved",
            bool_is_post                                                 AS "is_post",
            bool_is_off_target                                           AS "is_off_target",
            str_result_label                                             AS "result_label",
            dec_goal_mouth_y                                             AS "goal_mouth_y",
            dec_goal_mouth_z                                             AS "goal_mouth_z"
        FROM {{ this }}
    )
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM in_int_whoscored_events
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM in_int_whoscored_events
),
{% endif %}

-- ── Identifiants des events portant le qualifier « Penalty » (qual 9) ──────────
pen_ids AS (
    SELECT DISTINCT match_id, row_num
    FROM in_events_qual
    WHERE qual_type_id = 9
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ── OppositeRelatedEvent (qual 233) → event_id de l'event miroir adverse ───────
-- La valeur pointe vers l'event_id de l'event symétrique de l'autre équipe.
-- Sert à lier de façon déterministe : PenaltyFaced→frappe et conceder→drawer.
q233 AS (
    SELECT match_id, row_num, TRY_CAST(qual_value AS INTEGER) AS opposite_event_id
    FROM in_events_qual
    WHERE qual_type_id = 233
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ── PIVOT : la frappe ─────────────────────────────────────────────────────────
-- Source = int_event_enriched : porte goal_mouth_* (placement) + season /
-- league_source / scraped_at, cohérent avec int_shot_placement. Le tireur d'un
-- penalty n'est jamais NULL, donc le filtre player_id de l'enrichi ne perd rien.
-- int_event_enriched contient ~94 doublons (match_id, row_num) sur les tirs :
-- on garde une ligne par frappe pour l'unicité de la clé.
pen_shots AS (
    SELECT
        e.match_id,
        e.event_id,
        e.season,
        e.league_source,
        e.scraped_at,
        e.row_num,
        e.expanded_minute,
        e.team_id                       AS attacking_team_id,
        e.player_id                     AS taker_player_id,
        e.goal_mouth_y,
        e.goal_mouth_z,
        (e.type_id = 16)                AS is_goal,      -- marqué
        (e.type_id = 15)                AS is_saved,     -- arrêté par le gardien
        (e.type_id = 14)                AS is_post,      -- poteau
        (e.type_id = 13)                AS is_off_target -- hors cadre
    FROM in_int_event_enriched e
    JOIN pen_ids p
        ON p.match_id = e.match_id
       AND p.row_num  = e.row_num
    WHERE e.type_id IN (13, 14, 15, 16)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.match_id, e.row_num
                               ORDER BY e.event_id) = 1
),

-- Rang du penalty au sein de l'équipe (ordre chronologique du row_num). Calculé
-- APRÈS dédup, sinon les doublons de l'enrichi fausseraient le compte. Sert de
-- clé d'appariement avec la faute (k-ième tir ↔ k-ième faute de l'équipe).
shots AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY match_id, attacking_team_id
                           ORDER BY row_num) AS pen_seq
    FROM pen_shots
),

-- ── PenaltyFaced (type 58) → équipe qui défend + gardien ──────────────────────
-- Source = events bruts (pas l'enrichi) : on ne dépend d'aucun filtre player_id.
-- Tous les type 58 sont des penaltys → pas besoin de restreindre à qual 9.
-- Rattaché à la frappe par son qual 233 : PenaltyFaced.opposite = frappe.event_id.
faced AS (
    SELECT
        e.match_id,
        q.opposite_event_id  AS shot_event_id,   -- pointe vers la frappe
        e.team_id            AS defending_team_id,
        e.player_id          AS gk_player_id
    FROM in_int_whoscored_events e
    JOIN q233 q
        ON q.match_id = e.match_id
       AND q.row_num  = e.row_num
    WHERE e.type_id = 58
),

-- ── Le foul en double → joueurs qui obtiennent / concèdent ────────────────────
-- Source = events bruts : le côté « obtient » (outcome 1) est NULL à la source
-- dans ~24 % des cas ; le sourcer depuis l'enrichi (player_id NOT NULL) casserait
-- la paire et ferait perdre AUSSI le concédeur.
-- Appariement DÉTERMINISTE via qual 233 : le foul défensif pointe vers l'event_id
-- du foul offensif (100 % de couverture) → concédeur rattaché à l'obtenteur.
-- pen_seq = rang de la faute au sein de l'équipe qui obtient : clé d'appariement
-- avec la frappe en aval (aucune fenêtre de temps).
foul_pairs AS (
    SELECT
        o.match_id,
        o.won_team,
        o.pen_seq,
        o.player_drew,
        d.player_conceded
    FROM (
        SELECT e.match_id, e.event_id,
               e.team_id AS won_team, e.player_id AS player_drew,
               ROW_NUMBER() OVER (PARTITION BY e.match_id, e.team_id
                                  ORDER BY e.row_num) AS pen_seq
        FROM in_int_whoscored_events e
        JOIN pen_ids p ON p.match_id = e.match_id AND p.row_num = e.row_num
        WHERE e.type_id = 4 AND e.outcome_id = 1
    ) o
    LEFT JOIN (
        SELECT e.match_id, e.team_id AS conceded_team, e.player_id AS player_conceded,
               q.opposite_event_id
        FROM in_int_whoscored_events e
        JOIN pen_ids p ON p.match_id = e.match_id AND p.row_num = e.row_num
        JOIN q233 q     ON q.match_id = e.match_id AND q.row_num = e.row_num
        WHERE e.type_id = 4 AND e.outcome_id = 0
    ) d
        ON  d.match_id          = o.match_id
        AND d.opposite_event_id = o.event_id
        AND d.conceded_team    <> o.won_team   -- event_id scopé par équipe (garde)
)

-- ── ASSEMBLAGE ────────────────────────────────────────────────────────────────
SELECT
    s.match_id,
    s.row_num,                                   -- (match_id, row_num) = clé unique
    s.season,
    s.league_source,
    s.scraped_at,
    s.expanded_minute,

    -- Attaque
    s.attacking_team_id,
    s.taker_player_id,

    -- Défense (via type 58)
    f.defending_team_id,
    f.gk_player_id,

    -- Attribution de la faute (via le foul apparié par rang)
    fp.player_drew,
    fp.player_conceded,

    -- Résultat
    s.is_goal,
    s.is_saved,
    s.is_post,
    s.is_off_target,
    CASE
        WHEN s.is_goal       THEN 'scored'
        WHEN s.is_saved      THEN 'saved'
        WHEN s.is_post       THEN 'post'
        ELSE 'off_target'
    END                                          AS result_label,

    -- Placement (pour croiser avec int_shot_placement / profil gardien)
    s.goal_mouth_y,
    s.goal_mouth_z

FROM shots s

-- Défense + gardien : lien déterministe qual 233 (PenaltyFaced → frappe.event_id)
-- Garde équipe adverse : event_id est scopé par (match, équipe).
LEFT JOIN faced f
    ON  f.match_id          = s.match_id
    AND f.shot_event_id     = s.event_id
    AND f.defending_team_id <> s.attacking_team_id

-- Foul apparié par RANG : même équipe qui obtient, k-ième faute ↔ k-ième tir.
-- Déterministe, sans fenêtre de temps, robuste aux penaltys successifs.
LEFT JOIN foul_pairs fp
    ON  fp.match_id = s.match_id
    AND fp.won_team = s.attacking_team_id
    AND fp.pen_seq  = s.pen_seq
),

mdl_out AS (
    SELECT
        "match_id"                                                   AS str_match_id,
        "row_num"                                                    AS int_row_num,
        "season"                                                     AS str_season,
        "league_source"                                              AS str_league_source,
        TRY_CAST(scraped_at AS TIMESTAMP)                            AS dt_scraped_at,
        "expanded_minute"                                            AS int_expanded_minute,
        CAST(attacking_team_id AS VARCHAR)                           AS str_attacking_team_id,
        CAST(taker_player_id AS VARCHAR)                             AS str_taker_player_id,
        CAST(defending_team_id AS VARCHAR)                           AS str_defending_team_id,
        CAST(gk_player_id AS VARCHAR)                                AS str_gk_player_id,
        "player_drew"                                                AS int_player_drew,
        "player_conceded"                                            AS int_player_conceded,
        "is_goal"                                                    AS bool_is_goal,
        "is_saved"                                                   AS bool_is_saved,
        "is_post"                                                    AS bool_is_post,
        "is_off_target"                                              AS bool_is_off_target,
        "result_label"                                               AS str_result_label,
        "goal_mouth_y"                                               AS dec_goal_mouth_y,
        "goal_mouth_z"                                               AS dec_goal_mouth_z
    FROM mdl_body
)

SELECT * FROM mdl_out
