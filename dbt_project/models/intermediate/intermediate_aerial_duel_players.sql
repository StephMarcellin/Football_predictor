{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'int_event_id', 'int_opp_event_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        tags=['by_season'],
        alias='intermediate_aerial_duel_players'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- intermediate.intermediate_aerial_duel_players
-- Grain : UNE LIGNE PAR DUEL AÉRIEN ET PAR JOUEUR (2 lignes par duel).
-- Clé   : (str_match_id, str_team_id, int_event_id, int_opp_event_id). L'id d'événement
--         est propre à l'équipe : il ne suffit pas seul à identifier un événement dans un
--         match. L'id de l'événement adverse est dans la clé car, dans de rares duels à
--         trois (55 lignes sur 146 000 en 2013-2014, aucun en 2023-2024), un même
--         événement est lié à deux événements adverses : il donne alors deux lignes.
--
-- Répond à : « quels duels aériens a disputés ce joueur, contre qui, sur quel type de
-- ballon, et comment l'action s'est-elle conclue ? »
--
--   · Adversaire : lu dans intermediate_duel_pairs (pairage par le qualifier 233).
--   · Rôle : qualifiers 285 (défensif) / 286 (offensif) de l'événement aérien.
--   · Type de ballon : on remonte à l'événement précédent qui n'est PAS un aérien (la
--     passe, le dégagement… qui a envoyé le ballon), et on lit ses qualifiers.
--     Une prolongation de la tête apparaît comme une « passe de la tête » intercalée
--     entre deux duels : le second duel a alors pour type de ballon 'passe_tete'.
--   · Conclusion : type de l'événement suivant le duel et tirs / buts de chaque équipe
--     dans les 10 secondes suivantes. L'événement suivant est TOUJOURS celui de l'équipe
--     gagnante (100 % observé sur 2023-2024) et, dans 99,9 % des cas, du joueur qui a
--     gagné le duel : il décrit ce que fait le vainqueur de son ballon (type, réussite,
--     coordonnées dans le repère de l'équipe gagnante, tête ou non). Sur la ligne du
--     perdant, ces colonnes décrivent donc le geste de son adversaire.
--     Le danger créé (xT) n'est pas calculé ici : la grille xT est estimée par une étape
--     Python postérieure aux modèles par saison. int_duel_last_row_num et
--     int_next_event_row_num permettent de joindre intermediate_xt_actions et
--     machine_learning.xt_grid dans un modèle aval.
--   · Le rôle (offensif / défensif) est celui de WhoScored, pas dérivé de la source :
--     l'équipe qui a envoyé le ballon n'est « offensive » que dans 76 % des cas.
--
-- Seuils posés par hypothèse, à valider : centre « extérieur » si la passe part de
-- y <= 21 ou y >= 79 (échelle 0-100) ; fenêtre de conclusion = 10 s.
-- Le pairage n'existe qu'à partir de 2013-2014 (intermediate_duel_pairs).
-- ══════════════════════════════════════════════════════════════════════════════

WITH

-- ── Matchs à traiter (mêmes règles que intermediate_duel_pairs) ─────────────────
{% if var('only_season', none) %}
new_matches AS (
    SELECT DISTINCT str_match_id
    FROM {{ ref('intermediate_duel_pairs') }}
    WHERE str_match_id IN ({{ season_match_ids(this) }})
    {%- if is_incremental() %}
      AND str_match_id NOT IN (SELECT DISTINCT str_match_id FROM {{ this }})
    {%- endif %}
),
{% elif is_incremental() %}
new_matches AS (
    SELECT DISTINCT str_match_id
    FROM {{ ref('intermediate_duel_pairs') }}
    WHERE str_match_id NOT IN (SELECT DISTINCT str_match_id FROM {{ this }})
),
{% else %}
new_matches AS (
    SELECT DISTINCT str_match_id
    FROM {{ ref('intermediate_duel_pairs') }}
),
{% endif %}

pairs AS (
    SELECT *
    FROM {{ ref('intermediate_duel_pairs') }}
    WHERE str_duel_type_id = '44'
      AND str_match_id IN (SELECT str_match_id FROM new_matches)
),

ev AS (
    SELECT
        str_match_id,
        CAST(str_team_id AS BIGINT)               AS team_id,
        CAST(str_player_id AS BIGINT)             AS player_id,
        CAST(str_event_id AS BIGINT)              AS event_id,
        int_row_num                               AS row_num,
        str_type_id,
        str_type_name,
        str_outcome_id,
        bool_is_shot,
        dec_x, dec_y, dec_end_x, dec_end_y,
        int_expanded_minute * 60 + int_second     AS ts
    FROM {{ ref('intermediate_whoscored_events_enriched') }}
    WHERE str_match_id IN (SELECT str_match_id FROM new_matches)
),

-- ── Un duel = une ligne, avec les lignes d'événement des deux côtés ────────────
duels AS (
    SELECT
        p.str_match_id, p.str_season, p.str_league_source,
        p.int_expanded_minute, p.int_second,
        p.int_team_id_a,   p.int_player_id_a, p.int_event_id_a, p.int_outcome_a,
        p.int_team_id_b,   p.int_player_id_b, p.int_event_id_b, p.int_outcome_b,
        ea.row_num AS row_a, ea.dec_x AS x_a, ea.dec_y AS y_a,
        eb.row_num AS row_b, eb.dec_x AS x_b, eb.dec_y AS y_b,
        LEAST(ea.row_num, eb.row_num)    AS row_min,
        GREATEST(ea.row_num, eb.row_num) AS row_max,
        LEAST(ea.ts, eb.ts)              AS duel_ts
    FROM pairs p
    JOIN ev ea ON ea.str_match_id = p.str_match_id AND ea.team_id = p.int_team_id_a
              AND ea.event_id = p.int_event_id_a AND ea.str_type_id = '44'
    JOIN ev eb ON eb.str_match_id = p.str_match_id AND eb.team_id = p.int_team_id_b
              AND eb.event_id = p.int_event_id_b AND eb.str_type_id = '44'
),

-- ── Rôle et zone de chaque côté (qualifiers de l'événement aérien) ─────────────
aerial_qual AS (
    SELECT
        str_match_id, int_row_num AS row_num,
        MAX(CASE WHEN str_qual_type_id = '286' THEN 1 ELSE 0 END) AS is_offensive,
        MAX(CASE WHEN str_qual_type_id = '285' THEN 1 ELSE 0 END) AS is_defensive,
        MAX(CASE WHEN str_qual_type_id = '56'  THEN str_qual_value END) AS zone
    FROM {{ ref('intermediate_whoscored_event_qualifiers') }}
    WHERE str_match_id IN (SELECT str_match_id FROM new_matches)
      AND str_type_id = '44'
      AND str_qual_type_id IN ('56', '285', '286')
    GROUP BY str_match_id, int_row_num
),

-- ── Source du ballon : dernier événement non aérien avant le duel ──────────────
src AS (
    SELECT
        d.str_match_id, d.row_min,
        s.row_num AS src_row, s.team_id AS src_team_id, s.player_id AS src_player_id,
        s.str_type_name AS src_type,
        s.dec_x AS src_x, s.dec_y AS src_y, s.dec_end_x AS src_end_x, s.dec_end_y AS src_end_y,
        s.ts AS src_ts
    FROM duels d
    ASOF LEFT JOIN (SELECT * FROM ev WHERE str_type_id <> '44') s
        ON d.str_match_id = s.str_match_id AND d.row_min > s.row_num
),

src_qual AS (
    SELECT
        s.str_match_id, s.row_min,
        MAX(CASE WHEN q.str_qual_type_id = '1'   THEN 1 ELSE 0 END) AS f_longball,
        MAX(CASE WHEN q.str_qual_type_id = '2'   THEN 1 ELSE 0 END) AS f_cross,
        MAX(CASE WHEN q.str_qual_type_id = '3'   THEN 1 ELSE 0 END) AS f_headpass,
        MAX(CASE WHEN q.str_qual_type_id IN ('5', '241') THEN 1 ELSE 0 END) AS f_freekick,
        MAX(CASE WHEN q.str_qual_type_id = '6'   THEN 1 ELSE 0 END) AS f_corner,
        MAX(CASE WHEN q.str_qual_type_id = '107' THEN 1 ELSE 0 END) AS f_throwin,
        MAX(CASE WHEN q.str_qual_type_id = '124' THEN 1 ELSE 0 END) AS f_goalkick,
        MAX(CASE WHEN q.str_qual_type_id = '155' THEN 1 ELSE 0 END) AS f_chipped
    FROM src s
    JOIN {{ ref('intermediate_whoscored_event_qualifiers') }} q
      ON q.str_match_id = s.str_match_id AND q.int_row_num = s.src_row
    WHERE q.str_match_id IN (SELECT str_match_id FROM new_matches)
      AND q.str_qual_type_id IN ('1', '2', '3', '5', '241', '6', '107', '124', '155')
    GROUP BY s.str_match_id, s.row_min
),

-- ── Conclusion : événement suivant (hors aérien) ───────────────────────────────
nxt AS (
    SELECT
        d.str_match_id, d.row_min,
        n.str_type_name AS next_type, n.team_id AS next_team_id, n.ts AS next_ts,
        n.row_num AS next_row, n.player_id AS next_player_id, n.str_outcome_id AS next_outcome,
        n.dec_x AS next_x, n.dec_y AS next_y, n.dec_end_x AS next_end_x, n.dec_end_y AS next_end_y
    FROM duels d
    ASOF LEFT JOIN (SELECT * FROM ev WHERE str_type_id <> '44') n
        ON d.str_match_id = n.str_match_id AND d.row_max < n.row_num
),

-- Le geste suivant est-il une tête ? (qualifier 3 = passe de la tête, 15 = tir de la tête)
next_qual AS (
    SELECT
        x.str_match_id, x.row_min,
        MAX(CASE WHEN q.str_qual_type_id IN ('3', '15') THEN 1 ELSE 0 END) AS f_header
    FROM nxt x
    JOIN {{ ref('intermediate_whoscored_event_qualifiers') }} q
      ON q.str_match_id = x.str_match_id AND q.int_row_num = x.next_row
    WHERE q.str_match_id IN (SELECT str_match_id FROM new_matches)
      AND q.str_qual_type_id IN ('3', '15')
    GROUP BY x.str_match_id, x.row_min
),

-- Tirs et buts dans les 10 s qui suivent, par équipe
shots10 AS (
    SELECT
        d.str_match_id, d.row_min,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_a)                                  AS shots_a,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_b)                                  AS shots_b,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_a AND s.str_type_name = 'Goal')     AS goals_a,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_b AND s.str_type_name = 'Goal')     AS goals_b
    FROM duels d
    JOIN (SELECT * FROM ev WHERE bool_is_shot) s
      ON s.str_match_id = d.str_match_id AND s.row_num > d.row_max AND s.ts <= d.duel_ts + 10
    GROUP BY d.str_match_id, d.row_min
),

-- ── Un duel par ligne, tous attributs réunis ───────────────────────────────────
full_duel AS (
    SELECT
        d.*,
        qa.is_offensive AS off_a, qa.is_defensive AS def_a, qa.zone AS zone_a,
        qb.is_offensive AS off_b, qb.is_defensive AS def_b, qb.zone AS zone_b,
        s.src_row, s.src_team_id, s.src_player_id, s.src_type, s.src_end_x, s.src_end_y,
        s.src_y, s.src_ts,
        COALESCE(sq.f_longball, 0) AS f_longball, COALESCE(sq.f_cross, 0)    AS f_cross,
        COALESCE(sq.f_headpass, 0) AS f_headpass, COALESCE(sq.f_freekick, 0) AS f_freekick,
        COALESCE(sq.f_corner, 0)   AS f_corner,   COALESCE(sq.f_throwin, 0)  AS f_throwin,
        COALESCE(sq.f_goalkick, 0) AS f_goalkick, COALESCE(sq.f_chipped, 0)  AS f_chipped,
        n.next_type, n.next_team_id, n.next_ts,
        n.next_row, n.next_player_id, n.next_outcome,
        n.next_x, n.next_y, n.next_end_x, n.next_end_y,
        COALESCE(nq.f_header, 0) AS f_next_header,
        COALESCE(sh.shots_a, 0) AS shots_a, COALESCE(sh.shots_b, 0) AS shots_b,
        COALESCE(sh.goals_a, 0) AS goals_a, COALESCE(sh.goals_b, 0) AS goals_b,
        -- Distance entre la fin de la passe et le duel, dans le repère de l'équipe émettrice
        SQRT(
            POWER(s.src_end_x - CASE WHEN s.src_team_id = d.int_team_id_a THEN d.x_a ELSE d.x_b END, 2) +
            POWER(s.src_end_y - CASE WHEN s.src_team_id = d.int_team_id_a THEN d.y_a ELSE d.y_b END, 2)
        ) AS src_end_dist,
        CASE
            WHEN sq.f_corner = 1                         THEN 'corner'
            WHEN sq.f_freekick = 1                       THEN 'coup_franc'
            WHEN sq.f_goalkick = 1                       THEN 'six_metres'
            WHEN sq.f_throwin = 1                        THEN 'touche'
            WHEN sq.f_cross = 1                          THEN 'centre'
            WHEN s.src_type = 'Clearance'                THEN 'degagement'
            WHEN sq.f_headpass = 1                       THEN 'passe_tete'
            WHEN sq.f_longball = 1                       THEN 'longue_balle'
            WHEN sq.f_chipped = 1                        THEN 'passe_lobee'
            WHEN s.src_type = 'Pass'                     THEN 'passe_autre'
            WHEN s.src_type IS NOT NULL                  THEN 'autre'
            ELSE 'sans_source'
        END AS ball_type
    FROM duels d
    LEFT JOIN aerial_qual qa ON qa.str_match_id = d.str_match_id AND qa.row_num = d.row_a
    LEFT JOIN aerial_qual qb ON qb.str_match_id = d.str_match_id AND qb.row_num = d.row_b
    LEFT JOIN src s          ON s.str_match_id  = d.str_match_id AND s.row_min  = d.row_min
    LEFT JOIN src_qual sq    ON sq.str_match_id = d.str_match_id AND sq.row_min = d.row_min
    LEFT JOIN nxt n          ON n.str_match_id  = d.str_match_id AND n.row_min  = d.row_min
    LEFT JOIN next_qual nq   ON nq.str_match_id = d.str_match_id AND nq.row_min = d.row_min
    LEFT JOIN shots10 sh     ON sh.str_match_id = d.str_match_id AND sh.row_min = d.row_min
),

-- ── Dépliage : une ligne par joueur (côté A puis côté B) ───────────────────────
sides AS (
    SELECT
        str_match_id, str_season, str_league_source, int_expanded_minute, int_second,
        int_team_id_a AS team_id, int_player_id_a AS player_id, int_event_id_a AS event_id,
        int_outcome_a AS outcome, x_a AS x, y_a AS y,
        int_team_id_b AS opp_team_id, int_player_id_b AS opp_player_id, int_event_id_b AS opp_event_id,
        off_a AS is_off, def_a AS is_def, zone_a AS zone,
        shots_a AS own_shots, shots_b AS opp_shots, goals_a AS own_goals, goals_b AS opp_goals,
        row_min, row_max, src_row, src_team_id, src_player_id, src_type, src_y, src_end_dist, src_ts,
        f_longball, f_cross, f_headpass, f_freekick, f_corner, f_throwin,
        f_goalkick, f_chipped, ball_type, next_type, next_team_id, next_ts, duel_ts,
        next_row, next_player_id, next_outcome, next_x, next_y, next_end_x, next_end_y, f_next_header
    FROM full_duel
    UNION ALL
    SELECT
        str_match_id, str_season, str_league_source, int_expanded_minute, int_second,
        int_team_id_b, int_player_id_b, int_event_id_b,
        int_outcome_b, x_b, y_b,
        int_team_id_a, int_player_id_a, int_event_id_a,
        off_b, def_b, zone_b,
        shots_b, shots_a, goals_b, goals_a,
        row_min, row_max, src_row, src_team_id, src_player_id, src_type, src_y, src_end_dist, src_ts,
        f_longball, f_cross, f_headpass, f_freekick, f_corner, f_throwin,
        f_goalkick, f_chipped, ball_type, next_type, next_team_id, next_ts, duel_ts,
        next_row, next_player_id, next_outcome, next_x, next_y, next_end_x, next_end_y, f_next_header
    FROM full_duel
),

mdl_out AS (
    SELECT
        str_match_id,
        str_season,
        str_league_source,
        CAST(team_id AS VARCHAR)                                   AS str_team_id,
        CAST(player_id AS VARCHAR)                                 AS str_player_id,
        CAST(event_id AS BIGINT)                                   AS int_event_id,
        CAST(opp_team_id AS VARCHAR)                               AS str_opp_team_id,
        CAST(opp_player_id AS VARCHAR)                             AS str_opp_player_id,
        CAST(opp_event_id AS BIGINT)                               AS int_opp_event_id,
        int_expanded_minute,
        int_second,
        CAST(x AS DOUBLE)                                          AS dec_x,
        CAST(y AS DOUBLE)                                          AS dec_y,
        (outcome = 1)                                              AS bool_is_won,
        CASE WHEN is_off = 1 THEN 'offensif'
             WHEN is_def = 1 THEN 'defensif'
             ELSE 'inconnu' END                                    AS str_role,
        zone                                                       AS str_zone,
        ball_type                                                  AS str_ball_type,
        CASE WHEN ball_type = 'centre'
             THEN CASE WHEN src_y <= 21 OR src_y >= 79 THEN 'exterieur' ELSE 'interieur' END
        END                                                        AS str_cross_lane,
        (f_longball = 1)                                           AS bool_ball_longball,
        (f_chipped = 1)                                            AS bool_ball_chipped,
        (f_cross = 1)                                              AS bool_ball_cross,
        (f_headpass = 1)                                           AS bool_ball_headpass,
        (f_freekick = 1)                                           AS bool_ball_freekick,
        (f_corner = 1)                                             AS bool_ball_corner,
        (f_throwin = 1)                                            AS bool_ball_throwin,
        (f_goalkick = 1)                                           AS bool_ball_goalkick,
        src_type                                                   AS str_source_event_type,
        CAST(src_team_id AS VARCHAR)                               AS str_source_team_id,
        CAST(src_player_id AS VARCHAR)                             AS str_source_player_id,
        (src_team_id = team_id)                                    AS bool_source_is_own_team,
        CAST(duel_ts - src_ts AS INTEGER)                          AS int_source_gap_s,
        CAST(src_end_dist AS DOUBLE)                               AS dec_source_end_dist,
        next_type                                                  AS str_next_event_type,
        CAST(next_ts - duel_ts AS INTEGER)                         AS int_next_event_gap_s,
        CAST(row_max AS INTEGER)                                   AS int_duel_last_row_num,
        CAST(next_row AS INTEGER)                                  AS int_next_event_row_num,
        CAST(next_player_id AS VARCHAR)                            AS str_next_event_player_id,
        (next_player_id = player_id)                               AS bool_next_event_is_own_player,
        (next_outcome = '1')                                       AS bool_next_event_success,
        (f_next_header = 1)                                        AS bool_next_event_is_header,
        CAST(next_x AS DOUBLE)                                     AS dec_next_event_x,
        CAST(next_y AS DOUBLE)                                     AS dec_next_event_y,
        CAST(next_end_x AS DOUBLE)                                 AS dec_next_event_end_x,
        CAST(next_end_y AS DOUBLE)                                 AS dec_next_event_end_y,
        CAST(next_end_x - next_x AS DOUBLE)                        AS dec_next_event_progress,
        CAST(own_shots AS INTEGER)                                 AS int_own_shots_10s,
        CAST(opp_shots AS INTEGER)                                 AS int_opp_shots_10s,
        (own_goals > 0)                                            AS bool_own_goal_10s,
        (opp_goals > 0)                                            AS bool_opp_goal_10s
    FROM sides
)

SELECT * FROM mdl_out
