{{
    config(
        materialized='incremental',
        unique_key=['str_match_id', 'str_team_id', 'int_event_id', 'int_opp_event_id'],
        on_schema_change='sync_all_columns',
        schema='intermediate',
        tags=['by_season'],
        alias='intermediate_ground_duel_players'
    )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- intermediate.intermediate_ground_duel_players
-- Grain : UNE LIGNE PAR DUEL AU SOL ET PAR JOUEUR (2 lignes par duel).
-- Clé   : (str_match_id, str_team_id, int_event_id, int_opp_event_id) — l'id d'événement
--         est propre à l'équipe, comme pour intermediate_aerial_duel_players.
--
-- Répond à : « quels duels au sol (dribbles, tacles, dépossessions, fautes) a disputés
-- ce joueur, contre qui, dans quelle zone, avec quelle issue, et quel danger en a suivi ? »
--
-- Trois familles, toutes lues dans intermediate_duel_pairs (pairage par qualifier 233) :
--   · 'dribble'       : un TakeOn (attaquant) contre un défenseur. Réussi → le défenseur
--                       porte un événement Challenge (battu). Raté → un événement Tackle
--                       (≈ 74 % des dribbles ratés sont appariés ; les autres n'ont pas de
--                       défenseur identifié et ne figurent PAS ici — le taux de réussite
--                       d'un dribbleur doit donc se calculer sur les événements bruts si
--                       l'on veut inclure ces ratés).
--   · 'dispossession' : un Dispossessed (porteur qui perd le ballon) contre un Tackle
--                       (≈ 100 % apparié ; le tacle est « gagnant » s'il récupère le ballon).
--   · 'faute'         : une faute subie (victime) contre la faute commise (auteur).
--
-- Rôle : 'attaquant' / 'defenseur' pour dribble et dispossession ; 'victime' / 'auteur'
-- pour la faute. bool_is_won = issue favorable au joueur dans CE duel :
--   dribble       attaquant = dribble réussi ; défenseur = dribble raté de l'attaquant
--   dispossession attaquant = FALSE (a perdu le ballon) ; défenseur = son tacle récupère
--                 le ballon (outcome du Tackle = 1)
--   faute         victime = TRUE ; auteur = FALSE
--
-- Zone : coordonnées dans le repère propre du joueur (dec_x, dec_y), et zone DU DUEL dans
-- le repère de l'attaquant / de la victime (str_zone_5x5 'zX_cY', convention de
-- intermediate_player_zone_season : c1-c2 = gauche, c3 = axe, c4-c5 = droite), identique
-- pour les deux lignes du duel — c'est le contexte qui définit l'attendu du profil.
--
-- Conséquence : tirs et buts de chaque équipe dans les 10 s qui suivent (comme l'aérien),
-- carton du fautif dans les 120 s (faute) et penalty (intermediate_fouls_drawn).
-- Le danger xT n'est pas calculé ici (grille xT estimée par une étape Python postérieure) :
-- int_duel_last_row_num / int_next_event_row_num permettent de joindre
-- intermediate_xt_actions et machine_learning.xt_grid dans un modèle aval.
--
-- Seuils posés par hypothèse, à valider : fenêtre de conclusion = 10 s ; fenêtre carton
-- = 120 s après la faute. Le pairage n'existe qu'à partir de 2013-2014.
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

-- 3 = TakeOn, 45 = Challenge, 50 = Dispossessed, 4 = Foul (7 = Tackle : 1 ligne, ignoré)
pairs AS (
    SELECT *
    FROM {{ ref('intermediate_duel_pairs') }}
    WHERE str_duel_type_id IN ('3', '45', '50', '4')
      AND str_match_id IN (SELECT str_match_id FROM new_matches)
),

ev AS (
    SELECT
        str_match_id,
        CAST(str_team_id AS BIGINT)               AS team_id,
        CAST(str_player_id AS BIGINT)             AS player_id,
        CAST(str_event_id AS BIGINT)              AS event_id,
        int_row_num                               AS row_num,
        str_type_name,
        str_outcome_id,
        bool_is_shot,
        str_card_type,
        int_expanded_minute * 60 + int_second     AS ts
    FROM {{ ref('intermediate_whoscored_events_enriched') }}
    WHERE str_match_id IN (SELECT str_match_id FROM new_matches)
),

-- ── Un duel = une ligne, avec les événements des deux côtés ────────────────────
duels AS (
    SELECT
        p.str_match_id, p.str_season, p.str_league_source,
        p.int_expanded_minute, p.int_second, p.str_duel_type,
        p.int_team_id_a, p.int_player_id_a, p.int_event_id_a, p.int_outcome_a,
        p.int_team_id_b, p.int_player_id_b, p.int_event_id_b, p.int_outcome_b,
        p.dec_x, p.dec_y,
        ea.row_num AS row_a, ea.str_type_name AS type_a,
        eb.row_num AS row_b, eb.str_type_name AS type_b,
        GREATEST(ea.row_num, eb.row_num)  AS row_max,
        LEAST(ea.ts, eb.ts)               AS duel_ts
    FROM pairs p
    JOIN ev ea ON ea.str_match_id = p.str_match_id AND ea.team_id = p.int_team_id_a
              AND ea.event_id = p.int_event_id_a
    JOIN ev eb ON eb.str_match_id = p.str_match_id AND eb.team_id = p.int_team_id_b
              AND eb.event_id = p.int_event_id_b
    -- Un match de 2023-2024 porte des événements en double dans
    -- intermediate_whoscored_events_enriched : sans ce filtre une paire sortirait en 4 copies
    -- et le décompte des tirs de la fenêtre de 10 s serait multiplié.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.str_match_id, p.int_team_id_a, p.int_event_id_a, p.int_event_id_b
        ORDER BY ea.row_num, eb.row_num
    ) = 1
),

-- ── Qui est l'attaquant / la victime (côté « référence ») ──────────────────────
-- TakeOn, Dispossessed, Foul : le côté A. Challenge : le défenseur battu est en A,
-- l'attaquant (TakeOn réussi) est en B. dec_x/dec_y de la paire sont dans le repère de A.
oriented AS (
    SELECT
        d.*,
        (d.str_duel_type = 'Challenge')                                        AS ref_is_b,
        CASE d.str_duel_type WHEN 'Challenge' THEN 100 - d.dec_x ELSE d.dec_x END AS x_ref,
        CASE d.str_duel_type WHEN 'Challenge' THEN 100 - d.dec_y ELSE d.dec_y END AS y_ref,
        CASE
            WHEN d.str_duel_type IN ('TakeOn', 'Challenge') THEN 'dribble'
            WHEN d.str_duel_type = 'Dispossessed'           THEN 'dispossession'
            ELSE 'faute'
        END                                                                    AS family,
        -- dribble réussi : TakeOn de issue 1 (ou paire Challenge, toujours un dribble réussi)
        CASE
            WHEN d.str_duel_type = 'Challenge' THEN TRUE
            WHEN d.str_duel_type = 'TakeOn'    THEN (d.int_outcome_a = 1)
        END                                                                    AS dribble_success
    FROM duels d
),

-- ── Tirs et buts dans les 10 s qui suivent, par équipe ─────────────────────────
shots10 AS (
    SELECT
        d.str_match_id, d.row_a, d.row_b,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_a)                                AS shots_a,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_b)                                AS shots_b,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_a AND s.str_type_name = 'Goal')   AS goals_a,
        COUNT(*) FILTER (WHERE s.team_id = d.int_team_id_b AND s.str_type_name = 'Goal')   AS goals_b
    FROM oriented d
    JOIN (SELECT * FROM ev WHERE bool_is_shot) s
      ON s.str_match_id = d.str_match_id AND s.row_num > d.row_max AND s.ts <= d.duel_ts + 10
    GROUP BY d.str_match_id, d.row_a, d.row_b
),

-- ── Événement suivant le duel (toute nature) ───────────────────────────────────
nxt AS (
    SELECT
        d.str_match_id, d.row_a, d.row_b,
        n.str_type_name AS next_type, n.team_id AS next_team_id, n.player_id AS next_player_id,
        n.row_num AS next_row, n.ts AS next_ts
    FROM oriented d
    ASOF LEFT JOIN ev n
        ON d.str_match_id = n.str_match_id AND d.row_max < n.row_num
),

-- ── Carton du fautif dans les 120 s qui suivent la faute ───────────────────────
cards AS (
    SELECT
        d.str_match_id, d.row_a, d.row_b,
        MIN(c.str_card_type) FILTER (WHERE c.str_card_type = 'Yellow')                         AS yellow,
        MIN(c.str_card_type) FILTER (WHERE c.str_card_type IN ('Red', 'SecondYellow'))         AS red
    FROM oriented d
    JOIN ev c
      ON c.str_match_id = d.str_match_id AND c.str_type_name = 'Card'
     AND c.player_id = d.int_player_id_b AND c.team_id = d.int_team_id_b
     AND c.row_num > d.row_b AND c.ts <= d.duel_ts + 120
    WHERE d.family = 'faute'
    GROUP BY d.str_match_id, d.row_a, d.row_b
),

penalty AS (
    SELECT DISTINCT
        str_match_id,
        CAST(str_event_id AS BIGINT)        AS event_id,
        CAST(str_drawing_team_id AS BIGINT) AS team_id,
        bool_leads_to_penalty
    FROM {{ ref('intermediate_fouls_drawn') }}
    WHERE str_match_id IN (SELECT str_match_id FROM new_matches)
),

full_duel AS (
    SELECT
        o.*,
        COALESCE(sh.shots_a, 0) AS shots_a, COALESCE(sh.shots_b, 0) AS shots_b,
        COALESCE(sh.goals_a, 0) AS goals_a, COALESCE(sh.goals_b, 0) AS goals_b,
        n.next_type, n.next_team_id, n.next_player_id, n.next_row, n.next_ts,
        (c.yellow IS NOT NULL) AS yellow_after, (c.red IS NOT NULL) AS red_after,
        COALESCE(pe.bool_leads_to_penalty, FALSE) AS leads_to_penalty
    FROM oriented o
    LEFT JOIN shots10 sh ON sh.str_match_id = o.str_match_id AND sh.row_a = o.row_a AND sh.row_b = o.row_b
    LEFT JOIN nxt n      ON n.str_match_id  = o.str_match_id AND n.row_a  = o.row_a AND n.row_b  = o.row_b
    LEFT JOIN cards c    ON c.str_match_id  = o.str_match_id AND c.row_a  = o.row_a AND c.row_b  = o.row_b
    LEFT JOIN penalty pe ON pe.str_match_id = o.str_match_id AND pe.team_id = o.int_team_id_a
                        AND pe.event_id = o.int_event_id_a AND o.family = 'faute'
),

-- ── Dépliage : une ligne par joueur (côté A puis côté B) ───────────────────────
sides AS (
    SELECT
        str_match_id, str_season, str_league_source, int_expanded_minute, int_second,
        family, dribble_success, x_ref, y_ref,
        int_team_id_a AS team_id, int_player_id_a AS player_id, int_event_id_a AS event_id,
        int_outcome_a AS outcome, type_a AS action_type, dec_x AS x, dec_y AS y,
        int_team_id_b AS opp_team_id, int_player_id_b AS opp_player_id, int_event_id_b AS opp_event_id,
        type_b AS opp_action_type, (NOT ref_is_b) AS is_ref_side, int_outcome_b AS opp_outcome,
        shots_a AS own_shots, shots_b AS opp_shots, goals_a AS own_goals, goals_b AS opp_goals,
        row_max, next_type, next_team_id, next_player_id, next_row, next_ts, duel_ts,
        yellow_after, red_after, leads_to_penalty
    FROM full_duel
    UNION ALL
    SELECT
        str_match_id, str_season, str_league_source, int_expanded_minute, int_second,
        family, dribble_success, x_ref, y_ref,
        int_team_id_b, int_player_id_b, int_event_id_b,
        int_outcome_b, type_b, 100 - dec_x, 100 - dec_y,
        int_team_id_a, int_player_id_a, int_event_id_a,
        type_a, ref_is_b, int_outcome_a,
        shots_b, shots_a, goals_b, goals_a,
        row_max, next_type, next_team_id, next_player_id, next_row, next_ts, duel_ts,
        yellow_after, red_after, leads_to_penalty
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
        family                                                     AS str_duel_family,
        CASE
            WHEN family = 'faute'
                THEN CASE WHEN is_ref_side THEN 'victime' ELSE 'auteur' END
            ELSE CASE WHEN is_ref_side THEN 'attaquant' ELSE 'defenseur' END
        END                                                        AS str_role,
        action_type                                                AS str_action_type,
        opp_action_type                                            AS str_opp_action_type,
        CASE
            WHEN family = 'dribble'
                THEN CASE WHEN is_ref_side THEN dribble_success ELSE NOT dribble_success END
            WHEN family = 'dispossession'
                THEN CASE WHEN is_ref_side THEN FALSE ELSE (outcome = 1) END
            ELSE is_ref_side
        END                                                        AS bool_is_won,
        -- dribble : le défenseur a-t-il récupéré le ballon (Tackle réussi) sur un dribble raté ?
        CASE WHEN family = 'dribble' AND NOT is_ref_side AND action_type = 'Tackle'
             THEN (outcome = 1) END                                AS bool_ball_recovered,
        CAST(x AS DOUBLE)                                          AS dec_x,
        CAST(y AS DOUBLE)                                          AS dec_y,
        CAST(x_ref AS DOUBLE)                                      AS dec_x_duel,
        CAST(y_ref AS DOUBLE)                                      AS dec_y_duel,
        'z' || LEAST(FLOOR(x_ref / 20) + 1, 5)::INT
            || '_c' || LEAST(FLOOR(y_ref / 20) + 1, 5)::INT        AS str_zone_5x5,
        CASE WHEN x_ref < 100.0 / 3 THEN 'tiers_def'
             WHEN x_ref < 200.0 / 3 THEN 'milieu'
             ELSE 'tiers_off' END                                  AS str_zone_depth,
        CASE WHEN y_ref < 40 THEN 'gauche'
             WHEN y_ref < 60 THEN 'axe'
             ELSE 'droit' END                                      AS str_zone_corridor,
        CAST(own_shots AS INTEGER)                                 AS int_own_shots_10s,
        CAST(opp_shots AS INTEGER)                                 AS int_opp_shots_10s,
        (own_goals > 0)                                            AS bool_own_goal_10s,
        (opp_goals > 0)                                            AS bool_opp_goal_10s,
        next_type                                                  AS str_next_event_type,
        CAST(next_team_id AS VARCHAR)                              AS str_next_event_team_id,
        CAST(next_player_id AS VARCHAR)                            AS str_next_event_player_id,
        CAST(next_ts - duel_ts AS INTEGER)                         AS int_next_event_gap_s,
        CAST(row_max AS INTEGER)                                   AS int_duel_last_row_num,
        CAST(next_row AS INTEGER)                                  AS int_next_event_row_num,
        -- Conséquences de la faute (NULL hors famille 'faute' ; portées par les deux lignes)
        CASE WHEN family = 'faute' THEN yellow_after END           AS bool_yellow_card_120s,
        CASE WHEN family = 'faute' THEN red_after END              AS bool_red_card_120s,
        CASE WHEN family = 'faute' THEN leads_to_penalty END       AS bool_leads_to_penalty
    FROM sides
)

SELECT * FROM mdl_out
