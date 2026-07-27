{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'row_num'],
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

WITH

-- ── FILTRE INCRÉMENTAL ────────────────────────────────────────────────────────
-- Même patron que int_event_enriched : on ne traite que les matchs dont le
-- scraped_at dépasse le dernier déjà présent dans la table cible. Penalty = grain
-- append-only (les penaltys passés ne changent pas) → incrémental correct.
{% if is_incremental() %}
max_scraped AS (
    SELECT MAX(scraped_at) AS last_scraped FROM {{ this }}
),
new_matches AS (
    SELECT DISTINCT match_id
    FROM {{ ref('int_whoscored_events') }}
    CROSS JOIN max_scraped
    WHERE scraped_at > last_scraped
),
{% else %}
new_matches AS (
    SELECT DISTINCT match_id FROM {{ ref('int_whoscored_events') }}
),
{% endif %}

-- ── Identifiants des events portant le qualifier « Penalty » (qual 9) ──────────
pen_ids AS (
    SELECT DISTINCT match_id, row_num
    FROM {{ ref('events_qual') }}
    WHERE qual_type_id = 9
      AND match_id IN (SELECT match_id FROM new_matches)
),

-- ── OppositeRelatedEvent (qual 233) → event_id de l'event miroir adverse ───────
-- La valeur pointe vers l'event_id de l'event symétrique de l'autre équipe.
-- Sert à lier de façon déterministe : PenaltyFaced→frappe et conceder→drawer.
q233 AS (
    SELECT match_id, row_num, TRY_CAST(qual_value AS INTEGER) AS opposite_event_id
    FROM {{ ref('events_qual') }}
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
    FROM {{ ref('int_event_enriched') }} e
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
    FROM {{ ref('int_whoscored_events') }} e
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
        FROM {{ ref('int_whoscored_events') }} e
        JOIN pen_ids p ON p.match_id = e.match_id AND p.row_num = e.row_num
        WHERE e.type_id = 4 AND e.outcome_id = 1
    ) o
    LEFT JOIN (
        SELECT e.match_id, e.team_id AS conceded_team, e.player_id AS player_conceded,
               q.opposite_event_id
        FROM {{ ref('int_whoscored_events') }} e
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
