"""
Pipeline 03b — Feature Engineering (WhoScored Events → Gold)
=============================================================
Transforme les événements bruts silver.stg_whoscored_events en features
spatiales et comportementales pour le modèle de prédiction (Stacking).

ARCHITECTURE 3 PASSES
──────────────────────
  Passe 1 — Explosion DuckDB  : UNNEST qualifiers_json → table temp en mémoire
  Passe 2 — Agrégations       : GROUP BY (ws_match_id, team_id) via SQL pur
  Passe 3 — Pivot home/away + jointure sur gold.features_training

FEATURES (v1 + v2 — Anti-doublons vérifié vs Features_names.txt)
─────────────────────────────────────────────────────────────────
  Bloc A — Pépites v1 (signal upset)
  F1  — ws_field_tilt_actions  : % toutes actions en zone offensive (x > 66)
  F3  — ws_high_turnover_rate  : pertes de balle en zone haute (passes ratées x > 66)
  F5  — ws_deep_completion_rt  : passes réussies avec end_x > 83 / total passes
  F6  — ws_momentum_delta      : ratio (actions +10min) / (actions -10min) post-but encaissé
                                  > 1 → équipe réagit positivement (resilience)
                                  < 1 → équipe s'effondre après avoir concédé
  F7  — ws_counter_shot_rate   : tirs en contre (qualifier 26) / total tirs
  F8  — ws_set_piece_pressure  : (corners + FK offensifs) / total événements offensifs

  Bloc B — Attack Sides (axe y, x > 33)
  Bloc C — Action Zones (axe x)
  Bloc D — Shot Zones
  Bloc E — Attempt Types
  Bloc F — Pass Types

  Bloc G — Defensive Exposure (v2 — NOUVEAU)
  G1  — ws_def_exposed_left_pct   : % actions adverses reçues sur notre couloir gauche
                                     défensif (y < 33.3) → vulnérabilité côté gauche
  G2  — ws_def_exposed_center_pct : % actions adverses reçues dans l'axe central
                                     (33.3 ≤ y ≤ 66.6) → vulnérabilité axiale
  G3  — ws_def_exposed_right_pct  : % actions adverses reçues sur notre couloir droit
                                     (y > 66.6) → vulnérabilité côté droit
  Note : coordonnées depuis la perspective de l'équipe attaquante adverse.
         Un couloir "exposé" = l'adversaire y génère proportionnellement plus d'actions.

  Feature de qualité de données (v2 — NOUVEAU)
  Q1  — has_ws_events             : 1 si les ws_* sont renseignées, 0 sinon
                                     Permet à LGBM d'apprendre que les NULL sont
                                     systématiques pour Bundesliga/La Liga (pas du bruit).

DIFFÉRENTIELS dans gold.features_final (v1 + v2)
─────────────────────────────────────────────────
  v1 : ws_turnover_zone_diff, ws_deep_pass_diff, ws_momentum_diff,
       ws_counter_threat_diff, ws_attack_width_diff, ws_zone_att_diff,
       ws_shot_zone_diff, ws_conversion_diff, ws_cross_diff, ws_long_ball_diff
  v2 (NOUVEAU) :
       ws_left_matchup_adv    = team.ws_attack_left_pct  - opp.ws_def_exposed_right_pct
       ws_right_matchup_adv   = team.ws_attack_right_pct - opp.ws_def_exposed_left_pct
       ws_center_matchup_adv  = team.ws_attack_center_pct - opp.ws_def_exposed_center_pct
       > 0 = avantage structurel sur ce couloir (style attaque vs vulnérabilité défensive)

ANTI-LEAKAGE
────────────
  ⚠️  TOUTES les features sont des agrégats du match PASSÉ.
  Elles sont jointes via LAG(1) sur gold.features_training pour garantir
  que le match courant ne contamine pas la prédiction.
  Le script enrichit gold.features_training avec des colonnes ADD COLUMN IF NOT EXISTS.

Usage :
    python pipelines/03b_features_match_details.py
    python pipelines/03b_features_match_details.py --reset-cols
    python pipelines/03b_features_match_details.py --coverage-only
"""

import argparse
from pathlib import Path
import tempfile

import duckdb
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
# Fix Windows/OneDrive: s'assurer que le CWD est le dossier du projet
import os
os.chdir(Path(__file__).resolve().parent.parent)

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = Path(CFG["paths"]["db"])

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_match_details.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

WINDOW = CFG.get("features", {}).get("form_window", 5)

# ── Team Mapping (config.yaml → DuckDB lookup table) ─────────────────────────
# Transforme le dict {raw_name: canonical_name} en liste de tuples
# pour injection dans une table temporaire DuckDB utilisée lors de la jointure.
# Cela résout les divergences de nommage entre WhoScored (home_team_name)
# et gold.features_training (team normalisé par 02_process.py).
_RAW_MAPPING: dict = CFG.get("team_mapping", {})

# Rows : (raw_name, canonical_name)
TEAM_MAPPING_ROWS: list = [
    (str(raw), str(canonical))
    for raw, canonical in _RAW_MAPPING.items()
    if raw and canonical
]

# ── Mapping WhoScored type_id (référence officielle) ─────────────────────────
# Source : documentation communautaire WhoScored + reverse engineering
WS_TYPE = {
    "PASS":          1,
    "OFFSIDE_PASS":  2,
    "TAKE_ON":       3,
    "FOUL":          4,
    "OUT":           5,
    "CORNER_AWARDED":6,
    "TACKLE":        7,
    "INTERCEPTION":  8,
    "TURNOVER":      9,  # Perte de balle non contestée
    "SAVE":         10,
    "CLAIM":        11,
    "CLEARANCE":    12,
    "MISSED_SHOT":  13,
    "WOOD_WORK":    14,
    "KEEPER_PICKUP":15,
    "CHANCE_MISSED":16,
    "SAVED_SHOT":   16,  # Alias selon version WS
    "GOAL":         16,  # Outcome_id = 1
    "CARD":         17,
    "PLAYER_OFF":   18,
    "PLAYER_ON":    19,
    "KEEPER_SWEEPER":20,
    "CHANCE_SAVED": 41,
    "BLOCKED_SHOT": 74,  # Qualifier sur type_id 13/16
}

# Qualifiers utiles (qualifier.type.value dans le JSON)
WS_QUAL = {
    "THROUGH_BALL":     155,  # Passe dans la profondeur
    "HEAD":              72,  # Action de la tête
    "CORNER":            6,   # Tir/action depuis corner
    "FREE_KICK":         5,   # Tir/action depuis coup franc
    "PENALTY":          9,    # Tir au but = pénalty
    "COUNTER_ATTACK":   26,   # Action en contre-attaque
    "BIG_CHANCE":       233,  # Occasion franche
    "BLOCKED":          82,   # Tir bloqué
    "OWN_GOAL":         28,   # But contre son camp
    "ASSIST":           29,   # Passe décisive
    "INTENTIONAL_ASSIST": 210,# Passe décisive volontaire
    "CHIPPED":          155,  # Passe lobée
    "DEEP_BALL":        2,    # Longue balle
}


# ── Nouvelles colonnes ajoutées à gold.features_training ─────────────────────
# Organisées en 3 blocs : Pépites (v1) | Positional Classics | Style Classics
NEW_COLS = [
    # ── Bloc A : Pépites (v1 — inchangées) ───────────────────────────────────
    ("ws_field_tilt_actions",  "DOUBLE"),  # % touches en zone off. (x > 66)
    ("ws_high_turnover_rate",  "DOUBLE"),  # pertes balle zone haute
    ("ws_deep_completion_rt",  "DOUBLE"),  # passes réussies end_x > 83
    ("ws_momentum_delta",      "DOUBLE"),  # résilience post-but encaissé
    ("ws_counter_shot_rate",   "DOUBLE"),  # tirs en contre / total tirs
    ("ws_set_piece_pressure",  "DOUBLE"),  # phases arrêtées off. / actions off.

    # ── Bloc B : Positional Report — Attack Sides (axe y) ────────────────────
    ("ws_attack_left_pct",     "DOUBLE"),  # % actions offensives côté gauche  (y < 33.3)
    ("ws_attack_center_pct",   "DOUBLE"),  # % actions offensives axe central  (33.3 ≤ y ≤ 66.6)
    ("ws_attack_right_pct",    "DOUBLE"),  # % actions offensives côté droit   (y > 66.6)

    # ── Bloc C : Positional Report — Action Zones (axe x) ────────────────────
    ("ws_zone_def_pct",        "DOUBLE"),  # % touches en bloc défensif        (x < 33.3)
    ("ws_zone_mid_pct",        "DOUBLE"),  # % touches en milieu de terrain    (33.3 ≤ x ≤ 66.6)
    ("ws_zone_att_pct",        "DOUBLE"),  # % touches en bloc offensif        (x > 66.6)

    # ── Bloc D : Shot Zones (combinaison x/y) ────────────────────────────────
    ("ws_shot_six_yard_pct",   "DOUBLE"),  # % tirs depuis la cage 6m          (x > 94, y ∈ [36,64])
    ("ws_shot_penalty_pct",    "DOUBLE"),  # % tirs depuis la surface de répar. (x > 83, y ∈ [21,79])
    ("ws_shot_oob_pct",        "DOUBLE"),  # % tirs hors surface               (le reste)

    # ── Bloc E : Attempt Types (situations) ──────────────────────────────────
    ("ws_shot_open_play_pct",  "DOUBLE"),  # % tirs en jeu ouvert
    ("ws_shot_set_piece_pct",  "DOUBLE"),  # % tirs sur phase arrêtée (FK/corner)
    ("ws_shot_penalty_att_pct","DOUBLE"),  # % tirs = penaltys
    ("ws_conversion_rate",     "DOUBLE"),  # buts / total tirs (efficacité brute)

    # ── Bloc F : Pass Types (style) ──────────────────────────────────────────
    ("ws_cross_rate",          "DOUBLE"),  # centres / total passes
    ("ws_through_ball_rate",   "DOUBLE"),  # through balls / total passes
    ("ws_long_ball_rate",      "DOUBLE"),  # longues balles / total passes
    ("ws_short_pass_rate",     "DOUBLE"),  # passes courtes / total passes (résiduel)

    # ── Bloc G : Defensive Exposure (v2 — NOUVEAU) ───────────────────────────
    # Mesure où l'équipe ADVERSE génère ses actions offensives contre nous.
    # Calculé côté adversaire, puis pivoted → donne notre vulnérabilité défensive.
    # Coordonnées depuis la perspective de l'équipe attaquante (adversaire).
    ("ws_def_exposed_left_pct",   "DOUBLE"),  # % actions adverses sur notre gauche  (y < 33.3)
    ("ws_def_exposed_center_pct", "DOUBLE"),  # % actions adverses dans notre axe    (33.3 ≤ y ≤ 66.6)
    ("ws_def_exposed_right_pct",  "DOUBLE"),  # % actions adverses sur notre droite  (y > 66.6)

    # ── Qualité de données (v2 — NOUVEAU) ────────────────────────────────────
    # 1 si les features ws_* sont renseignées, 0 si toutes NULL.
    # Permet à LGBM de ne pas confondre "données manquantes" et "comportement neutre".
    # Doit être calculé APRÈS le remplissage de ws_field_tilt_actions.
    ("has_ws_events",             "INTEGER"),  # 0 / 1 — couverture WhoScored events
]

# Colonnes différentielles ajoutées à gold.features_final
DIFF_COLS = [
    # Pépites diffs (v1)
    # ("ws_field_tilt_diff",     "DOUBLE"),
    # ("ws_shot_quality_diff",   "DOUBLE"),
    ("ws_turnover_zone_diff",  "DOUBLE"),
    ("ws_deep_pass_diff",      "DOUBLE"),
    ("ws_momentum_diff",       "DOUBLE"),
    ("ws_counter_threat_diff", "DOUBLE"),
    # Classics diffs (v2)
    ("ws_attack_width_diff",   "DOUBLE"),  # center_pct team - center_pct opp (jeu axial vs large)
    ("ws_zone_att_diff",       "DOUBLE"),  # zone_att_pct diff (pression territoriale)
    ("ws_shot_zone_diff",      "DOUBLE"),  # shot_penalty_pct diff (qualité positionnelle tirs)
    ("ws_conversion_diff",     "DOUBLE"),  # conversion_rate diff (efficacité clinique)
    ("ws_cross_diff",          "DOUBLE"),  # cross_rate diff (style direct vs combinaison)
    ("ws_long_ball_diff",      "DOUBLE"),  # long_ball_rate diff (jeu long vs court)
    # Matchup advantages (v2 — NOUVEAU)
    # Croisement style attaque équipe vs vulnérabilité défensive adversaire.
    # > 0 = avantage structurel sur ce couloir (signal upset clé).
    ("ws_left_matchup_adv",    "DOUBLE"),  # team.attack_left  - opp.def_exposed_right
    ("ws_right_matchup_adv",   "DOUBLE"),  # team.attack_right - opp.def_exposed_left
    ("ws_center_matchup_adv",  "DOUBLE"),  # team.attack_center - opp.def_exposed_center
]


# ─────────────────────────────────────────────────────────────────────────────
# PASSE 1 — Explosion des qualifiers_json
# DuckDB UNNEST est 8-10x plus rapide que pandas.explode() sur >500k events
# ─────────────────────────────────────────────────────────────────────────────
SQL_EXPLODE_QUALIFIERS = """
CREATE OR REPLACE TEMP TABLE tmp_events_qual AS
SELECT
    e.ws_match_id,
    e.team_id,
    e.player_id,
    e.minute,
    e.second,
    e.expanded_minute,
    e.period,
    e.x,
    e.y,
    e.end_x,
    e.end_y,
    e.type_id,
    e.type_name,
    e.outcome_id,
    e.is_touch,
    e.is_shot,
    e.row_num,
    -- Extraction des qualifiers individuels depuis le tableau JSON
    TRY_CAST(
        json_extract_string(q.qual, '$.type.value') AS INTEGER
    ) AS qual_type_id,
    json_extract_string(q.qual, '$.type.displayName') AS qual_type_name,
    json_extract_string(q.qual, '$.value.value')       AS qual_value
FROM silver.stg_whoscored_events e,
     LATERAL (
         SELECT unnest(
             json_extract(e.qualifiers_json, '$[*]')::JSON[]
         ) AS qual
     ) q
WHERE e.qualifiers_json IS NOT NULL
  AND e.qualifiers_json != '[]'
;
"""

# Table d'events "plats" (sans explosion qualifier) pour les features de volume
SQL_EVENTS_FLAT = """
CREATE OR REPLACE TEMP TABLE tmp_events_flat AS
SELECT
    ws_match_id,
    team_id,
    minute,
    second,
    expanded_minute,
    period,
    x,
    y,
    end_x,
    end_y,
    type_id,
    type_name,
    outcome_id,
    is_touch,
    is_shot,
    row_num
FROM silver.stg_whoscored_events
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# PASSE 2 — Agrégations par (ws_match_id, team_id)
# ─────────────────────────────────────────────────────────────────────────────

SQL_TEAM_FEATURES = f"""
CREATE OR REPLACE TEMP TABLE tmp_team_features AS
WITH

-- ══════════════════════════════════════════════════════════════════════════
-- 2.1 Compteurs de base pour les dénominateurs
-- ══════════════════════════════════════════════════════════════════════════
base_counts AS (
    SELECT
        ws_match_id,
        team_id,
        COUNT(*)                                                AS total_events,
        COUNT(*) FILTER (WHERE is_touch = TRUE)                AS total_touches,
        COUNT(*) FILTER (WHERE is_shot = TRUE)                 AS total_shots,
        COUNT(*) FILTER (WHERE type_id = 1)                    AS total_passes,
        COUNT(*) FILTER (WHERE type_id = 1 AND outcome_id = 1) AS passes_successful,

        -- ── Pépites v1 ────────────────────────────────────────────────────
        -- Zone offensive : x > 66 (dernier tiers du terrain)
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 66)     AS touches_offensive_zone,

        -- Passes réussies en zone de finition (end_x > 83)
        COUNT(*) FILTER (
            WHERE type_id = 1 AND outcome_id = 1 AND end_x > 83
        )                                                       AS deep_completions,

        -- Pertes de balle en zone haute (passes ratées x > 66)
        COUNT(*) FILTER (
            WHERE type_id = 1 AND outcome_id = 0 AND x > 66
        )                                                       AS turnovers_high_zone,

        -- Actions offensives totales (passes + tirs + dribbles en zone off.)
        COUNT(*) FILTER (
            WHERE x > 66 AND type_id IN (1, 3, 13, 15, 16)
        )                                                       AS offensive_actions,

        -- ── Bloc B : Attack Sides (axe y — sur les touches en zone offensive) ─
        -- On restreint aux touches en x > 33 pour ne mesurer que le jeu
        -- en territoire adverse ou neutre, pas les relances défensives.
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33 AND y < 33.3)    AS att_touches_left,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33
                              AND y >= 33.3 AND y <= 66.6)                  AS att_touches_center,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33 AND y > 66.6)    AS att_touches_right,
        -- Dénominateur commun pour les 3 colonnes ci-dessus
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x > 33)                 AS att_touches_total,

        -- ── Bloc C : Action Zones (axe x — sur toutes les touches) ───────────
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x <  33.3)              AS zone_def_touches,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x >= 33.3 AND x <= 66.6)AS zone_mid_touches,
        COUNT(*) FILTER (WHERE is_touch = TRUE AND x >  66.6)              AS zone_att_touches,

        -- ── Bloc D : Shot Zones (combinaison x/y) ────────────────────────────
        -- 6-yard box  : x > 94 ET y ∈ [36, 64]  (surface de but)
        COUNT(*) FILTER (
            WHERE is_shot = TRUE AND x > 94 AND y BETWEEN 36 AND 64
        )                                                       AS shots_six_yard,

        -- Penalty area : x > 83 ET y ∈ [21, 79]  (surface de réparation)
        -- On exclut les tirs déjà comptabilisés en 6-yard
        COUNT(*) FILTER (
            WHERE is_shot = TRUE
              AND x > 83 AND y BETWEEN 21 AND 79
              AND NOT (x > 94 AND y BETWEEN 36 AND 64)
        )                                                       AS shots_penalty_area,

        -- Out of box : tout le reste
        COUNT(*) FILTER (
            WHERE is_shot = TRUE
              AND NOT (x > 83 AND y BETWEEN 21 AND 79)
        )                                                       AS shots_out_of_box,

        -- ── Buts (pour conversion_rate) ──────────────────────────────────────
        -- type_id = 16 avec outcome_id = 1 et is_shot = TRUE = but
        COUNT(*) FILTER (
            WHERE is_shot = TRUE AND type_id = 16 AND outcome_id = 1
        )                                                       AS goals_scored

    FROM tmp_events_flat
    GROUP BY ws_match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════
-- 2.1b Defensive Exposure — actions offensives de l'ADVERSAIRE contre nous
-- ──────────────────────────────────────────────────────────────────────────
-- Principe : pour chaque match × équipe, on regarde où l'ÉQUIPE ADVERSE
-- a généré ses actions offensives (x > 33, depuis la perspective de l'adversaire).
-- Ces coordonnées sont dans le référentiel de l'équipe qui attaque,
-- donc y < 33.3 = couloir gauche de l'attaquant = couloir DROIT du défenseur.
-- On stocke le résultat sur l'équipe qui DÉFEND (team_id adversaire).
--
-- ⚠️  Anti-leakage : comme tous les autres agrégats, ces valeurs sont issues
-- du match N-1 via LAG(1). Elles ne contiennent pas d'info sur le match courant.
-- ══════════════════════════════════════════════════════════════════════════
defensive_exposure AS (
    SELECT
        ws_match_id,
        -- L'équipe qui DÉFEND = l'équipe qui ne fait pas ces actions offensives
        other_team_id                                           AS team_id,

        COUNT(*) FILTER (WHERE x > 33 AND y < 33.3)            AS opp_att_left,
        COUNT(*) FILTER (WHERE x > 33 AND y >= 33.3
                              AND y <= 66.6)                    AS opp_att_center,
        COUNT(*) FILTER (WHERE x > 33 AND y > 66.6)            AS opp_att_right,
        COUNT(*) FILTER (WHERE x > 33)                          AS opp_att_total
    FROM (
        -- Sous-requête : pour chaque action offensive d'une équipe,
        -- on identifie l'équipe adverse (celle qui défend).
        SELECT
            f.ws_match_id,
            f.x,
            f.y,
            f.is_touch,
            -- Trouver l'autre team_id dans ce match
            other.team_id                                       AS other_team_id
        FROM tmp_events_flat f
        JOIN (
            SELECT ws_match_id, team_id
            FROM tmp_events_flat
            GROUP BY ws_match_id, team_id
        ) other
            ON  f.ws_match_id = other.ws_match_id
            AND f.team_id    != other.team_id
        WHERE f.is_touch = TRUE
          AND f.x > 33   -- Uniquement les actions en territoire adverse ou neutre
    ) att_actions
    GROUP BY ws_match_id, other_team_id
),

-- ══════════════════════════════════════════════════════════════════════════
-- 2.2 Features issues des qualifiers
-- ──────────────────────────────────────────────────────────────────────────
-- Attempt Types (qualifier IDs WhoScored) :
--   22 = Open Play shot         23 = Set Piece shot
--   9  = Penalty shot           26 = Counter Attack shot
--
-- Pass Types (qualifier IDs WhoScored) :
--   2  = Cross / Centre         155 = Through Ball / Passe en profondeur
--   1  = Long Ball              (reste) = Short Pass
--
-- Set Piece Pressure (existant v1) :
--   5  = Free Kick              6  = Corner Awarded
-- ══════════════════════════════════════════════════════════════════════════
qualifier_features AS (
    SELECT
        ws_match_id,
        team_id,

        -- ── Pépites v1 ────────────────────────────────────────────────────
        -- Tirs en contre-attaque (qual 26)
        COUNT(*) FILTER (
            WHERE is_shot = TRUE AND qual_type_id = 26
        )                                                       AS shots_counter_attack,

        -- Actions set piece offensives : corners (6) + free kicks (5)
        COUNT(DISTINCT row_num) FILTER (
            WHERE qual_type_id IN (5, 6) AND x > 50
        )                                                       AS set_pieces_offensive,

        -- Through balls réussies (qual 155)
        COUNT(*) FILTER (
            WHERE qual_type_id = 155 AND outcome_id = 1
        )                                                       AS through_balls_successful,

        -- ── Bloc E : Attempt Types ────────────────────────────────────────
        -- Tirs en jeu ouvert (qual 22)
        COUNT(DISTINCT row_num) FILTER (
            WHERE is_shot = TRUE AND qual_type_id = 22
        )                                                       AS shots_open_play,

        -- Tirs sur phase arrêtée (qual 23 = set piece shot)
        COUNT(DISTINCT row_num) FILTER (
            WHERE is_shot = TRUE AND qual_type_id = 23
        )                                                       AS shots_set_piece,

        -- Tirs = penaltys (qual 9)
        COUNT(DISTINCT row_num) FILTER (
            WHERE is_shot = TRUE AND qual_type_id = 9
        )                                                       AS shots_penalty,

        -- ── Bloc F : Pass Types ───────────────────────────────────────────
        -- Centres / Crosses (qual 2)
        COUNT(DISTINCT row_num) FILTER (
            WHERE type_id = 1 AND qual_type_id = 2
        )                                                       AS passes_cross,

        -- Through Balls (qual 155)
        COUNT(DISTINCT row_num) FILTER (
            WHERE type_id = 1 AND qual_type_id = 155
        )                                                       AS passes_through_ball,

        -- Long Balls (qual 1)
        COUNT(DISTINCT row_num) FILTER (
            WHERE type_id = 1 AND qual_type_id = 1
        )                                                       AS passes_long_ball

    FROM tmp_events_qual
    GROUP BY ws_match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════
-- 2.3 Momentum post-but encaissé
-- ──────────────────────────────────────────────────────────────────────────
-- Logique :
--   1. Identifier les minutes où l'équipe ADVERSE marque (buts encaissés)
--   2. Compter les actions de l'équipe dans les 10 min précédant le but
--   3. Compter les actions de l'équipe dans les 10 min suivant le but
--   4. momentum_delta = post / pre (> 1 = résilience, < 1 = effondrement)
--
-- ⚠️  Anti-leakage : on utilise uniquement les événements du match courant,
--    agrégés à la maille du match — pas d'information future.
-- ══════════════════════════════════════════════════════════════════════════
goals_conceded AS (
    -- Buts encaissés = buts marqués par l'équipe ADVERSE dans ce match
    -- On identifie le couple (ws_match_id, team_id_adverse, minute_but)
    SELECT DISTINCT
        f.ws_match_id,
        -- L'équipe qui CONCÈDE = l'autre équipe du match
        other.team_id                               AS conceding_team_id,
        f.expanded_minute                           AS goal_minute
    FROM tmp_events_flat f
    -- Joindre pour retrouver l'équipe adverse dans ce match
    JOIN (
        SELECT ws_match_id, team_id
        FROM tmp_events_flat
        GROUP BY ws_match_id, team_id
    ) other
        ON f.ws_match_id = other.ws_match_id
       AND f.team_id     != other.team_id
    WHERE f.type_id = 16          -- SavedShot / Goal
      AND f.outcome_id = 1        -- Successful = but marqué
      AND f.is_shot = TRUE
),

momentum_windows AS (
    SELECT
        gc.ws_match_id,
        gc.conceding_team_id                        AS team_id,

        -- Actions de l'équipe dans [-10min, 0min] avant le but
        COUNT(e.row_num) FILTER (
            WHERE e.expanded_minute >= gc.goal_minute - 10
              AND e.expanded_minute <  gc.goal_minute
              AND e.team_id = gc.conceding_team_id
        )                                           AS actions_pre,

        -- Actions de l'équipe dans [0min, +10min] après le but
        COUNT(e.row_num) FILTER (
            WHERE e.expanded_minute >  gc.goal_minute
              AND e.expanded_minute <= gc.goal_minute + 10
              AND e.team_id = gc.conceding_team_id
        )                                           AS actions_post

    FROM goals_conceded gc
    JOIN tmp_events_flat e
        ON e.ws_match_id = gc.ws_match_id
    GROUP BY gc.ws_match_id, gc.conceding_team_id, gc.goal_minute
),

momentum_agg AS (
    -- Moyenne du delta sur tous les buts encaissés du match
    SELECT
        ws_match_id,
        team_id,
        AVG(
            CASE
                WHEN actions_pre > 0
                THEN CAST(actions_post AS DOUBLE) / actions_pre
                ELSE NULL          -- Pas de données pré → NULL, pas 0
            END
        )                                           AS momentum_delta
    FROM momentum_windows
    GROUP BY ws_match_id, team_id
),

-- ══════════════════════════════════════════════════════════════════════════
-- 2.4 Assemblage final par (ws_match_id, team_id)
-- ══════════════════════════════════════════════════════════════════════════
assembled AS (
    SELECT
        b.ws_match_id,
        b.team_id,

        -- ── Bloc A : Pépites v1 ───────────────────────────────────────────

        -- F1 : Field Tilt (toutes actions)
        CASE WHEN b.total_touches > 0
            THEN CAST(b.touches_offensive_zone AS DOUBLE) / b.total_touches
        END                                         AS ws_field_tilt_actions,


        -- F3 : High Turnover Rate
        CASE WHEN b.total_passes > 0
            THEN CAST(b.turnovers_high_zone AS DOUBLE) / b.total_passes
        END                                         AS ws_high_turnover_rate,

        -- F5 : Deep Completion Rate
        CASE WHEN b.total_passes > 0
            THEN CAST(b.deep_completions AS DOUBLE) / b.total_passes
        END                                         AS ws_deep_completion_rt,

        -- F6 : Momentum Delta (post-but encaissé)
        m.momentum_delta                            AS ws_momentum_delta,

        -- F7 : Counter Shot Rate
        CASE WHEN b.total_shots > 0
            THEN CAST(COALESCE(q.shots_counter_attack, 0) AS DOUBLE) / b.total_shots
        END                                         AS ws_counter_shot_rate,

        -- F8 : Set Piece Pressure Index
        CASE WHEN b.offensive_actions > 0
            THEN CAST(COALESCE(q.set_pieces_offensive, 0) AS DOUBLE) / b.offensive_actions
        END                                         AS ws_set_piece_pressure,

        -- ── Bloc B : Attack Sides (axe y) ────────────────────────────────
        -- Tableau-ready : les 3 colonnes somment à ~1.0
        -- Utile pour visualiser "équipe large" vs "équipe axiale"

        CASE WHEN b.att_touches_total > 0
            THEN CAST(b.att_touches_left   AS DOUBLE) / b.att_touches_total
        END                                         AS ws_attack_left_pct,

        CASE WHEN b.att_touches_total > 0
            THEN CAST(b.att_touches_center AS DOUBLE) / b.att_touches_total
        END                                         AS ws_attack_center_pct,

        CASE WHEN b.att_touches_total > 0
            THEN CAST(b.att_touches_right  AS DOUBLE) / b.att_touches_total
        END                                         AS ws_attack_right_pct,

        -- ── Bloc C : Action Zones (axe x) ────────────────────────────────
        -- Tableau-ready : les 3 colonnes somment à ~1.0
        -- Complémentaire de field_tilt : donne la répartition complète du jeu

        CASE WHEN b.total_touches > 0
            THEN CAST(b.zone_def_touches AS DOUBLE) / b.total_touches
        END                                         AS ws_zone_def_pct,

        CASE WHEN b.total_touches > 0
            THEN CAST(b.zone_mid_touches AS DOUBLE) / b.total_touches
        END                                         AS ws_zone_mid_pct,

        CASE WHEN b.total_touches > 0
            THEN CAST(b.zone_att_touches AS DOUBLE) / b.total_touches
        END                                         AS ws_zone_att_pct,

        -- ── Bloc D : Shot Zones ───────────────────────────────────────────
        -- Tableau-ready : les 3 colonnes somment à ~1.0
        -- Signal modèle : shot_penalty_pct élevé = équipe qui pénètre bien

        CASE WHEN b.total_shots > 0
            THEN CAST(b.shots_six_yard    AS DOUBLE) / b.total_shots
        END                                         AS ws_shot_six_yard_pct,

        CASE WHEN b.total_shots > 0
            THEN CAST(b.shots_penalty_area AS DOUBLE) / b.total_shots
        END                                         AS ws_shot_penalty_pct,

        CASE WHEN b.total_shots > 0
            THEN CAST(b.shots_out_of_box  AS DOUBLE) / b.total_shots
        END                                         AS ws_shot_oob_pct,

        -- ── Bloc E : Attempt Types ────────────────────────────────────────
        -- Normalisés par total_shots → proportions comparables

        CASE WHEN b.total_shots > 0
            THEN CAST(COALESCE(q.shots_open_play,  0) AS DOUBLE) / b.total_shots
        END                                         AS ws_shot_open_play_pct,

        CASE WHEN b.total_shots > 0
            THEN CAST(COALESCE(q.shots_set_piece,  0) AS DOUBLE) / b.total_shots
        END                                         AS ws_shot_set_piece_pct,

        CASE WHEN b.total_shots > 0
            THEN CAST(COALESCE(q.shots_penalty,    0) AS DOUBLE) / b.total_shots
        END                                         AS ws_shot_penalty_att_pct,

        -- Conversion rate : buts / total tirs
        -- ⚠️  Feature de résultat du match — à utiliser uniquement en historique
        --    (LAG(1) dans tmp_latest_ws garantit l'absence de leakage)
        CASE WHEN b.total_shots > 0
            THEN CAST(b.goals_scored AS DOUBLE) / b.total_shots
        END                                         AS ws_conversion_rate,

        -- ── Bloc F : Pass Types ───────────────────────────────────────────
        -- Normalisés par total_passes → style de jeu relatif
        -- Short pass = résiduel (1 - cross - through - long) pour éviter
        -- la colinéarité parfaite dans le modèle

        CASE WHEN b.total_passes > 0
            THEN CAST(COALESCE(q.passes_cross,        0) AS DOUBLE) / b.total_passes
        END                                         AS ws_cross_rate,

        CASE WHEN b.total_passes > 0
            THEN CAST(COALESCE(q.passes_through_ball, 0) AS DOUBLE) / b.total_passes
        END                                         AS ws_through_ball_rate,

        CASE WHEN b.total_passes > 0
            THEN CAST(COALESCE(q.passes_long_ball,    0) AS DOUBLE) / b.total_passes
        END                                         AS ws_long_ball_rate,

        -- Short pass rate : résiduel des passes non qualifiées
        -- = 1 - cross_rate - through_ball_rate - long_ball_rate
        -- Représente les passes courtes/medium dans l'axe
        CASE WHEN b.total_passes > 0
            THEN 1.0 - (
                CAST(COALESCE(q.passes_cross,        0) AS DOUBLE) / b.total_passes
              + CAST(COALESCE(q.passes_through_ball, 0) AS DOUBLE) / b.total_passes
              + CAST(COALESCE(q.passes_long_ball,    0) AS DOUBLE) / b.total_passes
            )
        END                                         AS ws_short_pass_rate,

        -- ── Bloc G : Defensive Exposure (v2) ─────────────────────────────
        -- Où l'adversaire génère ses actions offensives contre nous.
        -- Coordonnées dans le référentiel de l'attaquant adverse :
        --   opp_att_left  (y < 33.3)  = notre couloir DROIT défensif exposé
        --   opp_att_right (y > 66.6)  = notre couloir GAUCHE défensif exposé
        -- On stocke dans la perspective de l'équipe qui DÉFEND.
        CASE WHEN de.opp_att_total > 0
            THEN CAST(de.opp_att_left   AS DOUBLE) / de.opp_att_total
        END                                         AS ws_def_exposed_left_pct,

        CASE WHEN de.opp_att_total > 0
            THEN CAST(de.opp_att_center AS DOUBLE) / de.opp_att_total
        END                                         AS ws_def_exposed_center_pct,

        CASE WHEN de.opp_att_total > 0
            THEN CAST(de.opp_att_right  AS DOUBLE) / de.opp_att_total
        END                                         AS ws_def_exposed_right_pct

    FROM base_counts b
    LEFT JOIN qualifier_features q
        ON  b.ws_match_id = q.ws_match_id
        AND b.team_id     = q.team_id
    LEFT JOIN momentum_agg m
        ON  b.ws_match_id = m.ws_match_id
        AND b.team_id     = m.team_id
    LEFT JOIN defensive_exposure de
        ON  b.ws_match_id = de.ws_match_id
        AND b.team_id     = de.team_id
)

SELECT * FROM assembled
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# PASSE 3 — Pivot home/away + jointure sur gold.features_training
# ─────────────────────────────────────────────────────────────────────────────

SQL_PIVOT_HOME_AWAY = """
CREATE OR REPLACE TEMP TABLE tmp_pivot AS
WITH

-- Récupérer home_team_id et away_team_id depuis stg_whoscored_match_index
match_meta AS (
    SELECT
        ws_match_id,
        home_team_id,
        away_team_id,
        season
    FROM silver.stg_whoscored_match_index
),

home_features AS (
    SELECT
        f.ws_match_id,
        -- Pépites v1
        f.ws_field_tilt_actions   AS home_field_tilt_actions,
        --f.ws_field_tilt_shots     AS home_field_tilt_shots,
        f.ws_high_turnover_rate   AS home_high_turnover_rate,
       --f.ws_shot_angle_quality   AS home_shot_angle_quality,
        f.ws_deep_completion_rt   AS home_deep_completion_rt,
        f.ws_momentum_delta       AS home_momentum_delta,
        f.ws_counter_shot_rate    AS home_counter_shot_rate,
        f.ws_set_piece_pressure   AS home_set_piece_pressure,
        -- Bloc B : Attack Sides
        f.ws_attack_left_pct      AS home_attack_left_pct,
        f.ws_attack_center_pct    AS home_attack_center_pct,
        f.ws_attack_right_pct     AS home_attack_right_pct,
        -- Bloc C : Action Zones
        f.ws_zone_def_pct         AS home_zone_def_pct,
        f.ws_zone_mid_pct         AS home_zone_mid_pct,
        f.ws_zone_att_pct         AS home_zone_att_pct,
        -- Bloc D : Shot Zones
        f.ws_shot_six_yard_pct    AS home_shot_six_yard_pct,
        f.ws_shot_penalty_pct     AS home_shot_penalty_pct,
        f.ws_shot_oob_pct         AS home_shot_oob_pct,
        -- Bloc E : Attempt Types
        f.ws_shot_open_play_pct   AS home_shot_open_play_pct,
        f.ws_shot_set_piece_pct   AS home_shot_set_piece_pct,
        f.ws_shot_penalty_att_pct AS home_shot_penalty_att_pct,
        f.ws_conversion_rate      AS home_conversion_rate,
        -- Bloc F : Pass Types
        f.ws_cross_rate           AS home_cross_rate,
        f.ws_through_ball_rate    AS home_through_ball_rate,
        f.ws_long_ball_rate       AS home_long_ball_rate,
        f.ws_short_pass_rate      AS home_short_pass_rate,
        -- Bloc G : Defensive Exposure (v2)
        f.ws_def_exposed_left_pct   AS home_def_exposed_left_pct,
        f.ws_def_exposed_center_pct AS home_def_exposed_center_pct,
        f.ws_def_exposed_right_pct  AS home_def_exposed_right_pct
    FROM tmp_team_features f
    JOIN match_meta m
        ON  f.ws_match_id = m.ws_match_id
        AND f.team_id     = m.home_team_id
),

away_features AS (
    SELECT
        f.ws_match_id,
        -- Pépites v1
        f.ws_field_tilt_actions   AS away_field_tilt_actions,
        --f.ws_field_tilt_shots     AS away_field_tilt_shots,
        f.ws_high_turnover_rate   AS away_high_turnover_rate,
        --f.ws_shot_angle_quality   AS away_shot_angle_quality,
        f.ws_deep_completion_rt   AS away_deep_completion_rt,
        f.ws_momentum_delta       AS away_momentum_delta,
        f.ws_counter_shot_rate    AS away_counter_shot_rate,
        f.ws_set_piece_pressure   AS away_set_piece_pressure,
        -- Bloc B : Attack Sides
        f.ws_attack_left_pct      AS away_attack_left_pct,
        f.ws_attack_center_pct    AS away_attack_center_pct,
        f.ws_attack_right_pct     AS away_attack_right_pct,
        -- Bloc C : Action Zones
        f.ws_zone_def_pct         AS away_zone_def_pct,
        f.ws_zone_mid_pct         AS away_zone_mid_pct,
        f.ws_zone_att_pct         AS away_zone_att_pct,
        -- Bloc D : Shot Zones
        f.ws_shot_six_yard_pct    AS away_shot_six_yard_pct,
        f.ws_shot_penalty_pct     AS away_shot_penalty_pct,
        f.ws_shot_oob_pct         AS away_shot_oob_pct,
        -- Bloc E : Attempt Types
        f.ws_shot_open_play_pct   AS away_shot_open_play_pct,
        f.ws_shot_set_piece_pct   AS away_shot_set_piece_pct,
        f.ws_shot_penalty_att_pct AS away_shot_penalty_att_pct,
        f.ws_conversion_rate      AS away_conversion_rate,
        -- Bloc F : Pass Types
        f.ws_cross_rate           AS away_cross_rate,
        f.ws_through_ball_rate    AS away_through_ball_rate,
        f.ws_long_ball_rate       AS away_long_ball_rate,
        f.ws_short_pass_rate      AS away_short_pass_rate,
        -- Bloc G : Defensive Exposure (v2)
        f.ws_def_exposed_left_pct   AS away_def_exposed_left_pct,
        f.ws_def_exposed_center_pct AS away_def_exposed_center_pct,
        f.ws_def_exposed_right_pct  AS away_def_exposed_right_pct
    FROM tmp_team_features f
    JOIN match_meta m
        ON  f.ws_match_id = m.ws_match_id
        AND f.team_id     = m.away_team_id
)

SELECT
    m.ws_match_id,
    m.season,
    mi.match_date,
    mi.home_team_name,
    mi.away_team_name,
    h.*  EXCLUDE (ws_match_id),
    a.*  EXCLUDE (ws_match_id)
FROM match_meta m
JOIN silver.stg_whoscored_match_index mi
    ON m.ws_match_id = mi.ws_match_id
LEFT JOIN home_features h ON m.ws_match_id = h.ws_match_id
LEFT JOIN away_features a ON m.ws_match_id = a.ws_match_id
;
"""


# ─────────────────────────────────────────────────────────────────────────────
# Anti-leakage : jointure sur gold.features_training avec LAG(1) par équipe
#
# Principe :
#   Pour chaque ligne de gold.features_training (= un match futur à prédire),
#   on cherche les features WhoScored du DERNIER match joué par cette équipe
#   AVANT la date courante (via LAG sur ws_match_date trié par date).
#
# La table tmp_pivot contient les features du match ws_match_id.
# On identifie à quelle équipe (home ou away) chaque match appartient,
# puis on joint sur (team_name, date) en prenant le match le plus récent
# strictement antérieur au match courant de features_training.
# ─────────────────────────────────────────────────────────────────────────────

SQL_JOIN_TRAINING = """
CREATE OR REPLACE TEMP TABLE tmp_ws_team_history AS
-- Vue unifiée team-centric des features WhoScored
-- Les noms WhoScored (home_team_name / away_team_name) sont normalisés
-- via tmp_team_mapping (chargé depuis config.yaml) pour correspondre
-- aux noms canoniques de gold.features_training.
-- Si un nom WhoScored n'est pas dans le mapping, on le garde tel quel
-- (COALESCE) pour maximiser la couverture.
WITH
home_side AS (
    SELECT
        p.match_date                              AS ws_date,
        p.season                                AS ws_season,
        COALESCE(tm.canonical_name, p.home_team_name) AS team_name,
        p.home_field_tilt_actions   AS ws_field_tilt_actions,
        --p.home_field_tilt_shots     AS ws_field_tilt_shots,
        p.home_high_turnover_rate   AS ws_high_turnover_rate,
        --p.home_shot_angle_quality   AS ws_shot_angle_quality,
        p.home_deep_completion_rt   AS ws_deep_completion_rt,
        p.home_momentum_delta       AS ws_momentum_delta,
        p.home_counter_shot_rate    AS ws_counter_shot_rate,
        p.home_set_piece_pressure   AS ws_set_piece_pressure,
        p.home_attack_left_pct      AS ws_attack_left_pct,
        p.home_attack_center_pct    AS ws_attack_center_pct,
        p.home_attack_right_pct     AS ws_attack_right_pct,
        p.home_zone_def_pct         AS ws_zone_def_pct,
        p.home_zone_mid_pct         AS ws_zone_mid_pct,
        p.home_zone_att_pct         AS ws_zone_att_pct,
        p.home_shot_six_yard_pct    AS ws_shot_six_yard_pct,
        p.home_shot_penalty_pct     AS ws_shot_penalty_pct,
        p.home_shot_oob_pct         AS ws_shot_oob_pct,
        p.home_shot_open_play_pct   AS ws_shot_open_play_pct,
        p.home_shot_set_piece_pct   AS ws_shot_set_piece_pct,
        p.home_shot_penalty_att_pct AS ws_shot_penalty_att_pct,
        p.home_conversion_rate      AS ws_conversion_rate,
        p.home_cross_rate           AS ws_cross_rate,
        p.home_through_ball_rate    AS ws_through_ball_rate,
        p.home_long_ball_rate       AS ws_long_ball_rate,
        p.home_short_pass_rate      AS ws_short_pass_rate,
        -- Bloc G : Defensive Exposure (v2)
        p.home_def_exposed_left_pct   AS ws_def_exposed_left_pct,
        p.home_def_exposed_center_pct AS ws_def_exposed_center_pct,
        p.home_def_exposed_right_pct  AS ws_def_exposed_right_pct
    FROM tmp_pivot p
    LEFT JOIN tmp_team_mapping tm
        ON p.home_team_name = tm.raw_name
    WHERE p.home_team_name IS NOT NULL
),
away_side AS (
    SELECT
        p.match_date,
        p.season,
        COALESCE(tm.canonical_name, p.away_team_name) AS team_name,
        p.away_field_tilt_actions,
        --p.away_field_tilt_shots,
        p.away_high_turnover_rate,
        --p.away_shot_angle_quality,
        p.away_deep_completion_rt,
        p.away_momentum_delta,
        p.away_counter_shot_rate,
        p.away_set_piece_pressure,
        p.away_attack_left_pct,
        p.away_attack_center_pct,
        p.away_attack_right_pct,
        p.away_zone_def_pct,
        p.away_zone_mid_pct,
        p.away_zone_att_pct,
        p.away_shot_six_yard_pct,
        p.away_shot_penalty_pct,
        p.away_shot_oob_pct,
        p.away_shot_open_play_pct,
        p.away_shot_set_piece_pct,
        p.away_shot_penalty_att_pct,
        p.away_conversion_rate,
        p.away_cross_rate,
        p.away_through_ball_rate,
        p.away_long_ball_rate,
        p.away_short_pass_rate,
        -- Bloc G : Defensive Exposure (v2)
        p.away_def_exposed_left_pct   AS ws_def_exposed_left_pct,
        p.away_def_exposed_center_pct AS ws_def_exposed_center_pct,
        p.away_def_exposed_right_pct  AS ws_def_exposed_right_pct
    FROM tmp_pivot p
    LEFT JOIN tmp_team_mapping tm
        ON p.away_team_name = tm.raw_name
    WHERE p.away_team_name IS NOT NULL
)
SELECT * FROM home_side
UNION ALL
SELECT * FROM away_side
;
"""


def add_columns_if_not_exist(conn: duckdb.DuckDBPyConnection) -> None:
    """Ajoute les nouvelles colonnes à gold.features_training si absentes."""
    for col_name, col_type in NEW_COLS:
        try:
            conn.execute(f"""
                ALTER TABLE gold.features_training
                ADD COLUMN IF NOT EXISTS {col_name} {col_type}
            """)
            logger.debug(f"  Colonne {col_name} vérifiée/ajoutée")
        except Exception as e:
            logger.warning(f"  ALTER TABLE features_training : {e}")

    for col_name, col_type in DIFF_COLS:
        try:
            conn.execute(f"""
                ALTER TABLE gold.features_final
                ADD COLUMN IF NOT EXISTS {col_name} {col_type}
            """)
            logger.debug(f"  Colonne {col_name} vérifiée/ajoutée (features_final)")
        except Exception as e:
            logger.warning(f"  ALTER TABLE features_final : {e}")


def reset_columns(conn: duckdb.DuckDBPyConnection) -> None:
    """Remet à NULL toutes les colonnes ajoutées par ce script."""
    logger.warning("  Reset des colonnes ws_* dans gold.features_training...")
    for col_name, _ in NEW_COLS:
        try:
            conn.execute(
                f"UPDATE gold.features_training SET {col_name} = NULL"
            )
        except Exception:
            pass
    for col_name, _ in DIFF_COLS:
        try:
            conn.execute(
                f"UPDATE gold.features_final SET {col_name} = NULL"
            )
        except Exception:
            pass


def inject_team_mapping(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Injecte le team_mapping de config.yaml dans une table temporaire DuckDB.

    Table créée : tmp_team_mapping (raw_name VARCHAR, canonical_name VARCHAR)

    Stratégie :
      - On charge TEAM_MAPPING_ROWS (list de tuples) en Python
      - On l'enregistre comme relation DuckDB via conn.register()
      - On crée la TEMP TABLE depuis cette relation

    Cela évite toute écriture sur disque et reste 100% en mémoire.
    """
    if not TEAM_MAPPING_ROWS:
        logger.warning("  team_mapping vide dans config.yaml — jointure sur nom brut")
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE tmp_team_mapping (
                raw_name       VARCHAR,
                canonical_name VARCHAR
            )
        """)
        return

    import pandas as pd
    df_mapping = pd.DataFrame(TEAM_MAPPING_ROWS, columns=["raw_name", "canonical_name"])
    conn.register("_df_team_mapping", df_mapping)
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_team_mapping AS
        SELECT raw_name, canonical_name
        FROM _df_team_mapping
    """)
    conn.unregister("_df_team_mapping")

    n = conn.execute("SELECT COUNT(*) FROM tmp_team_mapping").fetchone()[0]
    logger.info(f"  tmp_team_mapping chargé : {n:,} entrées depuis config.yaml")

    # Diagnostic : noms WhoScored non couverts par le mapping
    try:
        uncovered = conn.execute("""
            SELECT DISTINCT home_team_name AS name
            FROM tmp_pivot
            WHERE home_team_name IS NOT NULL
              AND home_team_name NOT IN (SELECT raw_name FROM tmp_team_mapping)
            UNION
            SELECT DISTINCT away_team_name
            FROM tmp_pivot
            WHERE away_team_name IS NOT NULL
              AND away_team_name NOT IN (SELECT raw_name FROM tmp_team_mapping)
            ORDER BY name
        """).fetchall()
        if uncovered:
            names = [r[0] for r in uncovered]
            logger.warning(
                f"  ⚠️  {len(names)} noms WhoScored hors mapping "
                f"(jointure sur nom brut) : {names[:15]}"
                + (" [...]" if len(names) > 15 else "")
            )
        else:
            logger.info("  ✅ Tous les noms WhoScored sont couverts par le mapping")
    except Exception:
        pass  # tmp_pivot pas encore créé = appel anticipé, ignoré


def run_passe1_2(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Passe 1 : explosion qualifiers_json
    Passe 2 : agrégations par (ws_match_id, team_id)
    Retourne le nombre de lignes dans tmp_team_features.
    """
    logger.info("Passe 1 — Explosion qualifiers_json (DuckDB UNNEST)...")
    conn.execute(SQL_EVENTS_FLAT)
    n_flat = conn.execute("SELECT COUNT(*) FROM tmp_events_flat").fetchone()[0]
    logger.info(f"  {n_flat:,} événements chargés dans tmp_events_flat")

    conn.execute(SQL_EXPLODE_QUALIFIERS)
    n_qual = conn.execute("SELECT COUNT(*) FROM tmp_events_qual").fetchone()[0]
    logger.info(f"  {n_qual:,} lignes qualifiers dans tmp_events_qual")

    logger.info("Passe 2 — Agrégations spatiales et comportementales...")
    conn.execute(SQL_TEAM_FEATURES)
    n_team = conn.execute("SELECT COUNT(*) FROM tmp_team_features").fetchone()[0]
    logger.info(f"  {n_team:,} lignes dans tmp_team_features ({n_team // 2} matchs)")

    return n_team


def run_passe3(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Passe 3 : pivot home/away + jointure sur gold.features_training avec LAG(1).
    Retourne le nombre de lignes mises à jour.
    """
    logger.info("Passe 3 — Pivot home/away + anti-leakage LAG(1)...")

    conn.execute(SQL_PIVOT_HOME_AWAY)
    n_pivot = conn.execute("SELECT COUNT(*) FROM tmp_pivot").fetchone()[0]
    logger.info(f"  {n_pivot:,} matchs dans tmp_pivot")

    # ── Injection du team_mapping pour normalisation des noms WhoScored ───────
    inject_team_mapping(conn)

    conn.execute(SQL_JOIN_TRAINING)
    n_hist = conn.execute("SELECT COUNT(*) FROM tmp_ws_team_history").fetchone()[0]
    logger.info(f"  {n_hist:,} lignes dans l'historique team-centric")

    # ── Anti-leakage UPDATE ───────────────────────────────────────────────────
    # Pour chaque équipe × match dans features_training,
    # on cherche le match WhoScored le plus récent STRICTEMENT AVANT la date.
    # QUALIFY ROW_NUMBER() garantit l'unicité : 1 seul match précédent retenu.
    logger.info("  UPDATE gold.features_training avec LAG(1) anti-leakage...")

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_latest_ws AS
        SELECT
            ft.team                             AS team,
            ft.date                             AS ft_date,
            wsh.ws_date,
            -- Pépites v1
            wsh.ws_field_tilt_actions,
            --wsh.ws_field_tilt_shots,
            wsh.ws_high_turnover_rate,
            --wsh.ws_shot_angle_quality,
            wsh.ws_deep_completion_rt,
            wsh.ws_momentum_delta,
            wsh.ws_counter_shot_rate,
            wsh.ws_set_piece_pressure,
            -- Bloc B : Attack Sides
            wsh.ws_attack_left_pct,
            wsh.ws_attack_center_pct,
            wsh.ws_attack_right_pct,
            -- Bloc C : Action Zones
            wsh.ws_zone_def_pct,
            wsh.ws_zone_mid_pct,
            wsh.ws_zone_att_pct,
            -- Bloc D : Shot Zones
            wsh.ws_shot_six_yard_pct,
            wsh.ws_shot_penalty_pct,
            wsh.ws_shot_oob_pct,
            -- Bloc E : Attempt Types
            wsh.ws_shot_open_play_pct,
            wsh.ws_shot_set_piece_pct,
            wsh.ws_shot_penalty_att_pct,
            wsh.ws_conversion_rate,
            -- Bloc F : Pass Types
            wsh.ws_cross_rate,
            wsh.ws_through_ball_rate,
            wsh.ws_long_ball_rate,
            wsh.ws_short_pass_rate,
            -- Bloc G : Defensive Exposure (v2)
            wsh.ws_def_exposed_left_pct,
            wsh.ws_def_exposed_center_pct,
            wsh.ws_def_exposed_right_pct
        FROM gold.features_training ft
        JOIN tmp_ws_team_history wsh
            ON  ft.team    = wsh.team_name
            AND wsh.ws_date < ft.date          -- anti-leakage strict
            AND ft.season   = wsh.ws_season    -- ← NOUVEAU : même saison uniquement
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ft.team, ft.date
            ORDER BY wsh.ws_date DESC
        ) = 1
    """)

    n_joinable = conn.execute("SELECT COUNT(*) FROM tmp_latest_ws").fetchone()[0]
    logger.info(f"  {n_joinable:,} lignes joignables avec anti-leakage")

    # Update final
    conn.execute("""
        UPDATE gold.features_training AS ft
        SET
            -- Pépites v1
            ws_field_tilt_actions    = lws.ws_field_tilt_actions,
            --ws_field_tilt_shots      = lws.ws_field_tilt_shots,
            ws_high_turnover_rate    = lws.ws_high_turnover_rate,
            --ws_shot_angle_quality    = lws.ws_shot_angle_quality,
            ws_deep_completion_rt    = lws.ws_deep_completion_rt,
            ws_momentum_delta        = lws.ws_momentum_delta,
            ws_counter_shot_rate     = lws.ws_counter_shot_rate,
            ws_set_piece_pressure    = lws.ws_set_piece_pressure,
            -- Bloc B : Attack Sides
            ws_attack_left_pct       = lws.ws_attack_left_pct,
            ws_attack_center_pct     = lws.ws_attack_center_pct,
            ws_attack_right_pct      = lws.ws_attack_right_pct,
            -- Bloc C : Action Zones
            ws_zone_def_pct          = lws.ws_zone_def_pct,
            ws_zone_mid_pct          = lws.ws_zone_mid_pct,
            ws_zone_att_pct          = lws.ws_zone_att_pct,
            -- Bloc D : Shot Zones
            ws_shot_six_yard_pct     = lws.ws_shot_six_yard_pct,
            ws_shot_penalty_pct      = lws.ws_shot_penalty_pct,
            ws_shot_oob_pct          = lws.ws_shot_oob_pct,
            -- Bloc E : Attempt Types
            ws_shot_open_play_pct    = lws.ws_shot_open_play_pct,
            ws_shot_set_piece_pct    = lws.ws_shot_set_piece_pct,
            ws_shot_penalty_att_pct  = lws.ws_shot_penalty_att_pct,
            ws_conversion_rate       = lws.ws_conversion_rate,
            -- Bloc F : Pass Types
            ws_cross_rate            = lws.ws_cross_rate,
            ws_through_ball_rate     = lws.ws_through_ball_rate,
            ws_long_ball_rate        = lws.ws_long_ball_rate,
            ws_short_pass_rate       = lws.ws_short_pass_rate,
            -- Bloc G : Defensive Exposure (v2)
            ws_def_exposed_left_pct   = lws.ws_def_exposed_left_pct,
            ws_def_exposed_center_pct = lws.ws_def_exposed_center_pct,
            ws_def_exposed_right_pct  = lws.ws_def_exposed_right_pct
        FROM tmp_latest_ws lws
        WHERE ft.team = lws.team
          AND ft.date = lws.ft_date
    """)

    # ── has_ws_events : flag qualité de données ───────────────────────────────
    # Calculé en une seule passe après le remplissage de ws_field_tilt_actions.
    # 1 = au moins une feature ws_* renseignée, 0 = toutes NULL.
    # Permet à LGBM de distinguer "données manquantes structurelles" (Bundesliga,
    # La Liga) du bruit aléatoire, sans biaiser les prédictions via l'imputer.
    logger.info("  Calcul has_ws_events...")
    conn.execute("""
        UPDATE gold.features_training
        SET has_ws_events = CASE
            WHEN ws_field_tilt_actions IS NOT NULL THEN 1
            ELSE 0
        END
    """)

    # Vérification count
    n_updated = conn.execute("""
        SELECT COUNT(*) FROM gold.features_training
        WHERE ws_field_tilt_actions IS NOT NULL
    """).fetchone()[0]
    logger.info(f"  {n_updated:,} lignes de features_training enrichies")

    return n_updated


def build_differential_features(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Calcule les différentiels home/away dans gold.features_final.

    Ces features différentielles sont les plus informatives pour LGBM :
    un seul split suffit pour capturer l'asymétrie entre les deux équipes.

    Logique :
        - features_final a déjà une ligne par équipe (team vs opponent)
        - On joint features_training pour récupérer les valeurs adversaire
        - diff = team_value - opp_value
    """
    logger.info("  Calcul des différentiels ws_* dans gold.features_final...")

    conn.execute("""
        UPDATE gold.features_final AS ff
        SET
            -- ── Pépites v1 diffs ──────────────────────────────────────────────
            -- Field Tilt : > 0 → l'équipe contrôle mieux le territoire offensif
            --ws_field_tilt_diff      = ft_team.ws_field_tilt_shots
            --                        - ft_opp.ws_field_tilt_shots,

            -- Shot Quality Spatial : > 0 → tirs depuis meilleurs angles
            --ws_shot_quality_diff    = ft_team.ws_shot_angle_quality
            --- ft_opp.ws_shot_angle_quality,

            -- Turnover Zone : < 0 → moins de pertes en zone haute (favorable)
            ws_turnover_zone_diff   = ft_team.ws_high_turnover_rate
                                    - ft_opp.ws_high_turnover_rate,

            -- Deep Pass : > 0 → pénètre plus souvent dans la surface
            ws_deep_pass_diff       = ft_team.ws_deep_completion_rt
                                    - ft_opp.ws_deep_completion_rt,

            -- Momentum : > 0 → meilleure résilience post-but encaissé
            ws_momentum_diff        = ft_team.ws_momentum_delta
                                    - ft_opp.ws_momentum_delta,

            -- Counter : > 0 → plus dangereux en transition
            ws_counter_threat_diff  = ft_team.ws_counter_shot_rate
                                    - ft_opp.ws_counter_shot_rate,

            -- ── Classics diffs (v2) ───────────────────────────────────────────
            -- Attack Width : > 0 → équipe plus axiale (jeu central vs large)
            -- Utile Tableau : visualiser le duel style étroit vs jeu ouvert
            ws_attack_width_diff    = ft_team.ws_attack_center_pct
                                    - ft_opp.ws_attack_center_pct,

            -- Zone Attack : > 0 → plus de temps en zone offensive (domination terr.)
            ws_zone_att_diff        = ft_team.ws_zone_att_pct
                                    - ft_opp.ws_zone_att_pct,

            -- Shot Penetration : > 0 → tire plus souvent depuis la surface
            -- Signal upset : outsider avec shot_penalty_pct élevé = efficace malgré faible xG
            ws_shot_zone_diff       = ft_team.ws_shot_penalty_pct
                                    - ft_opp.ws_shot_penalty_pct,

            -- Conversion : > 0 → plus clinique sur la période récente
            -- Attention : forte régression attendue si très positif
            ws_conversion_diff      = ft_team.ws_conversion_rate
                                    - ft_opp.ws_conversion_rate,

            -- Cross Style : > 0 → équipe plus directe / centres fréquents
            -- < 0 → équipe de combinaison : plus de passes courtes dans l'axe
            ws_cross_diff           = ft_team.ws_cross_rate
                                    - ft_opp.ws_cross_rate,

            -- Long Ball : > 0 → jeu plus direct/physique que l'adversaire
            ws_long_ball_diff       = ft_team.ws_long_ball_rate
                                    - ft_opp.ws_long_ball_rate,

            -- ── Matchup advantages (v2) ───────────────────────────────────────
            -- Croisement style attaque équipe vs vulnérabilité défensive adversaire.
            -- Logique : on attaque à gauche (attack_left_pct),
            --           l'adversaire est-il vulnérable sur son côté droit défensif
            --           (def_exposed_right_pct, qui correspond à notre gauche) ?
            -- > 0 = avantage structurel sur ce couloir
            -- Coordonnées : attack_left_pct = % actions équipe y < 33.3 (côté gauche attaquant)
            --               def_exposed_right_pct = % actions adverses sur y > 66.6 (côté droit attaquant)
            --               → les deux côtés se font face sur le terrain

            ws_left_matchup_adv     = ft_team.ws_attack_left_pct
                                    - ft_opp.ws_def_exposed_right_pct,

            ws_right_matchup_adv    = ft_team.ws_attack_right_pct
                                    - ft_opp.ws_def_exposed_left_pct,

            ws_center_matchup_adv   = ft_team.ws_attack_center_pct
                                    - ft_opp.ws_def_exposed_center_pct

        FROM gold.features_training ft_team
        JOIN gold.features_training ft_opp
            ON  ft_team.date          = ft_opp.date
            AND ft_team.opponent      = ft_opp.team
            AND ft_team.league_source = ft_opp.league_source
        WHERE ff.date          = ft_team.date
          AND ff.team          = ft_team.team
          AND ff.league_source = ft_team.league_source
          --AND ft_team.ws_field_tilt_shots IS NOT NULL
          --AND ft_opp.ws_field_tilt_shots  IS NOT NULL
    """)

    n_diff = conn.execute("""
        SELECT COUNT(*) FROM gold.features_final
        --WHERE ws_field_tilt_diff IS NOT NULL
    """).fetchone()[0]
    logger.info(f"  {n_diff:,} lignes de features_final enrichies avec différentiels")


def print_coverage_report(conn: duckdb.DuckDBPyConnection) -> None:
    """Rapport de couverture sur les nouvelles colonnes."""
    logger.info("═══ Rapport de couverture — 03b ═══")

    total = conn.execute(
        "SELECT COUNT(*) FROM gold.features_training"
    ).fetchone()[0]

    logger.info(f"  gold.features_training : {total:,} lignes totales")
    for col_name, _ in NEW_COLS:
        try:
            n_ok = conn.execute(
                f"SELECT COUNT(*) FROM gold.features_training "
                f"WHERE {col_name} IS NOT NULL"
            ).fetchone()[0]
            pct = n_ok / total * 100 if total else 0
            status = "✅" if pct > 50 else "⚠️ " if pct > 10 else "❌"
            logger.info(
                f"  {status} {col_name:<30} : {n_ok:>7,}/{total:,} "
                f"({pct:.1f}%)"
            )
        except Exception as e:
            logger.warning(f"  {col_name} : erreur coverage ({e})")

    # Couverture features_final
    total_ff = conn.execute(
        "SELECT COUNT(*) FROM gold.features_final"
    ).fetchone()[0]
    logger.info(f"\n  gold.features_final : {total_ff:,} lignes totales")
    for col_name, _ in DIFF_COLS:
        try:
            n_ok = conn.execute(
                f"SELECT COUNT(*) FROM gold.features_final "
                f"WHERE {col_name} IS NOT NULL"
            ).fetchone()[0]
            pct = n_ok / total_ff * 100 if total_ff else 0
            status = "✅" if pct > 50 else "⚠️ " if pct > 10 else "❌"
            logger.info(
                f"  {status} {col_name:<30} : {n_ok:>7,}/{total_ff:,} "
                f"({pct:.1f}%)"
            )
        except Exception as e:
            logger.warning(f"  {col_name} : erreur coverage ({e})")

    # Aperçu des valeurs moyennes pour sanity check
    logger.info("\n  Aperçu des valeurs moyennes (sanity check) :")
    for col_name, _ in NEW_COLS:
        try:
            stats = conn.execute(f"""
                SELECT
                    AVG({col_name})    AS mean,
                    MIN({col_name})    AS min_val,
                    MAX({col_name})    AS max_val
                FROM gold.features_training
                WHERE {col_name} IS NOT NULL
            """).fetchone()
            if stats and stats[0] is not None:
                logger.info(
                    f"    {col_name:<30} : "
                    f"mean={stats[0]:.4f}  "
                    f"min={stats[1]:.4f}  "
                    f"max={stats[2]:.4f}"
                )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(reset_cols: bool = False, coverage_only: bool = False) -> None:
    """Pipeline complet 03b."""
    logger.info("═══ Pipeline 03b — WhoScored Events → Gold Features ═══")

    if not DB_PATH.exists():
        logger.error(f"DuckDB introuvable : {DB_PATH}")
        raise FileNotFoundError(DB_PATH)

    # Fix: forcer un répertoire temporaire valide (chemin absolu Windows)
    
    _tmp_dir = Path(tempfile.gettempdir()) / "duckdb_03b_tmp"
    _tmp_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(f"SET temp_directory='{_tmp_dir.as_posix()}'")

    # ── Vérification prérequis ────────────────────────────────────────────────
    try:
        n_events = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_events"
        ).fetchone()[0]
        logger.info(f"  silver.stg_whoscored_events : {n_events:,} événements disponibles")

        n_index = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_match_index"
        ).fetchone()[0]
        logger.info(f"  silver.stg_whoscored_match_index : {n_index:,} matchs")

        n_training = conn.execute(
            "SELECT COUNT(*) FROM gold.features_training"
        ).fetchone()[0]
        logger.info(f"  gold.features_training : {n_training:,} lignes (cible)")

    except Exception as e:
        logger.error(f"  Prérequis manquant : {e}")
        conn.close()
        raise

    if n_events == 0:
        logger.warning("  Aucun événement WhoScored — pipeline 03b ignoré")
        conn.close()
        return

    # ── Mode coverage only ────────────────────────────────────────────────────
    if coverage_only:
        print_coverage_report(conn)
        conn.close()
        return

    # ── Ajout des colonnes ────────────────────────────────────────────────────
    add_columns_if_not_exist(conn)

    if reset_cols:
        reset_columns(conn)

    # ── Passes 1 & 2 ─────────────────────────────────────────────────────────
    run_passe1_2(conn)

    # ── Passe 3 ───────────────────────────────────────────────────────────────
    run_passe3(conn)

    # ── Différentiels features_final ─────────────────────────────────────────
    try:
        build_differential_features(conn)
    except Exception as e:
        logger.warning(
            f"  Différentiels features_final ignorés (features_final absent ?) : {e}"
        )

    # ── Rapport de couverture ─────────────────────────────────────────────────
    print_coverage_report(conn)

    conn.close()
    logger.success("═══ Pipeline 03b terminé ═══")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering WhoScored Events → Gold (03b)"
    )
    parser.add_argument(
        "--reset-cols",
        action="store_true",
        help="Remet à NULL toutes les colonnes ws_* avant recalcul",
    )
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Affiche uniquement le rapport de couverture sans recalculer",
    )
    args = parser.parse_args()

    run_pipeline(
        reset_cols=args.reset_cols,
        coverage_only=args.coverage_only,
    )