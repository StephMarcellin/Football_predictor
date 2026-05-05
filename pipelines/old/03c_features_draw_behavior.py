"""
Pipeline 03c — Feature Engineering Draw Behavior (WhoScored Events → Gold)
===========================================================================
Construit 3 features comportementales spécifiques au Draw, exploitant
la granularité temporelle de l'event stream WhoScored.

FEATURES (Bloc H — Draw Behavior)
──────────────────────────────────
  H1 — ws_late_equalizer_rate
       Fréquence à laquelle l'équipe égalise APRÈS la 70ème minute
       quand elle était menée. Mesure le caractère offensif tardif
       et le pressing de fin de match.
       Signal Draw : une équipe qui revient souvent → tend vers le nul
       plutôt que la défaite.

  H2 — ws_post_yellowcard_concede_rate
       Proportion de matchs où l'équipe concède un but dans les 10 minutes
       suivant l'un de ses cartons jaunes. Mesure la fragilité défensive
       sous pression disciplinaire.
       Signal Draw : une équipe fragilisée après un jaune peut laisser
       s'échapper une victoire et finir sur un nul.

  H3 — ws_post_redcard_resilience
       Ratio (actions offensives dans les 10 min APRÈS un rouge reçu) /
       (actions offensives dans les 10 min AVANT ce rouge).
       > 1 → l'équipe réagit positivement en infériorité numérique
       < 1 → l'équipe s'effondre
       Signal Draw : une équipe résiliente en infériorité peut tenir 1-1
       plutôt que de s'effondrer à 2-0 ou 3-0.

ANTI-LEAKAGE
────────────
  ⚠️  Ces features sont des rolling moyennes sur les W derniers matchs
  joués AVANT le match à prédire. Jointure via LAG(1) identique à 03b.
  Aucune information du match courant n'est utilisée.

ARCHITECTURE
────────────
  Passe 1 — tmp_events_flat déjà disponible (chargé par 03b)
            Si absent (run autonome), on le recrée depuis stg_whoscored_events.
  Passe 2 — Calcul des métriques brutes par (ws_match_id, team_id)
            via 3 CTEs dédiées (goals_timeline, card_timeline, resilience)
  Passe 3 — Pivot home/away + jointure features_training avec LAG(1)
  Passe 4 — UPDATE gold.features_training

Usage :
    python pipelines/03c_features_draw_behavior.py
    python pipelines/03c_features_draw_behavior.py --reset-cols
    python pipelines/03c_features_draw_behavior.py --coverage-only
    python pipelines/03c_features_draw_behavior.py --window 10
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import duckdb
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
os.chdir(Path(__file__).resolve().parent.parent)

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = Path(CFG["paths"]["db"])

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_draw_behavior.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

WINDOW = CFG.get("features", {}).get("form_window", 5)

# ── Colonnes ajoutées à gold.features_training ───────────────────────────────
NEW_COLS_H = [
    # Bloc H — Draw Behavior (v3)
    ("ws_late_equalizer_rate",          "DOUBLE"),  # % matchs avec égalisateur >70min quand menés
    ("ws_post_yellowcard_concede_rate",  "DOUBLE"),  # % matchs où on concède dans les 10min après un jaune
    ("ws_post_redcard_resilience",       "DOUBLE"),  # ratio actions offensives post/pré rouge reçu
]

# ── WhoScored type_id utilisés ────────────────────────────────────────────────
# type_id = 16, outcome_id = 1, is_shot = TRUE → But
# type_id = 17 → Carton (jaune et rouge via qualifier)
# Qualifier carton : valeur "Yellow" ou "Red" dans qual_type_name
# NB : on utilise type_name directement ('Card') car plus fiable que type_id seul
# selon les versions WhoScored


# ══════════════════════════════════════════════════════════════════════════════
# PASSE 1 — Table d'events plate (identique à 03b)
# ══════════════════════════════════════════════════════════════════════════════

SQL_EVENTS_FLAT_03C = """
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
    type_id,
    type_name,
    outcome_id,
    is_touch,
    is_shot,
    qualifiers_json,
    row_num
FROM silver.stg_whoscored_events
;
"""

# ══════════════════════════════════════════════════════════════════════════════
# PASSE 2 — Features brutes par (ws_match_id, team_id)
# ══════════════════════════════════════════════════════════════════════════════

SQL_DRAW_FEATURES = """
CREATE OR REPLACE TEMP TABLE tmp_draw_features AS
WITH

-- ────────────────────────────────────────────────────────────────────────────
-- H1 — Late Equalizer Rate
-- ────────────────────────────────────────────────────────────────────────────
-- Détection : l'équipe était-elle menée en 2ème mi-temps (après 70min) ET
-- a-t-elle marqué un but égalisateur entre la 70ème minute et la fin ?
--
-- Étape 1 : timeline des buts par match (chronologique)
-- Pour chaque but, on reconstitue le score cumulé domicile/extérieur
-- en utilisant une fenêtre analytique sur la minute.
-- ────────────────────────────────────────────────────────────────────────────
goals_raw AS (
    -- Tous les buts du match (hors CSC pour simplifier)
    -- type_id = 16 + outcome_id = 1 + is_shot = TRUE = but
    SELECT
        ws_match_id,
        team_id,
        minute,
        expanded_minute,
        -- Identifier l'autre équipe dans le match
        -- (nécessaire pour reconstituer le score adverse)
        1 AS is_goal
    FROM tmp_events_flat
    WHERE is_shot = TRUE
      AND type_id  = 16
      AND outcome_id = 1
),

-- Score cumulé à chaque but (point de vue domicile/extérieur)
score_timeline AS (
    SELECT
        g.ws_match_id,
        g.team_id,
        g.expanded_minute,
        SUM(CASE WHEN g2.team_id  = g.team_id  THEN g2.is_goal ELSE 0 END) AS goals_for_cumul,
        SUM(CASE WHEN g2.team_id != g.team_id  THEN g2.is_goal ELSE 0 END) AS goals_against_cumul
    FROM goals_raw g
    JOIN goals_raw g2
        ON  g2.ws_match_id     = g.ws_match_id
        AND g2.expanded_minute <= g.expanded_minute
    GROUP BY g.ws_match_id, g.team_id, g.expanded_minute
),

-- Détection égalisateur après 70min quand on était menés
late_equalizer_raw AS (
    SELECT
        ws_match_id,
        team_id,
        -- Ce but est un égalisateur tardif si :
        -- 1. Il intervient après la 70ème minute
        -- 2. Juste avant ce but, on était menés (goals_for < goals_against)
        -- 3. Après ce but, on est à égalité (goals_for = goals_against)
        CASE WHEN
            expanded_minute >= 70
            AND goals_against_cumul  >  (goals_for_cumul - 1)  -- on était menés
            AND goals_for_cumul      =   goals_against_cumul    -- on vient d'égaliser
        THEN 1 ELSE 0 END AS is_late_equalizer
    FROM score_timeline
),

late_equalizer_per_match AS (
    SELECT
        ws_match_id,
        team_id,
        MAX(is_late_equalizer) AS had_late_equalizer  -- 1 si au moins 1 égalisateur tardif
    FROM late_equalizer_raw
    GROUP BY ws_match_id, team_id
),

-- ────────────────────────────────────────────────────────────────────────────
-- H2 — Post Yellow Card Concede Rate
-- ────────────────────────────────────────────────────────────────────────────
-- Pour chaque carton JAUNE reçu par l'équipe, on regarde si l'adversaire
-- marque dans les 10 minutes suivantes.
-- Résultat : 1 si au moins un but adverse dans les 10min, 0 sinon.
-- On moyenne ensuite sur les W derniers matchs.
-- ────────────────────────────────────────────────────────────────────────────
yellow_cards AS (
    SELECT
        ws_match_id,
        team_id,
        minute        AS card_minute,
        expanded_minute AS card_expanded_minute
    FROM tmp_events_flat
    WHERE type_name = 'Card'
      -- Carton jaune : qualifier type displayName = 'Yellow'
      -- On filtre via le JSON directement sur qualifiers_json
    AND json_extract_string(qualifiers_json, '$[0].type.displayName') = 'Yellow'
),

goals_conceded_after_yellow AS (
    SELECT
        yc.ws_match_id,
        yc.team_id,
        yc.card_expanded_minute,
        -- Y a-t-il un but de l'adversaire dans les 10 min suivantes ?
        MAX(CASE WHEN g.ws_match_id IS NOT NULL THEN 1 ELSE 0 END) AS conceded_after_yellow
    FROM yellow_cards yc
    LEFT JOIN goals_raw g
        ON  g.ws_match_id      = yc.ws_match_id
        AND g.team_id          != yc.team_id
        AND g.expanded_minute   > yc.card_expanded_minute
        AND g.expanded_minute  <= yc.card_expanded_minute + 10
    GROUP BY yc.ws_match_id, yc.team_id, yc.card_expanded_minute
),

post_yellow_per_match AS (
    SELECT
        ws_match_id,
        team_id,
        -- 1 si au moins un carton jaune a été suivi d'un but adverse
        MAX(conceded_after_yellow) AS conceded_after_any_yellow,
        -- Flag : l'équipe a-t-elle reçu au moins un carton jaune dans ce match ?
        1 AS had_yellow_card
    FROM goals_conceded_after_yellow
    GROUP BY ws_match_id, team_id
),

-- ────────────────────────────────────────────────────────────────────────────
-- H3 — Post Red Card Resilience
-- ────────────────────────────────────────────────────────────────────────────
-- Après un carton rouge reçu, compare :
--   - les actions offensives (touches en x > 50) dans les 10 min AVANT
--   - les actions offensives dans les 10 min APRÈS
-- ratio > 1 → l'équipe maintient sa pression offensive malgré le rouge
-- ratio < 1 → l'équipe recule et subit
-- Si pas de rouge dans le match → NULL (sera ignoré dans la rolling moyenne)
-- ────────────────────────────────────────────────────────────────────────────
red_cards AS (
    SELECT
        ws_match_id,
        team_id,
        expanded_minute AS red_minute
    FROM tmp_events_flat
    WHERE type_name = 'Card'
    AND json_extract_string(qualifiers_json, '$[0].type.displayName') IN ('Red', 'SecondYellow')
),

offensive_touches AS (
    SELECT ws_match_id, team_id, expanded_minute
    FROM tmp_events_flat
    WHERE is_touch = TRUE
      AND x > 50
),

offensive_actions_window AS (
    SELECT
        rc.ws_match_id,
        rc.team_id,
        rc.red_minute,
        COUNT(CASE 
            WHEN e.expanded_minute >= rc.red_minute - 10 
             AND e.expanded_minute <  rc.red_minute 
            THEN 1 ELSE NULL 
        END) AS off_actions_before,
        COUNT(CASE 
            WHEN e.expanded_minute >  rc.red_minute 
             AND e.expanded_minute <= rc.red_minute + 10 
            THEN 1 ELSE NULL 
        END) AS off_actions_after
    FROM red_cards rc
    INNER JOIN offensive_touches e
        ON  e.ws_match_id = rc.ws_match_id
        AND e.team_id     = rc.team_id
        AND e.expanded_minute BETWEEN rc.red_minute - 10 AND rc.red_minute + 10
    GROUP BY rc.ws_match_id, rc.team_id, rc.red_minute
),

post_red_per_match AS (
    SELECT
        ws_match_id,
        team_id,
        -- Ratio moyen si plusieurs rouges dans le match (rare)
        AVG(
            CASE
                WHEN off_actions_before > 0
                THEN CAST(off_actions_after AS DOUBLE) / off_actions_before
                -- Pas d'actions avant → on regarde juste si actif après
                WHEN off_actions_after  > 0 THEN 1.0
                ELSE NULL
            END
        ) AS resilience_ratio
    FROM offensive_actions_window
    GROUP BY ws_match_id, team_id
),

-- ────────────────────────────────────────────────────────────────────────────
-- Assemblage final par (ws_match_id, team_id)
-- ────────────────────────────────────────────────────────────────────────────
-- On part de la liste unique des (ws_match_id, team_id) présents dans
-- tmp_events_flat comme base, pour ne pas perdre les matchs sans carton/but.
all_teams AS (
    SELECT DISTINCT ws_match_id, team_id
    FROM tmp_events_flat
)

SELECT
    at.ws_match_id,
    at.team_id,

    -- H1 : Late Equalizer (NULL si équipe n'a jamais été menée → 0)
    COALESCE(le.had_late_equalizer, 0)          AS had_late_equalizer,

    -- H2 : Post Yellow Concede (NULL si pas de carton jaune dans ce match)
    py.conceded_after_any_yellow                AS conceded_after_yellow,
    py.had_yellow_card                          AS had_yellow_card,

    -- H3 : Post Red Resilience (NULL si pas de carton rouge)
    pr.resilience_ratio                         AS red_card_resilience

FROM all_teams at
LEFT JOIN late_equalizer_per_match le
    ON  at.ws_match_id = le.ws_match_id
    AND at.team_id     = le.team_id
LEFT JOIN post_yellow_per_match py
    ON  at.ws_match_id = py.ws_match_id
    AND at.team_id     = py.team_id
LEFT JOIN post_red_per_match pr
    ON  at.ws_match_id = pr.ws_match_id
    AND at.team_id     = pr.team_id
;
"""

# ══════════════════════════════════════════════════════════════════════════════
# PASSE 3 — Pivot home/away (même pattern que 03b)
# ══════════════════════════════════════════════════════════════════════════════

SQL_PIVOT_DRAW = """
CREATE OR REPLACE TEMP TABLE tmp_pivot_draw AS
WITH
match_meta AS (
    SELECT
        ws_match_id,
        home_team_id,
        away_team_id,
        match_date,
        home_team_name,
        away_team_name,
        season
    FROM silver.stg_whoscored_match_index
)

SELECT
    m.ws_match_id,
    m.season,
    m.match_date,
    m.home_team_name,
    m.away_team_name,

    -- Home
    h.had_late_equalizer        AS home_had_late_equalizer,
    h.conceded_after_yellow     AS home_conceded_after_yellow,
    h.had_yellow_card           AS home_had_yellow_card,
    h.red_card_resilience       AS home_red_card_resilience,

    -- Away
    a.had_late_equalizer        AS away_had_late_equalizer,
    a.conceded_after_yellow     AS away_conceded_after_yellow,
    a.had_yellow_card           AS away_had_yellow_card,
    a.red_card_resilience       AS away_red_card_resilience

FROM match_meta m
LEFT JOIN tmp_draw_features h
    ON  m.ws_match_id  = h.ws_match_id
    AND m.home_team_id = h.team_id
LEFT JOIN tmp_draw_features a
    ON  m.ws_match_id  = a.ws_match_id
    AND m.away_team_id = a.team_id
;
"""

# ══════════════════════════════════════════════════════════════════════════════
# PASSE 4 — Vue team-centric + anti-leakage LAG(1) + UPDATE features_training
# ══════════════════════════════════════════════════════════════════════════════

SQL_TEAM_HISTORY_DRAW = """
CREATE OR REPLACE TEMP TABLE tmp_draw_team_history AS
WITH
home_side AS (
    SELECT
        p.match_date                                AS ws_date,
        p.season                                    AS ws_season,
        COALESCE(tm.canonical_name, p.home_team_name) AS team_name,
        p.home_had_late_equalizer                   AS had_late_equalizer,
        p.home_conceded_after_yellow                AS conceded_after_yellow,
        p.home_had_yellow_card                      AS had_yellow_card,
        p.home_red_card_resilience                  AS red_card_resilience
    FROM tmp_pivot_draw p
    LEFT JOIN tmp_team_mapping tm
        ON p.home_team_name = tm.raw_name
    WHERE p.home_team_name IS NOT NULL
),
away_side AS (
    SELECT
        p.match_date,
        p.season,
        COALESCE(tm.canonical_name, p.away_team_name) AS team_name,
        p.away_had_late_equalizer                   AS had_late_equalizer,
        p.away_conceded_after_yellow                AS conceded_after_yellow,
        p.away_had_yellow_card                      AS had_yellow_card,
        p.away_red_card_resilience                  AS red_card_resilience
    FROM tmp_pivot_draw p
    LEFT JOIN tmp_team_mapping tm
        ON p.away_team_name = tm.raw_name
    WHERE p.away_team_name IS NOT NULL
)
SELECT * FROM home_side
UNION ALL
SELECT * FROM away_side
;
"""


def add_columns(conn: duckdb.DuckDBPyConnection) -> None:
    """Ajoute les 3 colonnes H à gold.features_training si absentes."""
    for col_name, col_type in NEW_COLS_H:
        try:
            conn.execute(f"""
                ALTER TABLE gold.features_training
                ADD COLUMN IF NOT EXISTS {col_name} {col_type}
            """)
            logger.debug(f"  Colonne {col_name} vérifiée/ajoutée")
        except Exception as e:
            logger.warning(f"  ALTER TABLE : {e}")


def reset_columns(conn: duckdb.DuckDBPyConnection) -> None:
    """Remet à NULL les 3 colonnes (pour recalcul propre)."""
    for col_name, _ in NEW_COLS_H:
        try:
            conn.execute(
                f"UPDATE gold.features_training SET {col_name} = NULL"
            )
            logger.info(f"  {col_name} remis à NULL")
        except Exception as e:
            logger.warning(f"  Reset {col_name} : {e}")


def inject_team_mapping(conn: duckdb.DuckDBPyConnection) -> None:
    """Charge le team_mapping depuis config.yaml dans tmp_team_mapping."""
    import pandas as pd
    _raw: dict = CFG.get("team_mapping", {})
    rows = [(str(k), str(v)) for k, v in _raw.items() if k and v]
    df   = pd.DataFrame(rows, columns=["raw_name", "canonical_name"])
    conn.register("_df_tm", df)
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE tmp_team_mapping AS
        SELECT raw_name, canonical_name FROM _df_tm
    """)
    conn.unregister("_df_tm")
    n = conn.execute("SELECT COUNT(*) FROM tmp_team_mapping").fetchone()[0]
    logger.info(f"  tmp_team_mapping : {n:,} entrées")


def run_update(conn: duckdb.DuckDBPyConnection, window: int) -> int:
    """
    Anti-leakage LAG(1) + rolling moyenne sur W matchs + UPDATE.
    Retourne le nombre de lignes mises à jour.
    """
    logger.info(f"  Rolling moyenne sur W={window} matchs + UPDATE...")

    # Étape 1 : rolling moyenne par équipe sur W matchs
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_draw_rolling AS
        WITH ranked AS (
            SELECT
                ft.team                             AS team,
                ft.date                             AS ft_date,
                wsh.ws_date,
                wsh.had_late_equalizer,
                wsh.conceded_after_yellow,
                wsh.had_yellow_card,
                wsh.red_card_resilience,
                ROW_NUMBER() OVER (
                    PARTITION BY ft.team, ft.date
                    ORDER BY wsh.ws_date DESC
                ) AS rn
            FROM gold.features_training ft
            JOIN tmp_draw_team_history wsh
                ON  ft.team     = wsh.team_name
                AND wsh.ws_date  < ft.date          -- anti-leakage strict
                AND ft.season   = wsh.ws_season
        )
        SELECT
            team,
            ft_date,
            -- H1 : moyenne des W derniers matchs (toujours défini car on sait
            --      si l'équipe était menée ou non dans chaque match)
            AVG(had_late_equalizer) FILTER (WHERE rn <= {window})
                AS ws_late_equalizer_rate,

            -- H2 : moyenne uniquement sur les matchs où l'équipe avait un jaune
            --      (évite de diluer avec les matchs sans carton)
            AVG(conceded_after_yellow) FILTER (
                WHERE rn <= {window} AND had_yellow_card = 1
            )                           AS ws_post_yellowcard_concede_rate,

            -- H3 : moyenne uniquement sur les matchs avec rouge
            --      (NULL si aucun rouge dans la fenêtre → pas de mise à jour)
            AVG(red_card_resilience) FILTER (
                WHERE rn <= {window} AND red_card_resilience IS NOT NULL
            )                           AS ws_post_redcard_resilience

        FROM ranked
        WHERE rn <= {window}
        GROUP BY team, ft_date
    """)

    n_roll = conn.execute("SELECT COUNT(*) FROM tmp_draw_rolling").fetchone()[0]
    logger.info(f"  {n_roll:,} lignes dans tmp_draw_rolling")

    # Étape 2 : UPDATE final
    conn.execute("""
        UPDATE gold.features_training AS ft
        SET
            ws_late_equalizer_rate         = dr.ws_late_equalizer_rate,
            ws_post_yellowcard_concede_rate = dr.ws_post_yellowcard_concede_rate,
            ws_post_redcard_resilience      = dr.ws_post_redcard_resilience
        FROM tmp_draw_rolling dr
        WHERE ft.team = dr.team
          AND ft.date = dr.ft_date
    """)

    n_updated = conn.execute("""
        SELECT COUNT(*) FROM gold.features_training
        WHERE ws_late_equalizer_rate IS NOT NULL
    """).fetchone()[0]
    logger.info(f"  {n_updated:,} lignes mises à jour dans gold.features_training")
    return n_updated


def print_coverage(conn: duckdb.DuckDBPyConnection) -> None:
    """Rapport de couverture sur les 3 nouvelles colonnes."""
    logger.info("═══ Rapport de couverture — 03c ═══")
    total = conn.execute(
        "SELECT COUNT(*) FROM gold.features_training"
    ).fetchone()[0]
    logger.info(f"  gold.features_training : {total:,} lignes totales")

    for col_name, _ in NEW_COLS_H:
        try:
            n_ok = conn.execute(
                f"SELECT COUNT(*) FROM gold.features_training "
                f"WHERE {col_name} IS NOT NULL"
            ).fetchone()[0]
            pct    = n_ok / total * 100 if total else 0
            status = "✅" if pct > 50 else ("⚠️ " if pct > 10 else "❌")
            logger.info(
                f"  {status} {col_name:<40} : "
                f"{n_ok:>7,}/{total:,} ({pct:.1f}%)"
            )
            # Valeurs moyennes (sanity check)
            stats = conn.execute(f"""
                SELECT AVG({col_name}), MIN({col_name}), MAX({col_name})
                FROM gold.features_training
                WHERE {col_name} IS NOT NULL
            """).fetchone()
            if stats and stats[0] is not None:
                logger.info(
                    f"       mean={stats[0]:.4f}  "
                    f"min={stats[1]:.4f}  "
                    f"max={stats[2]:.4f}"
                )
        except Exception as e:
            logger.warning(f"  {col_name} : erreur coverage ({e})")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    reset_cols:    bool = False,
    coverage_only: bool = False,
    window:        int  = WINDOW,
) -> None:
    logger.info("═══ Pipeline 03c — Draw Behavior Features ═══")

    if not DB_PATH.exists():
        logger.error(f"DuckDB introuvable : {DB_PATH}")
        raise FileNotFoundError(DB_PATH)

    _tmp_dir = Path(tempfile.gettempdir()) / "duckdb_03c_tmp"
    _tmp_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(f"SET temp_directory='{_tmp_dir.as_posix()}'")

    # ── Prérequis ─────────────────────────────────────────────────────────────
    try:
        n_events = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_events"
        ).fetchone()[0]
        n_index  = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_match_index"
        ).fetchone()[0]
        n_train  = conn.execute(
            "SELECT COUNT(*) FROM gold.features_training"
        ).fetchone()[0]
        logger.info(f"  stg_whoscored_events  : {n_events:,} événements")
        logger.info(f"  stg_whoscored_match_index : {n_index:,} matchs")
        logger.info(f"  gold.features_training    : {n_train:,} lignes")
    except Exception as e:
        logger.error(f"  Prérequis manquant : {e}")
        conn.close()
        raise

    if n_events == 0:
        logger.warning("  Aucun événement WhoScored — pipeline 03c ignoré")
        conn.close()
        return

    # ── Mode coverage only ────────────────────────────────────────────────────
    if coverage_only:
        print_coverage(conn)
        conn.close()
        return

    # ── Ajout des colonnes ────────────────────────────────────────────────────
    add_columns(conn)
    if reset_cols:
        reset_columns(conn)

    # ── Passe 1 — Events flat ─────────────────────────────────────────────────
    # On vérifie si tmp_events_flat existe déjà (lancé après 03b dans le même
    # process). Sinon, on le recrée.
    try:
        n_flat = conn.execute("SELECT COUNT(*) FROM tmp_events_flat").fetchone()[0]
        logger.info(f"  tmp_events_flat déjà disponible : {n_flat:,} événements")
    except Exception:
        logger.info("  Passe 1 — Création de tmp_events_flat...")
        conn.execute(SQL_EVENTS_FLAT_03C)
        n_flat = conn.execute("SELECT COUNT(*) FROM tmp_events_flat").fetchone()[0]
        logger.info(f"  {n_flat:,} événements chargés")

    # ── Passe 2 — Features brutes Draw ───────────────────────────────────────
    logger.info("  Passe 2 — Calcul features Draw (H1/H2/H3)...")
    conn.execute(SQL_DRAW_FEATURES)
    n_df = conn.execute("SELECT COUNT(*) FROM tmp_draw_features").fetchone()[0]
    logger.info(f"  {n_df:,} lignes dans tmp_draw_features ({n_df // 2} matchs)")

    # ── Passe 3 — Pivot home/away ─────────────────────────────────────────────
    logger.info("  Passe 3 — Pivot home/away...")
    conn.execute(SQL_PIVOT_DRAW)
    n_piv = conn.execute("SELECT COUNT(*) FROM tmp_pivot_draw").fetchone()[0]
    logger.info(f"  {n_piv:,} matchs dans tmp_pivot_draw")

    # ── Team mapping ──────────────────────────────────────────────────────────
    # Réutilise tmp_team_mapping si déjà créé par 03b, sinon l'injecte.
    try:
        conn.execute("SELECT COUNT(*) FROM tmp_team_mapping")
        logger.info("  tmp_team_mapping déjà disponible (chargé par 03b)")
    except Exception:
        inject_team_mapping(conn)

    # ── Vue team-centric ──────────────────────────────────────────────────────
    logger.info("  Création de tmp_draw_team_history...")
    conn.execute(SQL_TEAM_HISTORY_DRAW)
    n_hist = conn.execute(
        "SELECT COUNT(*) FROM tmp_draw_team_history"
    ).fetchone()[0]
    logger.info(f"  {n_hist:,} lignes dans l'historique team-centric")

    # ── Passe 4 — Rolling moyenne + UPDATE ───────────────────────────────────
    run_update(conn, window)

    # ── Rapport de couverture ─────────────────────────────────────────────────
    print_coverage(conn)

    conn.close()
    logger.success("═══ Pipeline 03c terminé ═══")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering Draw Behavior (03c)"
    )
    parser.add_argument(
        "--reset-cols",
        action="store_true",
        help="Remet à NULL les 3 colonnes ws_draw_* avant recalcul",
    )
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Affiche uniquement le rapport de couverture",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=WINDOW,
        help=f"Fenêtre rolling (défaut: {WINDOW} matchs)",
    )
    args = parser.parse_args()

    run_pipeline(
        reset_cols=args.reset_cols,
        coverage_only=args.coverage_only,
        window=args.window,
    )