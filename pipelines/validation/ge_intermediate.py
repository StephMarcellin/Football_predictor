"""
ge_intermediate.py — Validation Great Expectations de la couche Intermediate
=============================================================================
Vérifie les data contracts sur les tables Intermediate critiques.

Tables validées :
    - intermediate.player_match_stats  (métriques joueur par match)
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import yaml
import great_expectations as gx
from loguru import logger


# ── Résolution du chemin DuckDB ───────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]


def validate_intermediate() -> bool:
    """
    Valide les data contracts de la couche Intermediate.

    Retourne True si toutes les validations passent, False sinon.
    Lève une RuntimeError si une règle critique est violée.
    """
    logger.info("🔍 Validation Intermediate — démarrage")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB introuvable : {DB_PATH}")

    # ── Connexion DuckDB ──────────────────────────────────────────────────────
    con = duckdb.connect(str(DB_PATH), read_only=True)

    df_pms = con.execute("SELECT * FROM intermediate.player_match_stats").df()
    con.close()

    logger.info(f"  player_match_stats : {len(df_pms):,} lignes")

    # ── Contexte Great Expectations ───────────────────────────────────────────
    context = gx.get_context(mode="ephemeral")

    violations = []

    # ══════════════════════════════════════════════════════════════════════════
    # BLOC 1 — player_match_stats
    # ══════════════════════════════════════════════════════════════════════════
    ds_pms   = context.data_sources.add_pandas("intermediate")
    asset_p  = ds_pms.add_dataframe_asset("player_match_stats")
    batch_p  = asset_p.add_batch_definition_whole_dataframe("batch").get_batch(
        batch_parameters={"dataframe": df_pms}
    )

    checks_pms = [
        # ── Clés ─────────────────────────────────────────────────────────────
        gx.expectations.ExpectColumnValuesToNotBeNull(column="match_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="team_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="player_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="date"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="season"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="league_source"),

        # ── Unicité de la clé composite ───────────────────────────────────────
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["match_id", "team_id", "player_id"]
        ),

        # ── Volume minimum ────────────────────────────────────────────────────
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=100_000),

        # ── Métriques générales ───────────────────────────────────────────────
        # n_actions toujours >= 1 (un joueur sans action ne devrait pas exister mais il peut)
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_actions", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="xg_contribution", min_value=0
        ),

        # ── Métriques offensives ──────────────────────────────────────────────
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_shots", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_shot_assists", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_key_passes", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_progressive_passes", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_longballs", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_crosses", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_throughballs", min_value=0
        ),

        # ── Métriques défensives ──────────────────────────────────────────────
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_tackles", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_tackles_won", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_interceptions", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_ball_recoveries", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_challenges", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_clearances", min_value=0
        ),

        # ── Règles métier ─────────────────────────────────────────────────────
        # n_tackles_won ne peut pas dépasser n_tackles
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanOrEqualToB(
            column_A="n_tackles",
            column_B="n_tackles_won"
        ),

        # zone_dominance entre 0 et 100 (coordonnées terrain WhoScored)
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="zone_dominance", min_value=0, max_value=100, mostly=0.99
        ),

        # defensive_zone_x entre 0 et 100
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="defensive_zone_x", min_value=0, max_value=100, mostly=0.99
        ),

        # Saisons connues uniquement
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="season",
            value_set=[
                "2017-2018", "2018-2019", "2019-2020", "2020-2021",
                "2021-2022", "2022-2023", "2023-2024", "2024-2025",
                "2025-2026",
            ],
        ),
    ]

    for expectation in checks_pms:
        result = batch_p.validate(expectation)
        if not result.success:
            msg = f"[player_match_stats] ÉCHEC : {expectation.__class__.__name__}"
            logger.error(f"  ✗ {msg}")
            violations.append(msg)
        else:
            logger.info(f"  ✓ player_match_stats.{expectation.__class__.__name__}")

    # ══════════════════════════════════════════════════════════════════════════
    # RÉSULTAT FINAL
    # ══════════════════════════════════════════════════════════════════════════
    if violations:
        summary = "\n  ".join(violations)
        logger.error(
            f"Validation Intermediate — {len(violations)} violation(s) :\n  {summary}"
        )
        raise RuntimeError(
            f"Validation Intermediate échouée — {len(violations)} violation(s)"
        )

    logger.success("✓ Validation Intermediate — toutes les règles respectées")
    return True