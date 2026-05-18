"""
ge_silver.py — Validation Great Expectations de la couche Silver
================================================================
Vérifie les data contracts sur les tables Silver critiques avant
que dbt commence le feature engineering Gold.

Tables validées :
    - silver.understat_schedule  (xG, résultats)
    - silver.fbref_schedule      (formations, result_1n2)
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import yaml
import great_expectations as gx
from loguru import logger


# ── Résolution du chemin DuckDB ───────────────────────────────────────────────
# Ce fichier est dans pipelines/validation/ → la racine est deux niveaux au-dessus
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]


def validate_silver() -> bool:
    """
    Valide les data contracts de la couche Silver.

    Retourne True si toutes les validations passent, False sinon.
    Lève une RuntimeError si une règle critique est violée.
    """
    logger.info("🔍 Validation Silver — démarrage")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB introuvable : {DB_PATH}")

    # ── Connexion DuckDB et chargement des tables ─────────────────────────────
    con = duckdb.connect(str(DB_PATH), read_only=True)

    df_understat = con.execute("SELECT * FROM silver.understat_schedule").df()
    df_fbref     = con.execute("SELECT * FROM silver.fbref_schedule").df()
    con.close()

    logger.info(f"  understat_schedule : {len(df_understat):,} lignes")
    logger.info(f"  fbref_schedule     : {len(df_fbref):,} lignes")

    # ── Contexte Great Expectations (in-memory, sans fichiers de config) ──────
    context = gx.get_context(mode="ephemeral")

    violations = []

    # ══════════════════════════════════════════════════════════════════════════
    # BLOC 1 — understat_schedule
    # ══════════════════════════════════════════════════════════════════════════
    # La connexion. "Je vais travailler avec des DataFrames pandas."
    ds_understat = context.data_sources.add_pandas("understat")

    # La table logique. "Cette connexion expose une table appelée understat_schedule."
    asset_u      = ds_understat.add_dataframe_asset("understat_schedule")

    # Le lot de données concret à valider. "Voici les données réelles à inspecter maintenant."
    batch_u      = asset_u.add_batch_definition_whole_dataframe("batch").get_batch(
        batch_parameters={"dataframe": df_understat}
    )

    checks_understat = [
        # Colonnes xG jamais NULL
        gx.expectations.ExpectColumnValuesToNotBeNull(column="home_xg", mostly=0.80),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="away_xg", mostly=0.80),
        # xG toujours positif ou nul
        gx.expectations.ExpectColumnValuesToBeBetween(column="home_xg", min_value=0),
        gx.expectations.ExpectColumnValuesToBeBetween(column="away_xg", min_value=0),
        # Volume minimum — détecte une perte massive
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=5000),
        # Saisons connues uniquement
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="season",
            value_set=[
                "2017-2018","2018-2019","2019-2020","2020-2021",
                "2021-2022","2022-2023","2023-2024","2024-2025",
                "2025-2026",
            ],
        ),
    ]

    for expectation in checks_understat:
        result = batch_u.validate(expectation)
        if not result.success:
            msg = f"[understat_schedule] ÉCHEC : {expectation.__class__.__name__}"
            logger.error(f"  ✗ {msg}")
            violations.append(msg)
        else:
            logger.info(f"  ✓ understat_schedule.{expectation.__class__.__name__}")

    # ══════════════════════════════════════════════════════════════════════════
    # BLOC 2 — fbref_schedule
    # ══════════════════════════════════════════════════════════════════════════
    ds_fbref = context.data_sources.add_pandas("fbref")
    asset_f  = ds_fbref.add_dataframe_asset("fbref_schedule")
    batch_f  = asset_f.add_batch_definition_whole_dataframe("batch").get_batch(
        batch_parameters={"dataframe": df_fbref}
    )

    checks_fbref = [
        # Variable cible — valeurs strictement contrôlées
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="result_1n2",
            value_set=["H", "D", "A"],
        ),
        # Date jamais NULL — clé des features rolling
        gx.expectations.ExpectColumnValuesToNotBeNull(column="date"),
        # Team jamais NULL — clé de jointure
        gx.expectations.ExpectColumnValuesToNotBeNull(column="team"),
    ]

    for expectation in checks_fbref:
        result = batch_f.validate(expectation)
        if not result.success:
            msg = f"[fbref_schedule] ÉCHEC : {expectation.__class__.__name__}"
            logger.error(f"  ✗ {msg}")
            violations.append(msg)
        else:
            logger.info(f"  ✓ fbref_schedule.{expectation.__class__.__name__}")

    # ══════════════════════════════════════════════════════════════════════════
    # RÉSULTAT FINAL
    # ══════════════════════════════════════════════════════════════════════════
    if violations:
        summary = "\n  ".join(violations)
        logger.error(f"Validation Silver — {len(violations)} violation(s) :\n  {summary}")
        raise RuntimeError(f"Validation Silver échouée — {len(violations)} violation(s)")

    logger.success("✓ Validation Silver — toutes les règles respectées")
    return True