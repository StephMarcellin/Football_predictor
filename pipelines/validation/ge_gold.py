"""
ge_gold.py — Validation Great Expectations de la couche Gold
============================================================
Vérifie les data contracts sur features_final avant l'entraînement
du modèle. Un problème ici signifie que dbt a produit des features
corrompues — le modèle ne doit pas s'entraîner dans ce cas.

Tables validées :
    - gold.features_final  (features ML prêtes à l'entraînement)
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


def validate_gold() -> bool:
    """
    Valide les data contracts de la couche Gold.

    Retourne True si toutes les validations passent.
    Lève une RuntimeError si une règle critique est violée.
    """
    logger.info("🔍 Validation Gold — démarrage")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB introuvable : {DB_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    df_features = con.execute("SELECT * FROM gold.features_final").df()
    con.close()

    logger.info(f"  features_final : {len(df_features):,} lignes")

    context = gx.get_context(mode="ephemeral")
    violations = []

    ds = context.data_sources.add_pandas("gold")
    asset = ds.add_dataframe_asset("features_final")
    batch = asset.add_batch_definition_whole_dataframe("batch").get_batch(
        batch_parameters={"dataframe": df_features}
    )

    checks = [
        # Volume — détecte une perte massive lors des joins dbt
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=30000),

        # Variable cible — doit survivre intacte jusqu'à Gold
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="result_1n2",
            value_set=["H", "D", "A"],
        ),

        # Clé primaire — jamais NULL
        gx.expectations.ExpectColumnValuesToNotBeNull(column="final_match_id"),

        # Les 5 grands championnats sont toujours présents dans les données
        gx.expectations.ExpectColumnDistinctValuesToContainSet(
            column="league_source",
            value_set=[
                "Premier League",
                "Ligue 1",
                "Bundesliga",
                "Serie A",
                "La Liga",
            ],
        ),

        # Features rolling critiques — tolère NULLs début de saison (mostly=0.60)
        # mais détecte une colonne entièrement vide
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="np_xg_roll_3",
            mostly=0.60,
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="ppda_roll_3",
            mostly=0.60,
        ),
    ]

    for expectation in checks:
        result = batch.validate(expectation)
        if not result.success:
            msg = f"[features_final] ÉCHEC : {expectation.__class__.__name__} — {expectation.column if hasattr(expectation, 'column') else 'table'}"
            logger.error(f"  ✗ {msg}")
            violations.append(msg)
        else:
            col = getattr(expectation, "column", "table")
            logger.info(f"  ✓ features_final.{expectation.__class__.__name__} ({col})")

    if violations:
        summary = "\n  ".join(violations)
        logger.error(f"Validation Gold — {len(violations)} violation(s) :\n  {summary}")
        raise RuntimeError(f"Validation Gold échouée — {len(violations)} violation(s)")

    logger.success("✓ Validation Gold — toutes les règles respectées")
    return True