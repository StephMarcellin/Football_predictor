"""
Pipeline 03 — Feature Engineering (Silver DuckDB → Gold DuckDB)
================================================================
Orchestrateur du package features/.

Remplace les 5 fichiers 03_*.py par un runner unique qui délègue
à des modules spécialisés :

    features/
    ├── columns.py     — registre centralisé des listes de colonnes
    ├── rolling.py     — Staging + Rolling + Match-up (ex-03_features.py)
    ├── whoscored.py   — WhoScored events → features spatiales (ex-03b)
    ├── draw.py        — Draw Behavior + Draw Signals F1–F20 (ex-03c ×2)
    └── sandbox.py     — features candidates read-only (ex-03c_suggested)

SÉQUENCE D'EXÉCUTION
──────────────────────
  Étape 1 : rolling   → crée gold.stg_backbone, features_training, features_final
  Étape 2 : whoscored → enrichit features_training avec ws_* (Blocs A–G)
  Étape 3 : draw      → enrichit features_training avec H1–H3 + F1–F20

Chaque étape est optionnelle via --step.

Usage :
    python pipelines/03_features.py                     # run complet (1+2+3)
    python pipelines/03_features.py --step rolling      # étape 1 uniquement
    python pipelines/03_features.py --step whoscored    # étape 2 uniquement
    python pipelines/03_features.py --step draw         # étape 3 uniquement
    python pipelines/03_features.py --step sandbox      # validation candidates
    python pipelines/03_features.py --reset             # supprime gold et recrée
    python pipelines/03_features.py --reset-cols        # remet ws_*/f* à NULL
    python pipelines/03_features.py --coverage-only     # rapport sans recalcul
    python pipelines/03_features.py --window 10         # fenêtre rolling draw
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# ── Résolution du path pour les imports relatifs ──────────────────────────────
# Ce fichier peut être dans pipelines/ ou à la racine selon la structure projet.
# On ajoute le répertoire parent de features/ au sys.path pour garantir
# que `from features import ...` fonctionne quelle que soit la CWD.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

os.chdir(_HERE.parent if (_HERE / "features").exists() else _HERE)

from features.rolling   import run_rolling_features
from features.whoscored import run_pipeline as run_whoscored
from features.draw      import run_pipeline as run_draw
from features.sandbox   import run          as run_sandbox

from loguru import logger
from pathlib import Path

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_pipeline.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [03] {message}",
)


def run_full_pipeline(
    reset:         bool = False,
    reset_cols:    bool = False,
    coverage_only: bool = False,
    window:        int  = 5,
    step:          str  = "all",
) -> None:
    """
    Orchestre les 3 étapes de feature engineering.

    Args:
        reset:         Supprime et recrée le schéma gold entier (étape rolling uniquement).
        reset_cols:    Remet à NULL les colonnes ws_* et f* (étapes 2+3).
        coverage_only: N'affiche que les rapports de couverture, sans recalcul.
        window:        Fenêtre rolling pour les étapes draw (défaut : config.yaml).
        step:          Étape(s) à exécuter : "all" | "rolling" | "whoscored" | "draw" | "sandbox".
    """
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║  Pipeline 03 — Feature Engineering Silver→Gold  ║")
    logger.info(f"║  step={step:<10} reset={reset} reset_cols={reset_cols}  ║")
    logger.info(f"║  coverage_only={coverage_only}                            ║")
    logger.info("╚══════════════════════════════════════════════════╝")

    # ── Mode coverage-only : rapports sans aucun recalcul ────────────────────
    # On n'exécute pas rolling (qui recrée les tables depuis zéro et perd les
    # colonnes ws_* et f* ajoutées par les étapes suivantes).
    # On se contente d'afficher les rapports de couverture de whoscored et draw.
    if coverage_only:
        logger.info("Mode --coverage-only : rapports sans recalcul")
        if step in ("all", "whoscored"):
            run_whoscored(reset_cols=False, coverage_only=True)
        if step in ("all", "draw"):
            run_draw(reset_cols=False, coverage_only=True, window=window)
        return

    # ── Étape 1 : Rolling (FBref / Understat / WhoScored saison) ─────────────
    if step in ("all", "rolling"):
        logger.info("▶ Étape 1/3 — Rolling features (FBref + Understat + WhoScored saison)")
        run_rolling_features(reset=reset)
        logger.info("✓ Étape 1 terminée")

    # ── Étape 2 : WhoScored events → features spatiales ──────────────────────
    if step in ("all", "whoscored"):
        logger.info("▶ Étape 2/3 — WhoScored events (Blocs A–G + has_ws_events)")
        run_whoscored(reset_cols=reset_cols, coverage_only=False)
        logger.info("✓ Étape 2 terminée")

    # ── Étape 3 : Draw Behavior + Draw Signals ────────────────────────────────
    if step in ("all", "draw"):
        logger.info("▶ Étape 3/3 — Draw Behavior (H1–H3) + Draw Signals (F1–F20)")
        run_draw(
            reset_cols=reset_cols,
            coverage_only=False,
            window=window,
        )
        logger.info("✓ Étape 3 terminée")

    # ── Mode sandbox (features candidates) ───────────────────────────────────
    if step == "sandbox":
        logger.info("▶ Sandbox — Validation des features candidates (read-only)")
        run_sandbox(dry_run=True)
        logger.info("✓ Sandbox terminé")

    if step == "all":
        logger.success("╔══════════════════════════════════════════╗")
        logger.success("║  Pipeline 03 complet — Gold prêt pour 04 ║")
        logger.success("╚══════════════════════════════════════════╝")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Feature Engineering Silver → Gold (pipeline 03)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python pipelines/03_features.py                        # run complet
  python pipelines/03_features.py --step rolling         # staging seul
  python pipelines/03_features.py --step whoscored       # events seul
  python pipelines/03_features.py --step draw --window 10
  python pipelines/03_features.py --reset                # recrée gold
  python pipelines/03_features.py --coverage-only        # rapport seulement
        """,
    )
    parser.add_argument(
        "--step",
        default="all",
        choices=["all", "rolling", "whoscored", "draw", "sandbox"],
        help="Étape(s) à exécuter (défaut: all)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Supprime et recrée le schéma gold (rolling uniquement)",
    )
    parser.add_argument(
        "--reset-cols",
        action="store_true",
        help="Remet à NULL les colonnes ws_* et f* avant recalcul",
    )
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Affiche uniquement les rapports de couverture",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=5,
        help="Fenêtre rolling pour les étapes draw (défaut: 5)",
    )
    args = parser.parse_args()

    run_full_pipeline(
        reset=args.reset,
        reset_cols=args.reset_cols,
        coverage_only=args.coverage_only,
        window=args.window,
        step=args.step,
    )