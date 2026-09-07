#!/usr/bin/env python3
"""
run_ml.py — Orchestrateur PHASE 4 : ML Training
=================================================
Entraîne les modèles de prédiction dans l'ordre imposé par le stacking :

    1. train_goals_oof   : Expanding Window OOF → λ par match historique
                           (placeholder : appelle train_goals.main() classique
                           tant que l'OOF n'est pas implémenté)
    2. train_goals       : Entraînement final sur toutes les données
                           → models/buts_equipe.joblib (inférence production)
    3. train_1n2         : Multiclasse H/D/A, consomme les λ OOF comme features
                           (quand le stacking sera actif)
    4. train_buteurs     : Binaire buteur (mart_scorers)
    5. train_passeurs    : Binaire passeur (mart_assists)

Modèles non inclus :
    - evenements : trainer pas encore écrit
    - predict / backtest / agent : futur run_predict.py (cadence différente)

Sortie : models/*.joblib + MLflow experiments
Durée  : ~15-30min (dépend de la taille du mart)

Usage :
    python run_ml.py                          # pipeline complet
    python run_ml.py --step train_1n2         # étape unique
    python run_ml.py --from train_buteurs     # reprend depuis
    python run_ml.py --dry-run                # simule sans exécuter
    python run_ml.py --list                   # affiche les étapes
"""

from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import argparse
import sys
from pathlib import Path

from prefect import flow
from loguru import logger

from orchestrator_common import (
    ROOT_DIR,
    load_config,
    import_from_path,
    make_run_step_task,
    execute_steps,
    print_summary,
)

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Logger fichier ───────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/ml_training.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [ML] {message}",
)


# ══════════════════════════════════════════════════════════════════════════════
# Chemins des modules d'entraînement
# ══════════════════════════════════════════════════════════════════════════════

MOD_TRAIN_GOALS   = ROOT_DIR / "pipelines" / "models" / "train_goals.py"
MOD_TRAIN_1N2     = ROOT_DIR / "pipelines" / "models" / "train_1n2.py"
MOD_TRAIN_PLAYERS = ROOT_DIR / "pipelines" / "models" / "train_players.py"


# ══════════════════════════════════════════════════════════════════════════════
# Noms d'étapes (l'ordre compte : stacking goals → 1n2)
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = [
    "train_goals_oof",
    "train_goals",
    "train_1n2",
    "train_buteurs",
    "train_passeurs",
]


# ══════════════════════════════════════════════════════════════════════════════
# Construction des étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict) -> dict:
    """
    Construit le dictionnaire ordonné des étapes d'entraînement ML.

    L'ordre reflète le stacking :
        1. goals OOF génère les λ pour chaque match historique
        2. goals final entraîne le modèle de production
        3. 1n2 consomme les λ OOF comme features additionnelles
        4-5. buteurs/passeurs sont indépendants

    Les imports sont différés (pas en tête de module) pour ne charger
    les dépendances lourdes (LightGBM, SHAP, etc.) qu'à l'exécution.
    """
    mod_goals   = import_from_path("train_goals",   MOD_TRAIN_GOALS)
    mod_1n2     = import_from_path("train_1n2",     MOD_TRAIN_1N2)
    mod_players = import_from_path("train_players", MOD_TRAIN_PLAYERS)

    # Hyperparamètres depuis config.yaml si présents, sinon défauts des trainers
    ml_cfg = cfg.get("ml", {})

    return {
        # ── Stacking étape 1 : OOF goals ────────────────────────────────────
        # Placeholder : appelle train_goals.main() classique.
        # Quand l'Expanding Window sera implémenté, cette step appellera
        # une fonction dédiée (ex: mod_goals.generate_oof_predictions).
        "train_goals_oof": {
            "fn":       mod_goals.main,
            "kwargs":   {
                "n_estimators":  ml_cfg.get("goals_n_estimators", 800),
                "learning_rate": ml_cfg.get("goals_learning_rate", 0.03),
            },
            "critical": True,
        },

        # ── Stacking étape 2 : goals final (production) ────────────────────
        # Entraîne sur TOUTES les données pour l'inférence.
        # Pour l'instant identique à l'OOF (même appel), sera différencié
        # quand l'OOF sera implémenté (OOF = folds, final = full dataset).
        "train_goals": {
            "fn":       mod_goals.main,
            "kwargs":   {
                "n_estimators":  ml_cfg.get("goals_n_estimators", 800),
                "learning_rate": ml_cfg.get("goals_learning_rate", 0.03),
            },
            "critical": True,
        },

        # ── Stacking étape 3 : 1N2 (consomme les λ OOF) ────────────────────
        "train_1n2": {
            "fn":       mod_1n2.main,
            "kwargs":   {
                "n_estimators":  ml_cfg.get("1n2_n_estimators", 600),
                "learning_rate": ml_cfg.get("1n2_learning_rate", 0.03),
            },
            "critical": True,
        },

        # ── Modèles joueurs (indépendants) ──────────────────────────────────
        "train_buteurs": {
            "fn":       mod_players.main,
            "kwargs":   {
                "model_key":     "buteurs",
                "n_estimators":  ml_cfg.get("players_n_estimators", 600),
                "learning_rate": ml_cfg.get("players_learning_rate", 0.03),
            },
            "critical": False,
        },

        "train_passeurs": {
            "fn":       mod_players.main,
            "kwargs":   {
                "model_key":     "passeurs",
                "n_estimators":  ml_cfg.get("players_n_estimators", 600),
                "learning_rate": ml_cfg.get("players_learning_rate", 0.03),
            },
            "critical": False,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="ML Training", log_prints=False)
def run_ml_flow(steps: dict, dry_run: bool = False, run_step_task=None) -> list[dict]:
    """Flow Prefect de la phase 4 (training uniquement)."""
    return execute_steps(
        steps,
        run_step_task,
        dry_run=dry_run,
        phase_name="ML TRAINING",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 4 — ML Training",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python run_ml.py                          # complet (5 étapes)
  python run_ml.py --step train_1n2         # une seule étape
  python run_ml.py --from train_buteurs     # reprend depuis buteurs
  python run_ml.py --dry-run                # simule sans exécuter
  python run_ml.py --list                   # affiche les étapes
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--step", choices=STEP_NAMES,
                       help="Exécute une seule étape")
    group.add_argument("--from", dest="from_step", choices=STEP_NAMES,
                       metavar="STEP",
                       help="Exécute depuis cette étape jusqu'à la fin")

    parser.add_argument("--dry-run", action="store_true",
                        help="Liste les étapes sans les exécuter")
    parser.add_argument("--list", action="store_true",
                        help="Affiche les étapes disponibles et quitte")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        print("\nÉtapes ML Training (dans l'ordre) :")
        for i, name in enumerate(STEP_NAMES, 1):
            print(f"  {i}. {name}")
        print()
        return

    cfg = load_config()
    pipeline_cfg = cfg.get("pipeline", {})

    run_step_task = make_run_step_task(
        retries=pipeline_cfg.get("retries", 2),
        retry_delay_seconds=pipeline_cfg.get("retry_delay_seconds", 30),
    )

    all_steps = build_steps(cfg)

    if args.step:
        steps_to_run = {args.step: all_steps[args.step]}
    elif args.from_step:
        idx = STEP_NAMES.index(args.from_step)
        steps_to_run = {n: all_steps[n] for n in STEP_NAMES[idx:]}
    else:
        steps_to_run = all_steps

    results = run_ml_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ ML TRAINING")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
