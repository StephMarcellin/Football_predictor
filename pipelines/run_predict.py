#!/usr/bin/env python3
"""
run_predict.py — Orchestrateur PHASE 5 : Prédiction + Backtest
===============================================================
Génère les prédictions, marchés dérivés, backtest la stratégie et
lance l'agent d'analyse post-run. Cadence quotidienne (vs run_ml.py
qui est hebdomadaire).

    1. predict_1n2      : P(H/D/A) directes depuis le modèle multiclasse
    2. predict_ensemble : blend 1N2 direct + goals Poisson (Dixon-Coles)
    3. predict_markets  : marchés détaillés (O/U, BTTS, scores exacts)
    4. backtest_1n2     : value betting Kelly, ROI, CLV
    5. agent_gemini     : analyse post-run ReAct (non-critique, optionnel)

Prérequis : modèles entraînés (run_ml.py doit avoir tourné avant).

Sortie : reports/*.csv + machine_learning.predictions_* (si --write)

Usage :
    python run_predict.py --season 2024-2025                # saison complète
    python run_predict.py --season 2024-2025 --write        # + écriture DuckDB
    python run_predict.py --step predict_ensemble            # étape unique
    python run_predict.py --from backtest_1n2                # reprend depuis
    python run_predict.py --no-agent                         # sans agent Gemini
    python run_predict.py --dry-run                          # simule
    python run_predict.py --list                             # affiche les étapes
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
    "logs/predict.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [PREDICT] {message}",
)


# ══════════════════════════════════════════════════════════════════════════════
# Chemins des modules
# ══════════════════════════════════════════════════════════════════════════════

MOD_PREDICT_1N2      = ROOT_DIR / "pipelines" / "models" / "predict_1n2.py"
MOD_PREDICT_ENSEMBLE = ROOT_DIR / "pipelines" / "models" / "predict_ensemble.py"
MOD_PREDICT_MARKETS  = ROOT_DIR / "pipelines" / "models" / "predict_markets.py"
MOD_BACKTEST_1N2     = ROOT_DIR / "pipelines" / "models" / "backtest_1n2.py"
MOD_AGENT_GEMINI     = ROOT_DIR / "pipelines" / "agent_gemini.py"


# ══════════════════════════════════════════════════════════════════════════════
# Noms d'étapes
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = [
    "predict_1n2",
    "predict_ensemble",
    "predict_markets",
    "backtest_1n2",
    "agent_gemini",
]


# ══════════════════════════════════════════════════════════════════════════════
# Construction des étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict, season: str, write: bool = False,
                include_agent: bool = True) -> dict:
    """
    Construit le dictionnaire ordonné des étapes de prédiction + backtest.

    L'ordre reflète les dépendances :
        1. predict_1n2 : prédictions directes (nécessaire pour l'ensemble)
        2. predict_ensemble : blend direct + goals (nécessite predict_1n2 + modèle goals)
        3. predict_markets : marchés détaillés (indépendant, utilise seulement goals)
        4. backtest_1n2 : backtest sur la saison (nécessite le modèle 1N2)
        5. agent_gemini : analyse post-run (lit les résultats précédents)

    Args:
        cfg:            config.yaml chargé
        season:         saison cible (ex: "2024-2025")
        write:          si True, écrit les tables dans DuckDB
        include_agent:  si False, exclut l'agent Gemini
    """
    mod_pred_1n2 = import_from_path("predict_1n2",      MOD_PREDICT_1N2)
    mod_ensemble = import_from_path("predict_ensemble",  MOD_PREDICT_ENSEMBLE)
    mod_markets  = import_from_path("predict_markets",   MOD_PREDICT_MARKETS)
    mod_backtest = import_from_path("backtest_1n2",      MOD_BACKTEST_1N2)

    steps = {
        # ── Prédictions ─────────────────────────────────────────────────────
        "predict_1n2": {
            "fn":       mod_pred_1n2.main,
            "kwargs":   {"match_ids": None, "write": write},
            "critical": True,
        },

        "predict_ensemble": {
            "fn":       mod_ensemble.main,
            "kwargs":   {"season": season, "write": write},
            "critical": True,
        },

        "predict_markets": {
            "fn":       mod_markets.main,
            "kwargs":   {"season": season, "csv": True},
            "critical": False,
        },

        # ── Backtest ────────────────────────────────────────────────────────
        "backtest_1n2": {
            "fn":       mod_backtest.main,
            "kwargs":   {"season": season},
            "critical": False,
        },
    }

    # ── Agent Gemini (optionnel) ────────────────────────────────────────────
    if include_agent:
        mod_agent = import_from_path("agent_gemini", MOD_AGENT_GEMINI)

        def _run_agent():
            """Lance l'agent en mode question unique avec un prompt d'analyse."""
            import sys as _s
            # Simule un appel CLI avec argument (mode non-interactif)
            saved_argv = _s.argv
            _s.argv = [
                "agent_gemini.py",
                "Analyse les derniers résultats de backtest et de prédiction. "
                "Résume les métriques clés (ROI, hit rate, CLV) et signale "
                "les anomalies éventuelles."
            ]
            try:
                mod_agent.main()
            finally:
                _s.argv = saved_argv

        steps["agent_gemini"] = {
            "fn":       _run_agent,
            "kwargs":   {},
            "critical": False,
        }

    return steps


# ══════════════════════════════════════════════════════════════════════════════
# Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Predict & Backtest", log_prints=False)
def run_predict_flow(steps: dict, dry_run: bool = False,
                     run_step_task=None) -> list[dict]:
    """Flow Prefect de la phase 5 (prédiction + backtest)."""
    return execute_steps(
        steps,
        run_step_task,
        dry_run=dry_run,
        phase_name="PREDICT & BACKTEST",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 5 — Prédiction + Backtest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python run_predict.py --season 2024-2025                # saison complète
  python run_predict.py --season 2024-2025 --write        # + écriture DuckDB
  python run_predict.py --step backtest_1n2               # une seule étape
  python run_predict.py --from predict_markets            # reprend depuis
  python run_predict.py --no-agent                        # sans agent Gemini
  python run_predict.py --dry-run                         # simule
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--step", choices=STEP_NAMES,
                       help="Exécute une seule étape")
    group.add_argument("--from", dest="from_step", choices=STEP_NAMES,
                       metavar="STEP",
                       help="Exécute depuis cette étape jusqu'à la fin")

    parser.add_argument("--season",
                        help="Saison cible (ex: 2024-2025). "
                             "Défaut : train.TEST_SEASON de config.yaml")
    parser.add_argument("--write", action="store_true",
                        help="Écrit les prédictions dans DuckDB")
    parser.add_argument("--no-agent", action="store_true",
                        help="Exclut l'agent Gemini du pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Liste les étapes sans les exécuter")
    parser.add_argument("--list", action="store_true",
                        help="Affiche les étapes disponibles et quitte")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        print("\nÉtapes Predict & Backtest (dans l'ordre) :")
        for i, name in enumerate(STEP_NAMES, 1):
            print(f"  {i}. {name}")
        print()
        return

    cfg = load_config()
    pipeline_cfg = cfg.get("pipeline", {})

    # Saison : argument CLI > config.yaml > erreur
    season = args.season or cfg.get("train", {}).get("TEST_SEASON")
    if not season:
        print("Erreur : --season requis (ou train.TEST_SEASON dans config.yaml).")
        sys.exit(1)

    run_step_task = make_run_step_task(
        retries=pipeline_cfg.get("retries", 2),
        retry_delay_seconds=pipeline_cfg.get("retry_delay_seconds", 30),
    )

    include_agent = not args.no_agent
    all_steps = build_steps(cfg, season, write=args.write,
                            include_agent=include_agent)

    # Filtrage des steps si nécessaire
    available_names = [n for n in STEP_NAMES if n in all_steps]

    if args.step:
        if args.step not in all_steps:
            print(f"Erreur : étape '{args.step}' non disponible (--no-agent ?).")
            sys.exit(1)
        steps_to_run = {args.step: all_steps[args.step]}
    elif args.from_step:
        if args.from_step not in available_names:
            print(f"Erreur : étape '{args.from_step}' non disponible.")
            sys.exit(1)
        idx = available_names.index(args.from_step)
        steps_to_run = {n: all_steps[n] for n in available_names[idx:]}
    else:
        steps_to_run = all_steps

    results = run_predict_flow(steps_to_run, dry_run=args.dry_run,
                               run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ PREDICT & BACKTEST")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
