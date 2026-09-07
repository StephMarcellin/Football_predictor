#!/usr/bin/env python3
"""
run_features_engineering.py — Orchestrateur PHASE 3 : Feature Engineering
=========================================================================
Transforme données Silver en features ML via dbt + modèles Python :
    1. dbt_run       : backbone + intermediate + stubs (xt_actions, xgot_features)
    2. xt_grid.py    : calcule grille Expected Threat 2D (~3h)
    3. xgot_score.py : applique modèle xGOT keeper (~2h)
    4. knn_impute.py : impute valeurs manquantes (~1h)
    5. dbt_run       : gold.* (10 modèles, 250+ features)
    6. dbt_test      : validation + fail-fast

Sortie : intermediate.* (25+) + gold.* (10) + machine_learning.* (artefacts)
Durée  : ~7h

Usage :
    python run_features_engineering.py                 # pipeline complet
    python run_features_engineering.py --step dbt_run  # étape unique
    python run_features_engineering.py --from xt_grid  # reprend depuis xt_grid
    python run_features_engineering.py --tables        # bloc tables_update
    python run_features_engineering.py --refit          # bloc tables_refit (xT, xGOT)
    python run_features_engineering.py --dry-run        # simule sans exécuter
    python run_features_engineering.py --serve          # scheduler Prefect (bloquant)

    tables_update suppose que models/xgot.joblib et la table xt_grid existent déjà
    (xgot_score et int_xt_contributions les consomment).
    Sur une base neuve, lance --refit d'abord.
"""

from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import argparse
import os
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
from dbt_helpers import run_dbt_run, run_dbt_test, check_dbt_test_results

from validation.run_validation import run_validate_gold

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Logger fichier ───────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/features_engineering.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [FEATURES] {message}",
)


# ══════════════════════════════════════════════════════════════════════════════
# Chemins des modules Python appelés entre deux phases dbt
# ══════════════════════════════════════════════════════════════════════════════

MOD_XGOT_SCORE = ROOT_DIR / "pipelines" / "features" / "xgot_score.py"
MOD_KNN        = ROOT_DIR / "pipelines" / "features" / "03_knn_impute.py"
MOD_XT_GRID    = ROOT_DIR / "pipelines" / "features" / "xt_grid.py"
MOD_XGOT_TRAIN = ROOT_DIR / "pipelines" / "xgot_train.py"


# ══════════════════════════════════════════════════════════════════════════════
# Blocs réutilisables
# ══════════════════════════════════════════════════════════════════════════════
# L'ordre est défini UNE fois ici ; les flux (daily/rare) y piochent par nom.

TABLES_UPDATE = [
    "dbt_transform", "xgot_score", "knn_impute",
    "dbt_transform_downstream", "dbt_test",
]
TABLES_REFIT = ["xt_grid", "xgot_train"]

FLOWS = {
    "daily": TABLES_UPDATE,          # transform + score, PAS d'entraînement
    "rare":  TABLES_REFIT,           # refit artefacts coûteux (xT, xGOT)
}


# ══════════════════════════════════════════════════════════════════════════════
# Étapes
# ══════════════════════════════════════════════════════════════════════════════

def build_steps(cfg: dict, full_refresh: bool = False) -> dict:
    """
    Construit le dictionnaire ordonné des étapes de feature engineering.

    Les imports sont différés ici (pas en tête de module) pour ne charger
    les dépendances lourdes qu'au moment de l'exécution.
    """
    mod_xgot = import_from_path("xgot_score_mod", MOD_XGOT_SCORE)
    mod_xt   = import_from_path("xt_grid_mod",    MOD_XT_GRID)

    return {
        # ── dbt run principal (tout sauf chaînes xT et xGOT) ────────────────
        "dbt_run": {
            "fn":       run_dbt_run,
            "kwargs":   {
                "select": "backbone features_rolling features_whoscored "
                          "features_draw features_final",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── Chaîne xT : dbt(int_xt_actions) → xt_grid.py → dbt(int_xt_contributions)
        "dbt_xt_actions": {
            "fn":       run_dbt_run,
            "kwargs":   {"select": "+int_xt_actions", "full_refresh": full_refresh},
            "critical": False,
        },
        "xt_grid": {
            "fn":       mod_xt.main,
            "kwargs":   {},
            "critical": False,
        },
        "dbt_xt_contributions": {
            "fn":       run_dbt_run,
            "kwargs":   {"select": "int_xt_contributions"},
            "critical": False,
        },

        # ── Chaîne PSxG gardien : dbt(xgot_features) → xgot_score → dbt(keeper)
        "dbt_xgot_features": {
            "fn":       run_dbt_run,
            "kwargs":   {"select": "xgot_features", "full_refresh": full_refresh},
            "critical": False,
        },
        "xgot_score": {
            "fn":       mod_xgot.main,
            "kwargs":   {},
            "critical": False,
        },
        "dbt_keeper_psxg": {
            "fn":       run_dbt_run,
            "kwargs":   {"select": "int_keeper_shots int_keeper_psxg"},
            "critical": False,
        },

        # ── Validation ──────────────────────────────────────────────────────
        "dbt_test": {
            "fn":       run_dbt_test,
            "kwargs":   {},
            "critical": False,
        },
        "dbt_test_check": {
            "fn":       check_dbt_test_results,
            "kwargs":   {},
            "critical": True,
        },
        "validate_gold": {
            "fn":       run_validate_gold,
            "kwargs":   {},
            "critical": True,
        },
    }


def build_table_steps(cfg: dict, full_refresh: bool = False) -> dict:
    """Catalogue des steps de construction des tables (blocs --tables / --refit).

    Les frontières de phase autour du KNN/xgot viennent du DAG dbt
    (opérateur '+' : int_keeper_shots+ = la chaîne gardien jusqu'aux marts ;
    joueur_match+ = jusqu'à mart_scorers).
    """
    mod_xgot = import_from_path("xgot_score_mod", MOD_XGOT_SCORE)
    mod_knn  = import_from_path("knn_mod",        MOD_KNN)
    mod_xt   = import_from_path("xt_grid_mod",    MOD_XT_GRID)
    mod_xgt  = import_from_path("xgot_train_mod", MOD_XGOT_TRAIN)

    return {
        # ── tables_update ────────────────────────────────────────────────────
        "dbt_transform": {
            "fn":       run_dbt_run,
            "kwargs":   {"exclude": "int_keeper_shots+ joueur_match+",
                         "full_refresh": full_refresh},
            "critical": True,
        },
        "xgot_score": {
            "fn":       mod_xgot.main,
            "kwargs":   {},
            "critical": False,
        },
        "knn_impute": {
            "fn":       mod_knn.main,
            "kwargs":   {"write": True},
            "critical": True,
        },
        "dbt_transform_downstream": {
            "fn":       run_dbt_run,
            "kwargs":   {"select": "int_keeper_shots+ joueur_match+"},
            "critical": True,
        },
        "dbt_test": {
            "fn":       run_dbt_test,
            "kwargs":   {},
            "critical": False,
        },

        # ── tables_refit (rare) ──────────────────────────────────────────────
        "xt_grid": {
            "fn":       mod_xt.main,
            "kwargs":   {},
            "critical": False,
        },
        "xgot_train": {
            "fn":       mod_xgt.run,
            "kwargs":   {},
            "critical": False,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Features Engineering", log_prints=False)
def run_features_flow(steps: dict, dry_run: bool = False, run_step_task=None) -> list[dict]:
    """Flow Prefect de la phase 3."""
    return execute_steps(
        steps,
        run_step_task,
        dry_run=dry_run,
        phase_name="FEATURES ENGINEERING",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

STEP_NAMES = [
    "dbt_run",
    "dbt_xt_actions", "xt_grid", "dbt_xt_contributions",
    "dbt_xgot_features", "xgot_score", "dbt_keeper_psxg",
    "dbt_test", "dbt_test_check", "validate_gold",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 3 — Feature Engineering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Exemples :
        python run_features_engineering.py                    # complet
        python run_features_engineering.py --step dbt_run     # une étape
        python run_features_engineering.py --from xgot_score  # reprend depuis
        python run_features_engineering.py --tables           # bloc tables_update
        python run_features_engineering.py --refit            # bloc tables_refit
        python run_features_engineering.py --dry-run          # simule
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--step", choices=STEP_NAMES, help="Exécute une seule étape")
    group.add_argument("--from", dest="from_step", choices=STEP_NAMES,
                       metavar="STEP", help="Exécute depuis cette étape jusqu'à la fin")

    parser.add_argument("--dry-run", action="store_true",
                        help="Liste les étapes sans les exécuter")
    parser.add_argument("--list", action="store_true",
                        help="Affiche les étapes disponibles et quitte")
    parser.add_argument("--tables", action="store_true",
                        help="Bloc tables_update : dbt transform → xgot → KNN → downstream → test")
    parser.add_argument("--refit", action="store_true",
                        help="Bloc tables_refit : refit grille xT + modèle xGOT")
    parser.add_argument("--flow", choices=["daily", "rare"],
                        help="Flux composé : daily=tables_update, rare=tables_refit")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Force dbt --full-refresh (recrée les tables depuis zéro)")
    parser.add_argument("--serve", action="store_true",
                        help="Démarre le scheduler Prefect (bloquant).")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        print("\nÉtapes disponibles (dans l'ordre) :")
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

    # ── Blocs tables ─────────────────────────────────────────────────────────
    if args.tables or args.refit:
        table_steps = build_table_steps(cfg, full_refresh=args.full_refresh)
        block = TABLES_UPDATE if args.tables else TABLES_REFIT
        steps_to_run = {n: table_steps[n] for n in block}
        results = run_features_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
        print_summary(results, title="RÉSUMÉ FEATURES ENGINEERING")
        sys.exit(1 if [r for r in results if r["status"] == "FAILED"] else 0)

    # ── Flux composés ────────────────────────────────────────────────────────
    if args.flow:
        table_steps = build_table_steps(cfg, full_refresh=args.full_refresh)
        steps_to_run = {n: table_steps[n] for n in FLOWS[args.flow]}
        results = run_features_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
        print_summary(results, title="RÉSUMÉ FEATURES ENGINEERING")
        sys.exit(1 if [r for r in results if r["status"] == "FAILED"] else 0)

    # ── Mode --serve ─────────────────────────────────────────────────────────
    if args.serve:
        cron = pipeline_cfg.get("cron", "0 12 * * 1")
        deployment_name = pipeline_cfg.get("deployment_name", "features-engineering")

        logger.info("Démarrage du scheduler Prefect (features engineering)")
        logger.info(f"  Déploiement : {deployment_name}")
        logger.info(f"  Cron        : {cron}")
        logger.info("  (Ctrl+C pour arrêter)")

        @flow(name="Features Engineering", log_prints=True)
        def scheduled_features():
            return run_features_flow(
                build_steps(load_config()),
                dry_run=False,
                run_step_task=run_step_task,
            )

        scheduled_features.serve(name=deployment_name, cron=cron)
        return

    # ── Mode normal : exécution immédiate ────────────────────────────────────
    all_steps = build_steps(cfg, full_refresh=args.full_refresh)

    if args.step:
        steps_to_run = {args.step: all_steps[args.step]}
    elif args.from_step:
        idx = STEP_NAMES.index(args.from_step)
        steps_to_run = {n: all_steps[n] for n in STEP_NAMES[idx:]}
    else:
        steps_to_run = all_steps

    results = run_features_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ FEATURES ENGINEERING")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
