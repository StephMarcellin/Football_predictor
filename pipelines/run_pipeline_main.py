#!/usr/bin/env python3
"""
run_pipeline_main.py — Master orchestrator du Projet 3-Étoiles
================================================================

Enchaîne les orchestrateurs de phase (scrapping, ingest, features, ml) en
subprocess indépendants, selon un flux de cadence choisi.

Trois flux de cadence, alignés sur LINEAGE_TABLES_ET_FREQUENCES.md :

  --daily   scrapping + ingest.whoscored_events + features.flow=daily
            → 1×/jour : mise à jour des tables features avec les nouveaux
              matchs WhoScored. Pas de retrain ML.

  --weekly  daily + ml
            → 1×/semaine : idem daily, plus retrain des 4 modèles
              (goals, 1n2, buteurs, passeurs).

  --yearly  scrapping + ingest complet + features.flow=yearly + ml
            → ~1×/saison : refit des artefacts stables (xt_grid, xgot),
              plus retrain complet.

Chaque phase reste exécutable seule via son propre script.

UI Prefect Cloud (https://app.prefect.cloud) :
    - 1 flow run "Pipeline Master 3-Étoiles" (le master, 1 task par phase)
    - N flow runs séparés (un par phase enfant, non nested — subprocess
      indépendants qui se connectent à Prefect Cloud via PREFECT_API_URL).

Scheduling :
    Paramètres dans config.yaml (section pipeline_main) :
        cron_daily   / cron_weekly / cron_yearly   — expressions cron
        timezone                                    — sinon interprété en UTC
        deployment_name_daily / _weekly / _yearly   — noms dans l'UI Prefect
        retries                                     — tentatives par phase
        retry_delay_seconds                         — délai entre tentatives
"""

from __future__ import annotations

# --- bootstrap : rend orchestrator_common (dans pipelines/) importable ---
import sys as _sys
from pathlib import Path as _Path
_pipelines_dir = _Path(__file__).resolve().parent
if str(_pipelines_dir) not in _sys.path:
    _sys.path.insert(0, str(_pipelines_dir))
# ------------------------------------------------------------------------

from dotenv import load_dotenv
load_dotenv()

import argparse
import subprocess
import sys
from pathlib import Path

from prefect import flow
from prefect.schedules import Cron
from loguru import logger

from orchestrator_common import (
    ROOT_DIR,
    load_config,
    make_run_step_task,
    execute_steps,
    print_summary,
)

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── Logger fichier (ajouté au runtime, pas à l'import) ──────────────────────
# Prefect Cloud peut sérialiser (cloudpickle) le flow pour l'exécuter.
# Un fichier de log ouvert n'est pas picklable → on l'ouvre seulement quand
# le flow tourne, via _ensure_file_log(). Pattern identique à run_scrapping.py.
Path("logs").mkdir(exist_ok=True)
_FILE_LOG_ADDED = False


def _ensure_file_log() -> None:
    """Ajoute le sink fichier logs/pipeline_main.log, une seule fois par processus."""
    global _FILE_LOG_ADDED
    if _FILE_LOG_ADDED:
        return
    logger.add(
        "logs/pipeline_main.log",
        level="INFO",
        encoding="utf-8",
        rotation="5 MB",
        retention=10,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [MASTER] {message}",
    )
    _FILE_LOG_ADDED = True


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Exécution d'une phase en subprocess
#
# Pourquoi subprocess plutôt qu'import + main() ?
#   Les orchestrateurs enfants ont chacun leur propre argparse (ils lisent
#   sys.argv) et un sys.exit() en fin de main(). Les importer et les appeler
#   dans le même processus provoquerait :
#     - une collision sur sys.argv (les enfants liraient les args du master)
#     - un sys.exit() qui tuerait le master
#   → subprocess = isolation totale (sys.argv, sys.exit, imports, os.chdir).
#   Chaque subprocess se connecte indépendamment à Prefect Cloud → son propre
#   flow run visible dans l'UI (non nested sous le master).
# ══════════════════════════════════════════════════════════════════════════════

def run_phase_subprocess(script_path: Path, extra_args: list = None, dry_run: bool = False) -> None:
    """
    Lance un orchestrateur de phase (run_scrapping.py, run_ingest.py, …)
    comme processus fils indépendant.

    Args:
        script_path : chemin absolu du run_*.py à exécuter
        extra_args  : liste d'arguments à passer à l'enfant (ex : ["--step", "whoscored_events"])
        dry_run     : si True, ajoute --dry-run en plus

    Streaming ligne par ligne (Popen) : sans ça, aucun retour visuel pendant
    les 2-7 h d'exécution d'une phase.

    RuntimeError si code de retour ≠ 0 → orchestrator_common.run_step capte
    l'exception et déclenche les retries Prefect.
    """
    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable : {script_path}")

    cmd = [sys.executable, str(script_path)]
    if extra_args:
        cmd.extend(extra_args)
    if dry_run:
        cmd.append("--dry-run")

    logger.info(f"→ Subprocess : {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # stderr → stdout, un seul flux
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,                  # line-buffered
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        # logger.opt(raw=True) : on écrit la ligne enfant telle quelle
        # (elle a déjà son propre timestamp/prefix loguru), sans re-préfixer.
        logger.opt(raw=True).info(line if line.endswith("\n") else line + "\n")

    return_code = proc.wait()
    if return_code != 0:
        raise RuntimeError(
            f"Phase '{script_path.name}' a échoué (code retour {return_code}). "
            f"Voir logs de la phase pour le détail."
        )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Compositions par flux de cadence
#
# Chaque flux liste les phases à exécuter dans l'ordre, avec leurs arguments
# spécifiques. Source de vérité : LINEAGE_TABLES_ET_FREQUENCES.md.
# ══════════════════════════════════════════════════════════════════════════════

PHASE_SCRIPTS = {
    "pre-scrapping":        ROOT_DIR / "pipelines" / "scrapping" / "run_pre_scrapping.py",
    "scrapping":            ROOT_DIR / "pipelines" / "scrapping" / "run_scrapping.py",
    "ingest":               ROOT_DIR / "pipelines" / "ingest"    / "run_ingest.py",
    "features_engineering": ROOT_DIR / "pipelines" / "run_features_engineering.py",
    "ml":                   ROOT_DIR / "pipelines" / "run_ml.py",
}

# Structure : liste de (nom_phase, script_path, extra_args)
PHASE_SPECS = {
    "daily": [
        ("pre-scrapping",        PHASE_SCRIPTS["pre-scrapping"],        []),
        ("scrapping",            PHASE_SCRIPTS["scrapping"],            []),
        ("ingest",               PHASE_SCRIPTS["ingest"],               ["--step", "whoscored_events"]),
        ("features_engineering", PHASE_SCRIPTS["features_engineering"], ["--flow", "daily"]),
    ],
    "weekly": [
        ("ml",                   PHASE_SCRIPTS["ml"],                   []),
    ],
    "yearly": [
        ("scrapping",            PHASE_SCRIPTS["scrapping"],            []),
        ("ingest",               PHASE_SCRIPTS["ingest"],               []),
        ("features_engineering", PHASE_SCRIPTS["features_engineering"], ["--flow", "yearly"]),
        ("ml",                   PHASE_SCRIPTS["ml"],                   []),
    ],
}


def build_phases(flow_name: str, dry_run: bool = False) -> dict:
    """
    Construit le dict de phases pour un flux donné.

    Chaque phase suit le schéma {fn, kwargs, critical} attendu par
    orchestrator_common.execute_steps.

    critical=True partout : les phases sont séquentielles et dépendantes.
    Un échec amont rend les phases suivantes inutiles → fail-fast.
    """
    if flow_name not in PHASE_SPECS:
        raise ValueError(f"Flux inconnu : {flow_name}. Choix : {list(PHASE_SPECS)}")

    return {
        name: {
            "fn":       run_phase_subprocess,
            "kwargs":   {"script_path": path, "extra_args": extra_args, "dry_run": dry_run},
            "critical": True,
        }
        for (name, path, extra_args) in PHASE_SPECS[flow_name]
    }


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Flow Prefect
# ══════════════════════════════════════════════════════════════════════════════

@flow(name="Pipeline Master 3-Étoiles", log_prints=False)
def run_master_flow(phases: dict, dry_run: bool = False, run_step_task=None) -> list:
    """
    Flow Prefect du master. Enchaîne les phases via execute_steps
    (module commun) — même moteur que les autres orchestrateurs.
    """
    _ensure_file_log()
    return execute_steps(
        phases,
        run_step_task,
        dry_run=dry_run,
        phase_name="PIPELINE MASTER 3-ÉTOILES",
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Flows planifiés (pour --serve)
#
# Définis au niveau MODULE (pas dans main). Prefect Cloud charge ces fonctions
# par entrypoint (fichier:fonction) lors d'un run planifié, au lieu de les
# sérialiser « par valeur ». Sinon le pickling embarquerait le sink loguru
# → PicklingError. Même pattern que scheduled_ingest / scheduled_scrapping.
# ══════════════════════════════════════════════════════════════════════════════

def _run_scheduled(flow_name: str) -> list:
    """Corps commun aux flows planifiés — recharge cfg, task, phases."""
    cfg = load_config()
    pm_cfg = cfg.get("pipeline_main", cfg.get("pipeline", {}))
    run_step_task = make_run_step_task(
        retries=pm_cfg.get("retries", 1),
        retry_delay_seconds=pm_cfg.get("retry_delay_seconds", 60),
    )
    phases_to_run = build_phases(flow_name, dry_run=False)
    return run_master_flow(phases_to_run, dry_run=False, run_step_task=run_step_task)


@flow(name="Pipeline Master 3-Étoiles — daily", log_prints=True)
def scheduled_master_daily() -> list:
    """Flux quotidien planifié (Prefect Cloud)."""
    return _run_scheduled("daily")


@flow(name="Pipeline Master 3-Étoiles — weekly", log_prints=True)
def scheduled_master_weekly() -> list:
    """Flux hebdomadaire planifié (Prefect Cloud)."""
    return _run_scheduled("weekly")


@flow(name="Pipeline Master 3-Étoiles — yearly", log_prints=True)
def scheduled_master_yearly() -> list:
    """Flux annuel planifié (Prefect Cloud)."""
    return _run_scheduled("yearly")


_SCHEDULED_FLOWS = {
    "daily":  scheduled_master_daily,
    "weekly": scheduled_master_weekly,
    "yearly": scheduled_master_yearly,
}

_DEFAULT_CRONS = {
    "daily":  "0 6 * * *",         # tous les jours à 6h
    "weekly": "0 20 * * 5",        # vendredi 20h
    "yearly": "0 3 1 7 *",         # 1er juillet à 3h
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — CLI
# ══════════════════════════════════════════════════════════════════════════════

FLOW_NAMES = ("daily", "weekly", "yearly")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Master orchestrator Pipeline 3-Étoiles (3 flux de cadence)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python pipelines/run_pipeline_main.py --daily              # exécution immédiate quotidienne
  python pipelines/run_pipeline_main.py --weekly             # exécution immédiate hebdomadaire
  python pipelines/run_pipeline_main.py --yearly             # exécution immédiate annuelle
  python pipelines/run_pipeline_main.py --daily --dry-run    # simulation
  python pipelines/run_pipeline_main.py --list               # liste les flux et leur composition
  python pipelines/run_pipeline_main.py --weekly --serve     # scheduler Prefect Cloud pour weekly
""",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daily",  action="store_true", help="Flux quotidien")
    group.add_argument("--weekly", action="store_true", help="Flux hebdomadaire (daily + ml)")
    group.add_argument("--yearly", action="store_true", help="Flux annuel (ingest complet + refit features + ml)")
    group.add_argument("--list",   action="store_true", help="Affiche les flux et leur composition, puis quitte")

    parser.add_argument("--serve", action="store_true",
                        help="Démarre le scheduler Prefect Cloud (bloquant) pour le flux "
                             "sélectionné. Utilise le cron de config.yaml (section pipeline_main).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Propage --dry-run à chaque phase enfant (aucune exécution réelle)")

    return parser.parse_args()


def _selected_flow(args: argparse.Namespace) -> str:
    """Retourne le nom du flux choisi ('daily' / 'weekly' / 'yearly')."""
    for name in FLOW_NAMES:
        if getattr(args, name):
            return name
    raise RuntimeError("Aucun flux sélectionné (should not happen : mutex group required=True)")


def main() -> None:
    args = parse_args()

    # --list : affiche les flux et quitte
    if args.list:
        print("\nFlux disponibles :\n")
        for flow_name in FLOW_NAMES:
            print(f"  --{flow_name} :")
            for i, (phase, path, extra) in enumerate(PHASE_SPECS[flow_name], 1):
                extra_str = " ".join(extra) if extra else "(aucun argument)"
                print(f"    {i}. {phase:<22} → {path.relative_to(ROOT_DIR)}   [{extra_str}]")
            print()
        return

    cfg = load_config()
    pm_cfg = cfg.get("pipeline_main", cfg.get("pipeline", {}))

    run_step_task = make_run_step_task(
        retries=pm_cfg.get("retries", 1),
        retry_delay_seconds=pm_cfg.get("retry_delay_seconds", 60),
    )

    flow_name = _selected_flow(args)

    # ── Mode --serve : scheduler Prefect Cloud (bloquant) ────────────────────
    if args.serve:
        cron_key             = f"cron_{flow_name}"
        deployment_name_key  = f"deployment_name_{flow_name}"

        cron            = pm_cfg.get(cron_key, _DEFAULT_CRONS[flow_name])
        deployment_name = pm_cfg.get(deployment_name_key, f"pipeline-master-{flow_name}")
        timezone        = pm_cfg.get("timezone", "Europe/Paris")

        logger.info(f"Démarrage du scheduler Prefect Cloud (master, flux {flow_name})")
        logger.info(f"  Déploiement : {deployment_name}")
        logger.info(f"  Cron        : {cron}  ({timezone})")
        logger.info(f"  UI          : https://app.prefect.cloud")
        logger.info("  (Ctrl+C pour arrêter le scheduler)")

        _SCHEDULED_FLOWS[flow_name].serve(
            name=deployment_name,
            schedule=Cron(cron, timezone=timezone),
        )
        return

    # ── Mode normal : exécution immédiate ────────────────────────────────────
    phases_to_run = build_phases(flow_name, dry_run=args.dry_run)

    results = run_master_flow(phases_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title=f"RÉSUMÉ MASTER 3-ÉTOILES ({flow_name})")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
