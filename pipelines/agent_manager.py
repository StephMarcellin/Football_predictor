"""
Orchestrateur — pipelines/agent_manager.py
==========================================
Script central pour coordonner le pipeline d'agents analytiques.

Flux :
  Analyste (auditor) → Stratégie (strategy_auditor) → Chercheur (researcher) → Développeur (developer)

Usage :
    python pipelines/agent_manager.py
    python pipelines/agent_manager.py --agents auditor
    python pipelines/agent_manager.py --agents auditor strategy_auditor
    python pipelines/agent_manager.py --agents auditor researcher
"""

import argparse
import importlib
import sys
import time
from pathlib import Path

from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR   = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
AGENTS_DIR = ROOT_DIR / "pipeline_agents"
LOGS_DIR   = ROOT_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stderr, level="INFO",
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>[ORCHESTRATOR]</cyan> {message}")
logger.add(
    LOGS_DIR / "agent_manager.log",
    level="DEBUG",
    rotation="5 MB",
    retention=10,
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [ORCHESTRATOR] {message}",
)

# ── Registre des agents ───────────────────────────────────────────────────────
# Ordre d'exécution séquentielle et module correspondant
AGENT_REGISTRY = [
    {
        "name":        "auditor",
        "label":       "Agent Analyste",
        "module":      "pipeline_agents.auditor",
        "entrypoint":  "run",
        "description": "Identifie les 50 matchs les plus catastrophiques et génère un rapport d'insights",
    },
    {
        "name":        "strategy_auditor",
        "label":       "Agent Stratégie",
        "module":      "pipeline_agents.strategy_auditor",
        "entrypoint":  "run",
        "description": (
            "Audit signal brut (mise fixe), comparaison modèle vs marché, "
            "optimisation grille edge×confidence → strategy_audit_report.md"
        ),
    },
    {
        "name":        "researcher",
        "label":       "Agent Chercheur",
        "module":      "pipeline_agents.researcher",
        "entrypoint":  "run",
        "description": "Explore de nouvelles features candidates à partir des patterns d'échec identifiés",
    },
    {
        "name":        "developer",
        "label":       "Agent Développeur",
        "module":      "pipeline_agents.developer",
        "entrypoint":  "run",
        "description": "Génère le code SQL/Python pour les features validées dans 03c_suggested_features.py",
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════════

def banner(text: str, width: int = 60):
    logger.info("═" * width)
    logger.info(f"  {text}")
    logger.info("═" * width)


def run_agent(agent_cfg: dict, context: dict) -> dict:
    """
    Charge dynamiquement le module de l'agent et appelle son entrypoint.
    Retourne le contexte enrichi par l'agent (pour passage au suivant).
    """
    name    = agent_cfg["name"]
    label   = agent_cfg["label"]
    module  = agent_cfg["module"]
    entry   = agent_cfg["entrypoint"]

    logger.info(f"▶ Lancement : {label} ({name})")
    t0 = time.time()

    try:
        mod = importlib.import_module(module)
    except ModuleNotFoundError as e:
        logger.error(f"  Module introuvable : {module} → {e}")
        logger.warning(f"  ⚠ {label} SKIPPED (module absent)")
        return context

    if not hasattr(mod, entry):
        logger.error(f"  Entrypoint '{entry}' absent dans {module}")
        return context

    try:
        result = getattr(mod, entry)(context=context)
        elapsed = time.time() - t0
        logger.success(f"  ✓ {label} terminé en {elapsed:.1f}s")

        # L'agent peut enrichir le contexte pour le prochain
        if isinstance(result, dict):
            context.update(result)

    except Exception as exc:  # noqa: BLE001
        logger.exception(f"  ✗ {label} a échoué : {exc}")

    return context


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATEUR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def main(agents_to_run: list[str] | None = None):
    banner("Projet 3-Étoiles — Pipeline Agents")
    logger.info(f"  ROOT_DIR  : {ROOT_DIR}")
    logger.info(f"  AGENTS    : {agents_to_run or 'tous'}")

    # Contexte partagé entre agents (clé-valeur)
    context: dict = {
        "root_dir":   str(ROOT_DIR),
        "agents_dir": str(AGENTS_DIR),
        "logs_dir":   str(LOGS_DIR),
    }

    # Filtrage des agents à exécuter
    pipeline = [
        a for a in AGENT_REGISTRY
        if agents_to_run is None or a["name"] in agents_to_run
    ]

    if not pipeline:
        logger.error("Aucun agent valide sélectionné. Agents disponibles : "
                     + ", ".join(a["name"] for a in AGENT_REGISTRY))
        sys.exit(1)

    logger.info(f"  Agents dans le pipeline : {[a['name'] for a in pipeline]}")

    # Exécution séquentielle
    t_global = time.time()
    for agent_cfg in pipeline:
        context = run_agent(agent_cfg, context)

    elapsed_total = time.time() - t_global
    banner(f"Pipeline terminé en {elapsed_total:.1f}s")

    # Rapport de sortie
    if "report_path" in context:
        logger.info(f"  📄 Rapport Analyste    : {context['report_path']}")
    if "strategy_report" in context:
        logger.info(f"  📈 Rapport Stratégie   : {context['strategy_report']}")
    if "suggested_features_path" in context:
        logger.info(f"  🛠  Features suggérées : {context['suggested_features_path']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orchestrateur Agents Projet 3-Étoiles")
    parser.add_argument(
        "--agents",
        nargs="+",
        choices=[a["name"] for a in AGENT_REGISTRY],
        default=None,
        help="Agents à exécuter (défaut : tous dans l'ordre)",
    )
    args = parser.parse_args()
    main(agents_to_run=args.agents)