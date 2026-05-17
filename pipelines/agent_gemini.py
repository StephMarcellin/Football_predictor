"""
agent_gemini.py — Agent Gemini pour piloter le Pipeline 3-Étoiles
==================================================================
SDK : google-genai (nouveau SDK officiel Google)
Pattern : ReAct + Function Calling

Prérequis :
    pip install google-genai python-dotenv httpx loguru pyyaml
    Fichier .env à la racine : GOOGLE_API_KEY=ta_clé

Usage :
    python pipelines/agent_gemini.py                    # mode interactif
    python pipelines/agent_gemini.py "ta question"      # question unique

Outils disponibles :
    read_logs()              — lit les fichiers de log
    get_pipeline_status()    — historique des runs Prefect
    read_backtest_results()  — métriques de la stratégie de paris
    run_pipeline_step(step)  — relance une étape (avec confirmation)
"""

from __future__ import annotations

import os
import sys
import csv
from pathlib import Path
from collections import defaultdict

import time
import re

import yaml
from dotenv import load_dotenv
from google import genai
from google.genai import types
from loguru import logger
import httpx
import subprocess

from prefect.artifacts import create_markdown_artifact

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent.parent
PREFECT_API: str = "http://127.0.0.1:4200/api"

# Force UTF-8 sur stdout/stderr Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Configuration
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    config_path = ROOT_DIR / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logger(log_file: str, verbose: bool) -> None:
    Path(log_file).parent.mkdir(exist_ok=True)
    logger.remove()
    logger.add(
        log_file,
        level="DEBUG",
        encoding="utf-8",
        rotation="2 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )
    console_level = "INFO" if verbose else "WARNING"
    logger.add(
        sys.stderr,
        level=console_level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Prompt système
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """
Tu es un agent spécialisé dans le pilotage du Pipeline 3-Étoiles,
un pipeline de prédiction de matchs de football.

Le pipeline comporte 6 étapes dans cet ordre :
  1. dbt_seed  — Charge les référentiels (team_mapping, transfermarkt_clubs)
  2. dbt_run   — Construit toute la chaîne Gold via dbt (backbone, features_rolling,
                 features_whoscored, features_draw, features_final)
  3. dbt_test  — Exécute les 235 tests qualité automatiques sur les données Gold
  4. train     — Entraînement du modèle LightGBM two-stage (04_train.py)
  5. predict   — Prédictions sur les matchs à venir (05_predict.py)
  6. backtest  — Backtest de la stratégie de paris (06_backtest.py)

Les étapes dbt_seed → dbt_run → dbt_test sont critiques : un échec stoppe le pipeline.
L'étape backtest est non-critique : un échec ne bloque pas les prédictions.

Tes outils disponibles :
  - read_logs()             — lit pipeline.log ou agent.log
  - get_pipeline_status()   — historique des runs Prefect
  - read_backtest_results() — métriques ROI/win rate de la stratégie
  - run_pipeline_step()     — relance une étape (confirmation gérée par le système)
  - read_dbt_logs()         — sortie complète du dernier dbt seed/run/test
  - get_dbt_test_results()  — résumé structuré PASS/WARN/FAIL/ERROR des tests dbt

Stratégie de diagnostic recommandée :
  - Pour un échec dbt_test → appelle d'abord get_dbt_test_results() pour les compteurs,
    puis read_dbt_logs(command="test") si tu as besoin du détail d'un test spécifique.
  - Pour un échec dbt_run  → appelle read_dbt_logs(command="run") pour voir quel modèle a planté.
  - Pour évaluer si un retraining est nécessaire → appelle read_backtest_results().

Tes responsabilités :
  - Diagnostiquer l'état du pipeline à partir des logs et de l'historique
  - Identifier les erreurs, les anomalies, les dégradations de performance
  - Recommander des actions (relance d'étapes, investigation)
  - Quand l'utilisateur te demande explicitement de relancer une étape,
    appelle IMMÉDIATEMENT run_pipeline_step sans demander de confirmation
    supplémentaire. La confirmation est gérée par le système, pas par toi.

Ton comportement :
  - Sois précis et factuel. Appuie-toi sur les données des outils.
  - Si tu n'as pas l'information nécessaire, utilise un outil pour l'obtenir.
  - Avant toute action sur le pipeline, explique ce que tu vas faire et pourquoi.
  - Réponds toujours en français.
"""


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Outils
# ══════════════════════════════════════════════════════════════════════════════

# ── Outil 1 : read_logs() ─────────────────────────────────────────────────────

def read_logs(n_lines: int = 100, log_file: str = "pipeline.log") -> str:
    """
    Lit les N dernières lignes du fichier de log demandé.

    Args:
        n_lines  : nombre de lignes à retourner (défaut 100)
        log_file : nom du fichier dans le dossier logs/ (défaut pipeline.log)

    Returns:
        Contenu des dernières lignes, ou message d'erreur si fichier absent.
    """
    log_path = ROOT_DIR / "logs" / log_file

    if not log_path.exists():
        return f"Fichier introuvable : {log_path}"

    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # On prend les N dernières lignes
    excerpt = lines[-n_lines:]
    result  = "".join(excerpt)

    logger.debug(f"read_logs({log_file}, {n_lines}) → {len(excerpt)} lignes lues")
    return result or "(fichier vide)"

# ── Outil 2 : get_pipeline_status() ──────────────────────────────────────────




def get_pipeline_status(n_runs: int = 10) -> str:
    """
    Interroge l'API Prefect locale pour récupérer l'historique des runs.

    Args:
        n_runs : nombre de runs à retourner (défaut 10)

    Returns:
        Résumé lisible des derniers runs, ou message d'erreur si serveur indisponible.
    """
    try:
        response = httpx.post(
            f"{PREFECT_API}/flow_runs/filter",
            json={
                "limit": n_runs,
                "sort": "START_TIME_DESC",   # les plus récents en premier
            },
            timeout=5.0,   # on n'attend pas si le serveur est éteint
        )
        response.raise_for_status()

    except httpx.ConnectError:
        return (
            "Serveur Prefect non disponible sur http://127.0.0.1:4200. "
            "Lance-le avec : prefect server start"
        )
    except httpx.TimeoutException:
        return "Timeout : le serveur Prefect ne répond pas dans les 5 secondes."
    except httpx.HTTPStatusError as e:
        return f"Erreur API Prefect : {e.response.status_code} — {e.response.text}"

    runs = response.json()

    if not runs:
        return "Aucun run trouvé dans l'historique Prefect."

    # ── Formatage lisible pour Gemini ─────────────────────────────────────────
    # On construit un résumé texte : une ligne par run
    lines = [f"Historique des {len(runs)} derniers runs Prefect :\n"]

    for run in runs:
        name       = run.get("name", "?")
        state      = run.get("state", {}).get("type", "?")
        start      = run.get("start_time", "?")
        end        = run.get("end_time")

        # Calcul durée si disponible
        duration = "?"
        if start and start != "?" and end:
            from datetime import datetime, timezone
            fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
            try:
                t0  = datetime.fromisoformat(start)
                t1  = datetime.fromisoformat(end)
                dur = (t1 - t0).total_seconds()
                duration = f"{dur:.0f}s"
            except Exception:
                duration = "?"

        # Icône selon statut
        icons = {
            "COMPLETED": "✓",
            "FAILED":    "✗",
            "RUNNING":   "▶",
            "CRASHED":   "💥",
            "CANCELLED": "⊘",
            "PENDING":   "⏳",
        }
        icon = icons.get(state, "?")

        # Formatage date lisible
        start_short = start[:19].replace("T", " ") if start and start != "?" else "?"

        lines.append(f"  {icon} [{state:<10}] {start_short}  durée: {duration:<8}  {name}")

    return "\n".join(lines)

# ── Outil 3 : read_backtest_results() ────────────────────────────────────────

def read_backtest_results(n_recent: int = 100) -> str:
    """
    Lit backtest_results.csv et retourne un résumé des métriques clés.

    Calcule sur les N paris les plus récents :
      - ROI global et par league
      - Win rate
      - Edge moyen, odd moyen
      - Profit cumulé
      - Nombre de paris par outcome (home/draw/away)

    Args:
        n_recent : nombre de paris récents à analyser (défaut 100)

    Returns:
        Résumé textuel des métriques, prêt à être interprété par l'agent.
    """
    csv_path = ROOT_DIR / "models" / "backtest_results.csv"

    if not csv_path.exists():
        return f"Fichier introuvable : {csv_path}"

    # ── Lecture du CSV ────────────────────────────────────────────────────────
    with open(csv_path, encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    if not reader:
        return "backtest_results.csv est vide."

    # On prend les N lignes les plus récentes
    rows = reader[-n_recent:]
    total = len(rows)

    # ── Calculs globaux ───────────────────────────────────────────────────────
    try:
        profits     = [float(r["profit"]) for r in rows]
        mises       = [float(r["mise"])   for r in rows]
        edges       = [float(r["edge"])   for r in rows]
        odds        = [float(r["odd"])    for r in rows]
        won_flags   = [r["won"].strip().lower() in ("1", "true", "yes") for r in rows]

        profit_total = sum(profits)
        mise_total   = sum(mises)
        roi          = (profit_total / mise_total * 100) if mise_total > 0 else 0
        win_rate     = (sum(won_flags) / total * 100) if total > 0 else 0
        edge_moyen   = sum(edges) / total
        odd_moyen    = sum(odds) / total
        bankroll_fin = float(rows[-1]["bankroll"]) if rows else 0
        bankroll_deb = float(rows[0]["bankroll"])  if rows else 0

    except (ValueError, KeyError) as e:
        return f"Erreur de lecture des données : {e}"

    # ── Calculs par league ────────────────────────────────────────────────────
    by_league: dict[str, dict] = defaultdict(lambda: {"profit": 0.0, "mise": 0.0, "n": 0})
    for r in rows:
        league = r.get("league", "?")
        by_league[league]["profit"] += float(r["profit"])
        by_league[league]["mise"]   += float(r["mise"])
        by_league[league]["n"]      += 1

    league_lines = []
    for league, data in sorted(by_league.items()):
        roi_l = (data["profit"] / data["mise"] * 100) if data["mise"] > 0 else 0
        league_lines.append(
            f"    {league:<20} n={data['n']:<5} ROI={roi_l:+.1f}%  profit={data['profit']:+.1f}€"
        )

    # ── Calculs par type de pari ──────────────────────────────────────────────
    by_outcome: dict[str, dict] = defaultdict(lambda: {"profit": 0.0, "n": 0, "won": 0})
    for r, won in zip(rows, won_flags):
        outcome = r.get("bet_outcome", "?")
        by_outcome[outcome]["profit"] += float(r["profit"])
        by_outcome[outcome]["n"]      += 1
        by_outcome[outcome]["won"]    += int(won)

    outcome_lines = []
    for outcome, data in sorted(by_outcome.items()):
        wr = (data["won"] / data["n"] * 100) if data["n"] > 0 else 0
        outcome_lines.append(
            f"    {outcome:<8} n={data['n']:<5} win_rate={wr:.0f}%  profit={data['profit']:+.1f}€"
        )

    # ── Formatage du résumé ───────────────────────────────────────────────────
    signe_roi     = "+" if roi >= 0 else ""
    signe_profit  = "+" if profit_total >= 0 else ""
    signe_bankroll = "+" if bankroll_fin >= bankroll_deb else ""

    summary = f"""
=== Résumé Backtest ({total} paris analysés) ===

Métriques globales :
  ROI            : {signe_roi}{roi:.2f}%
  Profit total   : {signe_profit}{profit_total:.2f}€
  Mise totale    : {mise_total:.2f}€
  Win rate       : {win_rate:.1f}%  ({sum(won_flags)}/{total} paris gagnés)
  Edge moyen     : {edge_moyen:.3f}
  Odd moyen      : {odd_moyen:.2f}
  Bankroll début : {bankroll_deb:.2f}€
  Bankroll fin   : {bankroll_fin:.2f}€  ({signe_bankroll}{bankroll_fin - bankroll_deb:.2f}€)

Par league :
{chr(10).join(league_lines)}

Par type de pari :
{chr(10).join(outcome_lines)}
""".strip()

    logger.debug(f"read_backtest_results({n_recent}) → {total} lignes analysées")
    return summary

# ── Outil 4 : run_pipeline_step() ────────────────────────────────────────────

VALID_STEPS = ["dbt_seed", "dbt_run", "dbt_test", "train", "predict", "backtest"]

def run_pipeline_step(step: str) -> str:
    """
    Demande confirmation à l'utilisateur puis déclenche une étape du pipeline.

    La confirmation est gérée côté Python — Gemini n'en a pas conscience.
    L'étape est lancée via run_pipeline.py --step <step> en sous-processus.

    Args:
        step : nom de l'étape (dbt_seed / dbt_run / dbt_test / train / predict / backtest)

    Returns:
        Message décrivant l'issue : confirmé+résultat, annulé, ou erreur.
    """
    # ── Validation du nom d'étape ─────────────────────────────────────────────
    if step not in VALID_STEPS:
        return (
            f"Étape invalide : '{step}'. "
            f"Étapes disponibles : {', '.join(VALID_STEPS)}"
        )

    # ── Confirmation utilisateur ──────────────────────────────────────────────
    # On sort temporairement du flux agent pour interroger l'humain
    print(f"\n{'='*50}")
    print(f"  ⚠️  L'agent veut relancer l'étape : '{step}'")
    print(f"{'='*50}")

    try:
        answer = input("  Confirmer l'exécution ? (o/n) : ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        return "Action annulée (interruption utilisateur)."

    if answer not in ("o", "oui", "y", "yes"):
        logger.info(f"run_pipeline_step('{step}') annulé par l'utilisateur")
        return f"Action annulée par l'utilisateur. L'étape '{step}' n'a pas été relancée."

    # ── Exécution ─────────────────────────────────────────────────────────────
    logger.info(f"run_pipeline_step('{step}') confirmé — lancement...")
    print(f"\n  ▶ Lancement de l'étape '{step}'...\n")

    pipeline_script = ROOT_DIR / "pipelines" / "run_pipeline.py"

    try:
        result = subprocess.run(
            [sys.executable, str(pipeline_script), "--step", step],
            capture_output=True,
            text=True,
            cwd=str(ROOT_DIR),   # important : même logique que dans run_pipeline.py
            encoding="utf-8",        # ← forcer UTF-8
            errors="replace",
        )

        # Résumé du résultat pour Gemini
        status  = "succès" if result.returncode == 0 else "échec"
        # On extrait les dernières lignes stdout (résumé pipeline)
        stdout_tail = "\n".join(result.stdout.splitlines()[-20:]) if result.stdout else ""
        stderr_tail = "\n".join(result.stderr.splitlines()[-10:]) if result.stderr else ""

        output = f"Étape '{step}' terminée avec {status} (code {result.returncode}).\n"
        if stdout_tail:
            output += f"\nSortie :\n{stdout_tail}"
        if stderr_tail and result.returncode != 0:
            output += f"\nErreurs :\n{stderr_tail}"

        logger.info(f"run_pipeline_step('{step}') → {status}")
        return output

    except FileNotFoundError:
        return f"Erreur : run_pipeline.py introuvable à {pipeline_script}"
    except Exception as e:
        return f"Erreur lors du lancement de '{step}' : {e}"

# ── Outil 5 : read_dbt_logs() ─────────────────────────────────────────────────

def read_dbt_logs(command: str = "test", n_lines: int = 100) -> str:
    """
    Lit les dernières lignes du log du dernier run dbt (seed / run / test).

    Args:
        command : commande dbt concernée — "seed", "run", ou "test"
        n_lines : nombre de lignes à retourner depuis la fin (défaut 100)

    Returns:
        Contenu du fichier dbt_<command>_last.log, ou message d'erreur.
    """
    valid_commands = ("seed", "run", "test")
    if command not in valid_commands:
        return (
            f"Commande invalide : '{command}'. "
            f"Valeurs possibles : {', '.join(valid_commands)}"
        )

    log_path = ROOT_DIR / "logs" / f"dbt_{command}_last.log"

    if not log_path.exists():
        return (
            f"Fichier introuvable : {log_path}. "
            f"Le pipeline dbt {command} n'a peut-être pas encore tourné."
        )

    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    excerpt = lines[-n_lines:]
    logger.debug(f"read_dbt_logs({command}, {n_lines}) → {len(excerpt)} lignes lues")
    return "".join(excerpt) or "(fichier vide)"

# ── Outil 6 : get_dbt_test_results() ──────────────────────────────────────────

def get_dbt_test_results() -> str:
    """
    Parse dbt_test_last.log et retourne un résumé structuré des tests.

    Extrait :
      - Compteurs globaux : PASS / WARN / ERROR / FAIL
      - Liste des tests en échec avec leur message
      - Ligne de résumé finale de dbt

    Returns:
        Résumé lisible, prêt à être interprété par l'agent.
    """
    log_path = ROOT_DIR / "logs" / "dbt_test_last.log"

    if not log_path.exists():
        return (
            "Fichier dbt_test_last.log introuvable. "
            "Lance d'abord l'étape dbt_test via le pipeline."
        )

    content = log_path.read_text(encoding="utf-8", errors="replace")
    lines   = content.splitlines()

    if not lines:
        return "dbt_test_last.log est vide."

    # ── Compteurs ─────────────────────────────────────────────────────────────
    # dbt écrit des lignes du type :
    #   "PASS test_name"  /  "FAIL test_name"  /  "WARN test_name"  /  "ERROR test_name"
    counts = {"PASS": 0, "WARN": 0, "FAIL": 0, "ERROR": 0}
    failures = []   # lignes FAIL ou ERROR avec contexte

    for line in lines:
        stripped = line.strip()
        for status in counts:
            if stripped.startswith(status + " "):
                counts[status] += 1
                if status in ("FAIL", "ERROR"):
                    # On garde le nom du test (après le statut)
                    test_name = stripped[len(status):].strip()
                    failures.append(f"  {status} — {test_name}")
                break

    # ── Ligne de résumé finale dbt ────────────────────────────────────────────
    # dbt termine toujours par une ligne comme :
    #   "Done. PASS=230 WARN=0 ERROR=5 SKIP=0 TOTAL=235"
    summary_line = ""
    for line in reversed(lines):
        if "PASS=" in line and "TOTAL=" in line:
            summary_line = line.strip()
            break

    # ── Formatage ─────────────────────────────────────────────────────────────
    total = sum(counts.values())
    status_global = "✓ Tous les tests passent" if counts["FAIL"] == 0 and counts["ERROR"] == 0 else "✗ Des tests ont échoué"

    result = f"=== Résultats dbt test ===\n\n"
    result += f"Statut global : {status_global}\n"
    result += f"  PASS  : {counts['PASS']}\n"
    result += f"  WARN  : {counts['WARN']}\n"
    result += f"  FAIL  : {counts['FAIL']}\n"
    result += f"  ERROR : {counts['ERROR']}\n"

    if summary_line:
        result += f"\nRésumé dbt : {summary_line}\n"

    if failures:
        result += f"\nTests en échec ({len(failures)}) :\n"
        result += "\n".join(failures)
    else:
        result += "\nAucun test en échec.\n"

    logger.debug(f"get_dbt_test_results() → {total} tests parsés, {len(failures)} échecs")
    return result


# ── Déclarations des outils (ce que Gemini voit) ─────────────────────────────

TOOL_DECLARATIONS = [
    types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="read_logs",
            description=(
                "Lit les dernières lignes d'un fichier de log du pipeline. "
                "Utilise cet outil pour diagnostiquer les erreurs, vérifier "
                "le déroulement des étapes récentes, ou analyser les performances. "
                "Fichiers disponibles : pipeline.log, features.log, train.log, "
                "ingest.log, odds.log, backtest.log, process.log."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "n_lines": types.Schema(
                        type=types.Type.INTEGER,
                        description="Nombre de lignes à lire depuis la fin du fichier. Défaut : 100.",
                    ),
                    "log_file": types.Schema(
                        type=types.Type.STRING,
                        description="Nom du fichier de log dans le dossier logs/. Défaut : pipeline.log.",
                    ),
                },
                required=[],  # les deux paramètres ont des valeurs par défaut
            ),
        ),

        types.FunctionDeclaration(
            name="get_pipeline_status",
            description=(
                "Interroge l'API Prefect locale pour récupérer l'historique "
                "des runs du pipeline. Utilise cet outil pour savoir quand le "
                "pipeline a tourné pour la dernière fois, si des runs ont échoué, "
                "et combien de temps chaque exécution a pris. "
                "Nécessite que le serveur Prefect soit lancé (prefect server start)."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "n_runs": types.Schema(
                        type=types.Type.INTEGER,
                        description="Nombre de runs à récupérer. Défaut : 10.",
                    ),
                },
                required=[],
            ),
        ),

        types.FunctionDeclaration(
            name="read_backtest_results",
            description=(
                "Analyse les résultats du backtest de la stratégie de paris. "
                "Retourne les métriques clés : ROI, win rate, profit, edge moyen, "
                "ventilés par league et par type de pari (home/draw/away). "
                "Utilise cet outil pour évaluer si la stratégie est rentable, "
                "si les performances se dégradent, ou pour justifier un retraining."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "n_recent": types.Schema(
                        type=types.Type.INTEGER,
                        description="Nombre de paris récents à analyser. Défaut : 100.",
                    ),
                },
                required=[],
            ),
        ),

        types.FunctionDeclaration(
            name="run_pipeline_step",
            description=(
                "Relance une étape du pipeline. "
                "Quand l'utilisateur demande explicitement de relancer une étape, "
                "appelle cet outil DIRECTEMENT sans demander de confirmation textuelle. "
                "La confirmation utilisateur est gérée automatiquement par le système "
                "avant toute exécution réelle — tu n'as pas à la gérer toi-même. "
                "Étapes disponibles : dbt_seed, dbt_run, dbt_test, train, predict, backtest. "
                "Ordre naturel : dbt_seed → dbt_run → dbt_test → train → predict → backtest."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "step": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Nom de l'étape à relancer. "
                            "Valeurs possibles : dbt_seed, dbt_run, dbt_test, train, predict, backtest."
                        ),
                    ),
                },
                required=["step"],
            ),
        ),

        types.FunctionDeclaration(
            name="read_dbt_logs",
            description=(
                "Lit la sortie complète du dernier run d'une commande dbt. "
                "Utilise cet outil pour diagnostiquer pourquoi dbt seed, dbt run "
                "ou dbt test a échoué, ou pour voir le détail de l'exécution. "
                "Commandes disponibles : seed, run, test."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "command": types.Schema(
                        type=types.Type.STRING,
                        description="Commande dbt concernée : 'seed', 'run', ou 'test'. Défaut : 'test'.",
                    ),
                    "n_lines": types.Schema(
                        type=types.Type.INTEGER,
                        description="Nombre de lignes à lire depuis la fin. Défaut : 100.",
                    ),
                },
                required=[],
            ),
        ),

        types.FunctionDeclaration(
            name="get_dbt_test_results",
            description=(
                "Parse le dernier rapport dbt test et retourne un résumé structuré : "
                "compteurs PASS/WARN/FAIL/ERROR et liste des tests en échec. "
                "Préfère cet outil à read_dbt_logs quand tu veux évaluer la qualité "
                "des données — il te donne directement les chiffres clés sans lire "
                "des centaines de lignes brutes."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={},
                required=[],
            ),
        ),
        


    ])
]

# ── Registre Python ───────────────────────────────────────────────────────────
TOOL_REGISTRY: dict[str, callable] = {
    "read_logs": read_logs,
    "get_pipeline_status": get_pipeline_status,
    "read_backtest_results":   read_backtest_results,
    "run_pipeline_step":     run_pipeline_step,
    "read_dbt_logs":   read_dbt_logs,
    "get_dbt_test_results": get_dbt_test_results,
}


def dispatch_tool(name: str, args: dict) -> str:
    if name not in TOOL_REGISTRY:
        msg = f"Outil inconnu : '{name}'. Disponibles : {list(TOOL_REGISTRY.keys())}"
        logger.warning(msg)
        return msg

    logger.info(f"🔧 Outil appelé : {name}({args})")
    try:
        result = TOOL_REGISTRY[name](**args)
        logger.debug(f"Résultat {name} : {str(result)[:200]}")
        return str(result)
    except Exception as e:
        msg = f"Erreur lors de l'exécution de '{name}' : {e}"
        logger.error(msg)
        return msg

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Boucle ReAct
#
# Différence clé avec l'ancien SDK :
#   - On n'utilise plus ChatSession
#   - On gère manuellement l'historique dans une liste `contents`
#   - client.models.generate_content() à chaque tour
# ══════════════════════════════════════════════════════════════════════════════


def _generate_with_retry(client, model_name, contents, config, model_fallback="?",max_retries=3):
    """
    Appelle generate_content avec retry exponentiel sur les erreurs 503/429.
    """
    from google.genai import errors as genai_errors

    for attempt in range(max_retries):
        try:
            return client.models.generate_content(
                model=model_name,
                contents=contents,
                config=config,
            )
        except genai_errors.ServerError:
            # 503 — surcharge temporaire → on retente
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(f"Serveur indisponible, retry {attempt+1}/{max_retries} dans {wait}s...")
                print(f"\n  ⏳ Serveur surchargé, nouvelle tentative dans {wait}s...")
                time.sleep(wait)
            else:
                raise

        except genai_errors.ClientError as e:
            # 429 — distinguer quota court terme vs quota journalier
            match = re.search(r"retry in (\d+)s", str(e))
            wait_suggested = int(match.group(1)) if match else None

            if "RESOURCE_EXHAUSTED" in str(e) and wait_suggested and wait_suggested > 60:
                # Quota journalier épuisé — inutile de retenter
                raise RuntimeError(
                    f"Quota journalier épuisé pour {model_name}. "
                    f"Change de modèle dans config.yaml.\n"
                    f"Fallback disponible : {model_fallback}"
                ) from e
            elif attempt < max_retries - 1:
                # Rate limit court terme → on attend
                wait = wait_suggested or 2 ** attempt
                logger.warning(f"Rate limit, retry {attempt+1}/{max_retries} dans {wait}s...")
                print(f"\n  ⏳ Rate limit, nouvelle tentative dans {wait}s...")
                time.sleep(wait)
            else:
                raise


def run_agent_turn(
    client: genai.Client,
    model_name: str,
    contents: list,          # historique complet de la conversation
    user_message: str,
    max_turns: int,
    verbose: bool,
    model_fallback: str = "?",
) -> str:
    """
    Exécute un tour utilisateur → agent avec boucle ReAct.

    Avec le nouveau SDK, on gère l'historique manuellement :
    chaque message (user, model, tool) est ajouté à `contents`.
    Cela nous donne un contrôle total sur le contexte envoyé au modèle.
    """
    logger.info(f"Nouveau tour : {user_message[:100]}")

    # Ajout du message utilisateur à l'historique
    contents.append(types.Content(
        role="user",
        parts=[types.Part(text=user_message)]
    ))

    # Config de génération : on branche les outils si disponibles
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=TOOL_DECLARATIONS if TOOL_DECLARATIONS else None,
    )

    for turn in range(max_turns):

        response = _generate_with_retry(client, model_name, contents, config, model_fallback=model_fallback)

        candidate = response.candidates[0]
        content   = candidate.content  # objet Content(role="model", parts=[...])

        # Ajout de la réponse modèle à l'historique
        contents.append(content)

        # ── CAS 1 : Function calls ────────────────────────────────────────────
        function_calls = [
            p for p in content.parts
            if p.function_call is not None
        ]

        if function_calls:
            tool_response_parts = []

            for part in function_calls:
                fc        = part.function_call
                tool_name = fc.name
                tool_args = dict(fc.args) if fc.args else {}

                if verbose:
                    print(f"\n  🤔 [Raisonnement] → appel : {tool_name}({tool_args})")

                result = dispatch_tool(tool_name, tool_args)

                if verbose:
                    preview = result[:300] + "..." if len(result) > 300 else result
                    print(f"  📋 [Résultat {tool_name}] {preview}")

                tool_response_parts.append(
                    types.Part(
                        function_response=types.FunctionResponse(
                            name=tool_name,
                            response={"result": result},
                        )
                    )
                )

            # Ajout des résultats outils à l'historique
            contents.append(types.Content(
                role="user",
                parts=tool_response_parts,
            ))
            continue  # on reboucle — Gemini lit les résultats

        # ── CAS 2 : Réponse textuelle ─────────────────────────────────────────
        text_parts = [p.text for p in content.parts if p.text]
        if text_parts:
            final_text = "\n".join(text_parts)
            logger.info(f"Réponse finale en {turn + 1} tour(s)")
            return final_text

        # ── CAS 3 : Réponse inattendue ────────────────────────────────────────
        logger.warning(f"Tour {turn + 1} : réponse inattendue")
        break

    return "⚠️ L'agent n'a pas pu produire de réponse dans la limite de tours autorisés."

def run_post_pipeline_analysis(cfg: dict) -> str:
    """
    Analyse automatique post-pipeline, conçue pour être appelée
    depuis run_pipeline.py comme task Prefect finale.

    Pas d'interaction humaine — l'agent travaille en autonomie
    et retourne une synthèse textuelle.
    """
    client, model_name = init_client(cfg)
    model_fallback = cfg.get("agent", {}).get("model_fallback", "gemini-2.0-flash-lite")
    max_turns = cfg.get("agent", {}).get("max_turns", 10)

    question = """
    Le pipeline 3-Étoiles vient de terminer son exécution.
    
    Effectue une analyse complète en suivant ces étapes :
    1. Consulte l'historique Prefect pour voir le statut du dernier run
    2. Lis les logs pipeline pour identifier erreurs ou anomalies
    3. Consulte les résultats du backtest pour évaluer la performance
    
    Puis produis une synthèse structurée avec :
    - Statut global du run (succès / échec partiel / échec)
    - Points d'attention identifiés
    - Performance de la stratégie (ROI, paris)
    - Recommandation : faut-il relancer une étape ? Laquelle et pourquoi ?
    
    Termine par une conclusion en une phrase.
    """

    contents: list = []
    synthese = run_agent_turn(
            client, model_name, contents,
            question, max_turns, verbose=False,
            model_fallback=model_fallback,
        )
    try:
        create_markdown_artifact(
            key="agent-synthese",
            markdown=f"# 🤖 Synthèse Agent Gemini\n\n{synthese}",
            description="Synthèse de l'analyse post-pipeline",
        )
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Initialisation
# ══════════════════════════════════════════════════════════════════════════════

def init_client(cfg: dict) -> tuple[genai.Client, str]:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "Variable d'environnement GOOGLE_API_KEY non définie.\n"
            "Crée un fichier .env avec : GOOGLE_API_KEY=ta_clé"
        )

    client     = genai.Client(api_key=api_key)
    model_name = cfg.get("agent", {}).get("model", "gemini-2.5-flash")
    


    logger.info(f"Gemini initialisé : {model_name}")
    return client, model_name


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — Point d'entrée CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    global PREFECT_API
    cfg       = load_config()
    agent_cfg = cfg.get("agent", {})

    setup_logger(
        log_file=agent_cfg.get("log_file", "logs/agent.log"),
        verbose=agent_cfg.get("verbose", True),
    )

    max_turns = agent_cfg.get("max_turns", 10)
    verbose   = agent_cfg.get("verbose", True)
    model_fallback = agent_cfg.get("model_fallback", "gemini-2.0-flash-lite")
    PREFECT_API = cfg.get("agent", {}).get("prefect_api", "http://127.0.0.1:4200/api")

    client, model_name = init_client(cfg)

    # L'historique est une liste qu'on passe à chaque appel
    # C'est nous qui le gérons (contrairement à ChatSession de l'ancien SDK)
    contents: list = []

    # ── Mode argument unique ──────────────────────────────────────────────────
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        print(f"\n🤖 Agent : ", end="", flush=True)
        answer = run_agent_turn(client, model_name, contents, question, max_turns, verbose, model_fallback)
        print(answer)
        return

    # ── Mode interactif ───────────────────────────────────────────────────────
    print("\n╔══════════════════════════════════════╗")
    print("║   Agent Gemini — Pipeline 3-Étoiles  ║")
    print("║   'exit' ou Ctrl+C pour quitter       ║")
    print("╚══════════════════════════════════════╝\n")

    while True:
        try:
            user_input = input("Vous : ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\nAu revoir.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "q"):
            print("Au revoir.")
            break

        print(f"\n🤖 Agent : ", end="", flush=True)
        answer = run_agent_turn(client, model_name, contents, user_input, max_turns, verbose, model_fallback)
        print(answer)
        print()



if __name__ == "__main__":
    main()