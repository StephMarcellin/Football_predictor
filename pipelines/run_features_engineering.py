#!/usr/bin/env python3
"""
run_features_engineering.py — Orchestrateur PHASE 3 : Feature Engineering
=========================================================================
Transforme données Silver en features ML via dbt + modèles Python :
    1. dbt_run       : intermediate_team_match_backbone + intermediate + stubs (xt_actions, xgot_features)
    2. xt_grid.py    : calcule grille Expected Threat 2D (~3h)
    3. xgot_score.py : applique modèle xGOT keeper (~2h)
    4. knn_impute.py : impute valeurs manquantes (~1h)
    5. dbt_run       : gold.* (10 modèles, 250+ features)
    6. dbt_test      : validation + fail-fast

Sortie : intermediate.* (25+) + gold.* (10) + machine_learning.* (artefacts)
Durée  : ~7h

Usage :
    python run_features_engineering.py                    # pipeline complet
    python run_features_engineering.py --step dbt_run     # étape unique
    python run_features_engineering.py --from xt_grid     # reprend depuis xt_grid
    python run_features_engineering.py --flow daily       # cadence quotidienne (TABLES_UPDATE)
    python run_features_engineering.py --flow yearly      # cadence annuelle (refit xT + xGOT)
    python run_features_engineering.py --dry-run          # simule sans exécuter
    python run_features_engineering.py --serve            # scheduler Prefect (bloquant)

    Le flux `daily` suppose que models/xgot.joblib et machine_learning.xt_grid
    existent déjà (xgot_score et intermediate_xt_contributions les consomment).
    Sur une base neuve, lance `--flow yearly` d'abord pour produire ces artefacts.
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
from spark.spark_helpers import run_spark_job

from validation.run_validation import (
    run_validate_silver,
    run_validate_intermediate,
    run_validate_gold,
)

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

# Scripts Spark — lancés en subprocess via spark_helpers, pas importés.
# export_to_parquet et check_spark_outputs sont du DuckDB pur et pourraient
# tourner en direct ; on les passe quand même par le même chemin pour que les
# trois étapes se comportent et se loguent de façon identique.
SPARK_EXPORT = "export_to_parquet.py"
SPARK_EVENTS = "spark_events.py"
SPARK_CHECK  = "check_spark_outputs.py"
SPARK_CHAINS = "spark_possession_chains.py"


# ══════════════════════════════════════════════════════════════════════════════
# Blocs réutilisables
# ══════════════════════════════════════════════════════════════════════════════
# L'ordre est défini UNE fois ici ; les flux (daily/yearly) y piochent par nom.
#
# Cadences (voir LINEAGE_TABLES_ET_FREQUENCES.md du vault Obsidian) :
#   - daily  : ce qui bouge chaque jour (nouveaux matchs WhoScored)
#   - yearly : refit des artefacts stables (grille xT, modèle xGOT)
# ══════════════════════════════════════════════════════════════════════════════

TABLES_UPDATE = [
    "validate_silver",                                # gate d'entrée
    "dbt_intermediate_match_index",                     
    "export_to_parquet",                               # export Silver → Parquet
    "spark_events",                                    # calcule les événements (Spark)
    "check_spark_outputs",                             # validation des exports Spark
    "spark_possession_chains",                         # chaînes de possession (Spark)
    "dbt_intermediate_base",
    "dbt_intermediate_by_season",                      # modèles par match, saison par saison
    "dbt_ml_features",
    # "validate_intermediate",                          # après matérialisation intermediate
    "xgot_score",
    "dbt_intermediate_downstream",
    "validate_intermediate",                          # après matérialisation intermediate
    "dbt_gold_base",
    "knn_impute",
    "dbt_joueur_match",
    "dbt_test",
    "dbt_test_check",
    "validate_gold",                                  # après matérialisation gold
]

# refit des artefacts coûteux (grille xT ~3h, modèle xGOT ~30min)
# dbt_test à la fin pour valider les tables aval qui consomment ces artefacts.
TABLES_YEARLY = ["xt_grid", "xgot_train"]

FLOWS = {
    "daily":  TABLES_UPDATE,          # transform + score, PAS d'entraînement
    "yearly": TABLES_YEARLY,          # refit artefacts coûteux (xT, xGOT)
}

STEP_NAMES = TABLES_UPDATE + TABLES_YEARLY  # ordre d'exécution complet

# ══════════════════════════════════════════════════════════════════════════════
# Étapes
# ══════════════════════════════════════════════════════════════════════════════

def _seasons_spark_wrote() -> list[str]:
    """
    Saisons que spark_events.py vient d'écrire, d'après le rapport de CE run.

    Rapport absent ou illisible → liste vide, donc comportement inchangé. On ne
    force pas le full-refresh « par précaution » : spark_events écrit ce fichier
    à chaque exécution, y compris à vide, donc son absence signifie que Spark
    n'a pas tourné dans ce flux.
    """
    import json

    report = ROOT_DIR / "data" / "spark_out" / "_run_report.json"
    try:
        return list(json.loads(report.read_text(encoding="utf-8")).get("seasons") or [])
    except Exception as e:  # fichier absent, JSON tronqué, etc.
        logger.debug(f"Rapport Spark illisible ({e}) — pas de bascule full-refresh.")
        return []


def _seasons_deja_dans_dbt(seasons: list[str]) -> list[str]:
    """
    Parmi `seasons`, celles pour lesquelles la couche intermediate contient DÉJÀ
    des lignes.

    C'est LE bon critère, et pas « saison déjà présente en sortie Spark » :
    Spark recalcule une saison ENTIÈRE, donc tout match de cette saison déjà
    présent dans les tables dbt a pu changer de contenu. Or le filtre
    incrémental de ces modèles est un anti-join sur match_id
    (`WHERE t.match_id IS NULL`) : un match déjà présent est écarté AVANT toute
    fusion, même sur un modèle à unique_key. La correction ne se propage donc
    jamais, et seul un --full-refresh la fait descendre.

    Table témoin : intermediate_shots, incrémentale et porteuse de str_season.
    Absente (premier run) → liste vide, pas de bascule : il n'y a rien à
    corriger dans une table qui n'existe pas.
    """
    if not seasons:
        return []

    import duckdb
    import yaml

    try:
        with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
            db = ROOT_DIR / yaml.safe_load(f)["paths"]["db"]
        con = duckdb.connect(str(db), read_only=True)
        try:
            rows = con.sql(
                "SELECT DISTINCT str_season FROM intermediate.intermediate_shots"
            ).fetchall()
        finally:
            con.close()
        return sorted(set(seasons) & {r[0] for r in rows})
    except Exception as e:
        logger.debug(f"Témoin intermediate_shots illisible ({e}) — pas de bascule.")
        return []


def run_dbt_intermediate_base(select: str, exclude: str, full_refresh: bool) -> None:
    """
    Étape `dbt_intermediate_base`, avec bascule automatique en --full-refresh.

    POURQUOI UN WRAPPER plutôt que de calculer la valeur dans build_steps() :
    le dictionnaire d'étapes est construit AVANT que Spark tourne, donc un
    `full_refresh` figé là serait décidé trop tôt. Ici la décision est prise à
    l'appel, après spark_events.

    POURQUOI LA BASCULE : le filtre incrémental des modèles intermediate est un
    anti-join sur match_id (`WHERE t.match_id IS NULL`). Un match déjà présent
    est donc écarté AVANT toute fusion, même sur un modèle à unique_key — une
    CORRECTION de ce match ne se propage jamais en incrémental. C'est le
    mécanisme qui a laissé 467 544 lignes à team_id NULL dans
    whoscored_team_match_minute après la correction de la cause en amont.
    Quand Spark recalcule une saison déjà en sortie, l'aval doit donc être
    reconstruit, pas complété.
    """
    ecrites = _seasons_spark_wrote()
    forcee  = _seasons_deja_dans_dbt(ecrites)

    if full_refresh:
        logger.info("--full-refresh demandé explicitement.")
    elif forcee:
        logger.warning(
            f"Spark a recalculé {', '.join(forcee)}, dont la couche intermediate "
            f"contient déjà des lignes → bascule automatique en --full-refresh. "
            f"L'incrémental écarterait ces matchs par anti-join et ne "
            f"propagerait aucune correction."
        )
    elif ecrites:
        logger.info(
            f"Spark a écrit {', '.join(ecrites)} : aucune de ces saisons n'est "
            f"déjà dans intermediate → incrémental suffisant."
        )

    run_dbt_run(select=select, exclude=exclude,
                full_refresh=full_refresh or bool(forcee))


def _by_season_models() -> list[str]:
    """Noms des modèles dbt tagués `by_season` (lus dans leur config, sans lancer dbt)."""
    models_dir = ROOT_DIR / "dbt_project" / "models"
    return sorted(f.stem for f in models_dir.rglob("*.sql")
                  if "tags=['by_season']" in f.read_text(encoding="utf-8", errors="replace"))


def _by_season_tables_missing(names: list[str]) -> list[str]:
    """Modèles by_season dont la table n'existe pas encore (premier run).

    Sans table, dbt construit tout d'un coup : exactement le cas qui sature la
    mémoire. On bascule alors en boucle par saison, avec full-refresh à la première.
    """
    import duckdb
    import yaml

    try:
        with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
            db = ROOT_DIR / yaml.safe_load(f)["paths"]["db"]
        con = duckdb.connect(str(db), read_only=True)
        try:
            rows = con.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'intermediate'"
            ).fetchall()
        finally:
            con.close()
        return sorted(set(names) - {r[0] for r in rows})
    except Exception as e:
        logger.debug(f"Base illisible ({e}) — on suppose les tables absentes.")
        return list(names)


def _chain_seasons() -> list[str]:
    """Saisons disponibles en sortie Spark des chaînes (une partition str_season=... chacune)."""
    out = ROOT_DIR / "data" / "spark_out" / "int_possession_chains"
    return sorted(d.name.split("=", 1)[1] for d in out.glob("str_season=*") if d.is_dir())


def run_dbt_intermediate_by_season(exclude: str, full_refresh: bool) -> None:
    """
    Étape `dbt_intermediate_by_season` : les modèles tagués `by_season`, puis leurs
    descendants dans `intermediate`.

    POURQUOI : ces modèles sont incrémentaux par match. Les construire sur 18 saisons
    d'un coup sature la mémoire de DuckDB (shot_creating_actions : 1 h 57) alors qu'une
    saison prend 2-3 s. On boucle donc sur les saisons avec `only_season`.

    DEUX MODES :
      - reconstruction (full_refresh demandé, saison déjà en base recalculée par Spark,
        ou table absente) : une passe par saison ; la PREMIÈRE vide les tables
        (--full-refresh), les suivantes s'y ajoutent. Un échec ne perd que la saison
        en cours : les précédentes sont écrites.
      - quotidien : une seule passe sans `only_season` (critère scraped_at / anti-join
        habituel). Peu de matchs : pas besoin de découper.

    ORDRE : cette étape suit dbt_intermediate_base (les parents des modèles tagués y
    sont construits) et précède dbt_ml_features (qui lit `shots`).
    """
    modeles = _by_season_models()
    ecrites = _seasons_spark_wrote()
    forcee  = _seasons_deja_dans_dbt(ecrites)
    absents = _by_season_tables_missing(modeles)
    rebuild = full_refresh or bool(forcee) or bool(absents)

    # Descendants dans intermediate (hors modèles exclus de la base, exécutés plus loin).
    desc_select  = "path:models/intermediate,tag:by_season+"
    desc_exclude = f"tag:by_season {exclude}".strip()

    if not rebuild:
        logger.info("Mode quotidien : une passe sans only_season.")
        run_dbt_run(select="tag:by_season", full_refresh=False)
        run_dbt_run(select=desc_select, exclude=desc_exclude, full_refresh=False)
        return

    raison = ("--full-refresh demandé" if full_refresh else
              f"Spark a recalculé {', '.join(forcee)}" if forcee else
              f"tables absentes : {', '.join(absents)}")
    saisons = _chain_seasons()
    if not saisons:
        raise RuntimeError("Aucune saison dans int_possession_chains : lance d'abord "
                           "l'étape spark_possession_chains.")
    logger.warning(f"Reconstruction par saison ({raison}) : {len(saisons)} saisons, "
                   f"{len(modeles)} modèles.")
    for i, saison in enumerate(saisons, 1):
        logger.info(f"══ [{i}/{len(saisons)}] {saison} ══")
        run_dbt_run(select="tag:by_season", full_refresh=(i == 1),
                    extra_vars={"only_season": saison})
    # Les descendants lisent les tables complètes : reconstruits UNE fois, à la fin.
    run_dbt_run(select=desc_select, exclude=desc_exclude, full_refresh=True)


def build_steps(cfg: dict, full_refresh: bool = False) -> dict:
    """
    Construit le dictionnaire ordonné des étapes de feature engineering (Mode Initial).

    Les imports sont différés ici pour ne charger les dépendances lourdes
    qu'au moment de l'exécution.
    """
    mod_xt         = import_from_path("xt_grid_mod", MOD_XT_GRID)
    mod_xgot_train = import_from_path("xgot_train_mod", MOD_XGOT_TRAIN)
    mod_xgot_score = import_from_path("xgot_score_mod", MOD_XGOT_SCORE)
    mod_knn        = import_from_path("knn_impute_mod", MOD_KNN)

    return {
        # ── 0. Gate d'entrée (validation Silver) ───────────────────────────────
        "validate_silver": {
            "fn": run_validate_silver,
            "kwargs": {},
            "critical": True,
        },

        "dbt_intermediate_match_index": {
            "fn": run_dbt_run,
            "kwargs": {
                "select": "intermediate.intermediate_whoscored_match_bridge",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 0b. Frontière DuckDB → Spark (ADR-010) ───────────────────────────
        "export_to_parquet": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_EXPORT, "extra_args": ["--clean", "--strict"]},
            "critical": True,
        },
        # Produit intermediate_whoscored_events / intermediate_whoscored_event_qualifiers / intermediate_whoscored_events_enriched en
        # Parquet. Les vues dbt du schéma intermediate lisent ces fichiers.
        "spark_events": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_EVENTS},
            "critical": True,
        },
        # Une vue ne valide rien : sur un Parquet absent ou vide, CREATE VIEW
        # réussit et l'erreur ne surgit qu'au premier modèle aval. Cette porte
        # transforme une panne silencieuse en échec explicite, au bon endroit.
        "check_spark_outputs": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_CHECK, "extra_args": ["--min-rows", "1000"]},
            "critical": True,
        },
        # Chaînes de possession (ex-modèle dbt de 2 h 30+). Lit les Parquet de
        # spark_events ; --from-report ne traite que les saisons que spark_events
        # vient d'écrire + celles qui manquent en sortie. Doit précéder
        # dbt_intermediate_base : le modèle dbt relit ce Parquet.
        "spark_possession_chains": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_CHAINS, "extra_args": ["--from-report"]},
            "critical": True,
        },
        # ── 1. Intermédiaires de base (hors dépendances Python/Downstream) ──
        "dbt_intermediate_base": {
            "fn": run_dbt_intermediate_base,
            "kwargs": {
                "select": "intermediate.*",
                "exclude": "tag:by_season+ intermediate_xt_contributions intermediate_keeper_shots_faced intermediate_keeper_season_psxg",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 1b. Modèles par match, construits SAISON PAR SAISON ─────────────────
        # 20 modèles tagués by_season + leurs descendants intermediate. Après la base
        # (leurs parents), avant dbt_ml_features (xgot_features lit shots).
        "dbt_intermediate_by_season": {
            "fn": run_dbt_intermediate_by_season,
            "kwargs": {
                "exclude": "intermediate_xt_contributions intermediate_keeper_shots_faced intermediate_keeper_season_psxg",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 2. Features ML (tables SQL du schéma ML) ────────────────────────
        "dbt_ml_features": {
            "fn": run_dbt_run,
            "kwargs": {
                "select": "machine_learning.*",
                "exclude": "xt_grid xgot_predictions player_style_clusters zonal_profiles_imputed",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 3. Grille xT (Py) ────────────────────────────────────────────────
        "xt_grid": {
            "fn": mod_xt.main,
            "kwargs": {},
            "critical": True,
        },

        # ── 4 & 5. Chaîne xGOT : Entraînement et Scoring (Py) ────────────────
        "xgot_train": {
            "fn": mod_xgot_train.run,
            "kwargs": {},
            "critical": True,
        },
        "xgot_score": {
            "fn": mod_xgot_score.main,
            "kwargs": {},
            "critical": True,
        },

        # ── 6. Intermédiaires aval (dépendent de xt_grid et xgot_predictions) ──
        "dbt_intermediate_downstream": {
            "fn": run_dbt_run,
            "kwargs": {
                "select": "intermediate_xt_contributions intermediate_keeper_shots_faced intermediate_keeper_season_psxg",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 7. Gate de validation Intermediate ──────────────────────────────
        "validate_intermediate": {
            "fn": run_validate_intermediate,
            "kwargs": {},
            "critical": True,
        },

        # ── 8. Gold base (hors gold_player_match_scorer) ─────────────────────────────────
        "dbt_gold_base": {
            "fn": run_dbt_run,
            "kwargs": {
                "select": "gold.*",
                "exclude": "gold_player_match_scorer",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 9. Imputation KNN (Py) ───────────────────────────────────────────
        "knn_impute": {
            "fn": mod_knn.main,
            "kwargs": {"write": True},
            "critical": True,
        },

        # ── 10. Gold final (dépend des clusters/profils KNN) ─────────────────
        "dbt_joueur_match": {
            "fn": run_dbt_run,
            "kwargs": {
                "select": "gold.gold_player_match_scorer",
                "full_refresh": full_refresh,
            },
            "critical": True,
        },

        # ── 11, 12 & 13. Tests & Validations finales ────────────────────────
        "dbt_test": {
            "fn": run_dbt_test,
            "kwargs": {},
            "critical": False,
        },
        "dbt_test_check": {
            "fn": check_dbt_test_results,
            "kwargs": {},
            "critical": True,
        },
        "validate_gold": {
            "fn": run_validate_gold,
            "kwargs": {},
            "critical": True,
        },
    }

def build_table_steps(cfg: dict, full_refresh: bool = False) -> dict:
    """Catalogue des steps de construction des tables (blocs --flow daily / yearly).

    Les frontières de phase autour du KNN/xgot viennent du DAG dbt
    (opérateur '+' : intermediate_keeper_shots_faced+ = la chaîne gardien jusqu'aux marts ;
    gold_player_match_scorer+ = jusqu'à mart_scorers).
    """
    mod_xt         = import_from_path("xt_grid_mod", MOD_XT_GRID)
    mod_xgot_train = import_from_path("xgot_train_mod", MOD_XGOT_TRAIN)
    mod_xgot_score = import_from_path("xgot_score_mod", MOD_XGOT_SCORE)
    mod_knn        = import_from_path("knn_impute_mod", MOD_KNN)

    return {
        # ── flow daily (TABLES_UPDATE) ───────────────────────────────────────
        # ── 0. Gate d'entrée (validation Silver) ───────────────────────────────
        "validate_silver": {
                "fn": run_validate_silver,
                "kwargs": {},
                "critical": True,
            },

        "dbt_intermediate_match_index": {
                "fn": run_dbt_run,
                "kwargs": {
                    "select": "intermediate.intermediate_whoscored_match_bridge",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },
        # ── 0b. Frontière DuckDB → Spark (ADR-010) ───────────────────────────
        "export_to_parquet": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_EXPORT, "extra_args": ["--clean", "--strict"]},
            "critical": True,
        },
        # Produit intermediate_whoscored_events / intermediate_whoscored_event_qualifiers / intermediate_whoscored_events_enriched en
        # Parquet. Les vues dbt du schéma intermediate lisent ces fichiers.
        "spark_events": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_EVENTS},
            "critical": True,
        },
        # Une vue ne valide rien : sur un Parquet absent ou vide, CREATE VIEW
        # réussit et l'erreur ne surgit qu'au premier modèle aval. Cette porte
        # transforme une panne silencieuse en échec explicite, au bon endroit.
        "check_spark_outputs": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_CHECK, "extra_args": ["--min-rows", "1000"]},
            "critical": True,
        },
        # Chaînes de possession (ex-modèle dbt de 2 h 30+). Lit les Parquet de
        # spark_events ; --from-report ne traite que les saisons que spark_events
        # vient d'écrire + celles qui manquent en sortie. Doit précéder
        # dbt_intermediate_base : le modèle dbt relit ce Parquet.
        "spark_possession_chains": {
            "fn": run_spark_job,
            "kwargs": {"script_name": SPARK_CHAINS, "extra_args": ["--from-report"]},
            "critical": True,
        },
            # ── 1. Intermédiaires de base (hors dépendances Python/Downstream) ──
            "dbt_intermediate_base": {
                "fn": run_dbt_intermediate_base,
                "kwargs": {
                    "select": "intermediate.*",
                    "exclude": "tag:by_season+ intermediate_xt_contributions intermediate_keeper_shots_faced intermediate_keeper_season_psxg",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },

            # ── 1b. Modèles par match, construits SAISON PAR SAISON ─────────────────
            # 20 modèles tagués by_season + leurs descendants intermediate. Après la base
            # (leurs parents), avant dbt_ml_features (xgot_features lit shots).
            "dbt_intermediate_by_season": {
                "fn": run_dbt_intermediate_by_season,
                "kwargs": {
                    "exclude": "intermediate_xt_contributions intermediate_keeper_shots_faced intermediate_keeper_season_psxg",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },
    
            # ── 2. Features ML (tables SQL du schéma ML) ────────────────────────
            "dbt_ml_features": {
                "fn": run_dbt_run,
                "kwargs": {
                    "select": "machine_learning.*",
                    "exclude": "xt_grid xgot_predictions player_style_clusters zonal_profiles_imputed",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },
    
            # ── 3. Grille xT (Py) ────────────────────────────────────────────────
            "xt_grid": {
                "fn": mod_xt.main,
                "kwargs": {},
                "critical": True,
            },
    

    
            # ── 6. Intermédiaires aval (dépendent de xt_grid et xgot_predictions) ──
            "dbt_intermediate_downstream": {
                "fn": run_dbt_run,
                "kwargs": {
                    "select": "intermediate_xt_contributions intermediate_keeper_shots_faced intermediate_keeper_season_psxg",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },
    
            # ── 7. Gate de validation Intermediate ──────────────────────────────
            "validate_intermediate": {
                "fn": run_validate_intermediate,
                "kwargs": {},
                "critical": True,
            },
    
            # ── 8. Gold base (hors gold_player_match_scorer) ─────────────────────────────────
            "dbt_gold_base": {
                "fn": run_dbt_run,
                "kwargs": {
                    "select": "gold.*",
                    "exclude": "gold_player_match_scorer",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },
    
            # ── 9. Imputation KNN (Py) ───────────────────────────────────────────
            "knn_impute": {
                "fn": mod_knn.main,
                "kwargs": {"write": True},
                "critical": True,
            },
    
            # ── 10. Gold final (dépend des clusters/profils KNN) ─────────────────
            "dbt_joueur_match": {
                "fn": run_dbt_run,
                "kwargs": {
                    "select": "gold.gold_player_match_scorer",
                    "full_refresh": full_refresh,
                },
                "critical": True,
            },
    
            # ── 11, 12 & 13. Tests & Validations finales ────────────────────────
            "dbt_test": {
                "fn": run_dbt_test,
                "kwargs": {},
                "critical": False,
            },
            "dbt_test_check": {
                "fn": check_dbt_test_results,
                "kwargs": {},
                "critical": True,
            },
            "validate_gold": {
                "fn": run_validate_gold,
                "kwargs": {},
                "critical": True,
            },


        # ── flow yearly (TABLES_YEARLY) ──────────────────────────────────────
        # ── 4 & 5. Chaîne xGOT : Entraînement et Scoring (Py) ────────────────
            "xgot_train": {
                "fn": mod_xgot_train.run,
                "kwargs": {},
                "critical": True,
            },
            "xgot_score": {
                "fn": mod_xgot_score.main,
                "kwargs": {},
                "critical": True,
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




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur PHASE 3 — Feature Engineering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Exemples :
        python run_features_engineering.py                    # complet
        python run_features_engineering.py --step dbt_run     # une étape
        python run_features_engineering.py --from xgot_score  # reprend depuis
        python run_features_engineering.py --flow daily       # cadence quotidienne
        python run_features_engineering.py --flow yearly      # refit xT + xGOT
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
    parser.add_argument("--flow", choices=["daily", "yearly"],
                        help="Cadence : daily=TABLES_UPDATE (features quotidiennes), "
                             "yearly=TABLES_YEARLY (refit xT + xGOT)")
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
        print("\nFlux disponibles :")
        for flow_name, step_list in FLOWS.items():
            print(f"  --flow {flow_name} : {' → '.join(step_list)}")
        print()
        return

    cfg = load_config()
    pipeline_cfg = cfg.get("pipeline_main", {})

    run_step_task = make_run_step_task(
        retries=pipeline_cfg.get("retries", 2),
        retry_delay_seconds=pipeline_cfg.get("retry_delay_seconds", 30),
    )

    # ── Flux composés (--flow daily / --flow yearly) ─────────────────────────
    if args.flow:
        table_steps  = build_table_steps(cfg, full_refresh=args.full_refresh)
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
        # Ordre = celui du dictionnaire d'étapes (= ordre d'un run complet), PAS celui de
        # STEP_NAMES : celle-ci range xt_grid / xgot_train à la fin et la reprise les
        # exécuterait après les étapes qui en dépendent.
        ordre = list(all_steps)
        if args.from_step not in ordre:
            parser.error(f"--from {args.from_step} : étape absente de ce mode "
                         f"({', '.join(ordre)})")
        steps_to_run = {n: all_steps[n] for n in ordre[ordre.index(args.from_step):]}
    else:
        steps_to_run = all_steps

    results = run_features_flow(steps_to_run, dry_run=args.dry_run, run_step_task=run_step_task)
    print_summary(results, title="RÉSUMÉ FEATURES ENGINEERING")

    failed = [r for r in results if r["status"] == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
