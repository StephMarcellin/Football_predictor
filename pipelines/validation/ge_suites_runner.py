"""
ge_suites_runner.py — Helper unique pour lancer toutes les suites GE d'une couche
==================================================================================
Parcourt le dossier tests/great_expectations/<layer>/, exécute chaque suite YAML
via notre runner pandas-natif (tests/great_expectations/runner.py — le même que
celui appelé par les bridges pytest), agrège les résultats et applique un
fail-fast en fin de boucle.

Utilisé par les 3 wrappers ge_silver.py / ge_intermediate.py / ge_gold.py,
eux-mêmes appelés depuis le flow Prefect via run_validation.py.

Pourquoi ne pas appeler pytest depuis Prefect ?
    pytest est un runner de tests, pas une lib de validation. L'appeler
    depuis un flow serait laid (parsing stdout, gestion couleurs ANSI, exit
    codes). run_suite() est la fonction Python que les bridges appellent
    déjà — on la réutilise directement, sans intermédiaire.

Fail-fast en fin de boucle (et non au premier échec) : on veut voir TOUTES
les suites cassées d'un coup, pas se faire arrêter à la première pour
découvrir la suivante après un fix. Debug plus rapide.

Usage typique :
    from pipelines.validation.ge_suites_runner import run_ge_suites_for_layer

    run_ge_suites_for_layer("silver")         # 19 suites
    run_ge_suites_for_layer("intermediate")   # 47 suites
    run_ge_suites_for_layer("gold")           # 14 suites
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from tests.great_expectations.runner import run_suite


# ── Chemins ───────────────────────────────────────────────────────────────────
# Ce fichier est à pipelines/validation/ge_suites_runner.py.
# ROOT_DIR remonte de 3 niveaux : validation/ → pipelines/ → racine repo.
ROOT_DIR    = Path(__file__).resolve().parent.parent.parent
SUITES_ROOT = ROOT_DIR / "tests" / "great_expectations"


def run_ge_suites_for_layer(layer: str) -> None:
    """
    Exécute toutes les suites GE d'une couche.

    Args:
        layer: nom du sous-dossier sous tests/great_expectations/.
               Ex : 'silver', 'intermediate', 'gold', 'machine_learning'.

    Raises:
        FileNotFoundError : si le dossier tests/great_expectations/<layer>
                            n'existe pas.
        RuntimeError      : si au moins une suite retourne success=False
                            (c-à-d au moins une expectation severity=error
                            a échoué).

    Warnings :
        - Un dossier vide (0 fichier .yml) est traité comme un no-op :
          on log un warning et on retourne sans erreur. Utile pendant
          l'écriture des suites d'une nouvelle couche.
        - Les expectations severity=warn ne provoquent PAS de RuntimeError.
          Elles sont comptées dans le bilan mais laissent le pipeline
          continuer (dettes documentées type A-004, A-011, A-018...).
    """
    layer_dir = SUITES_ROOT / layer
    if not layer_dir.exists():
        raise FileNotFoundError(f"Dossier de suites GE introuvable : {layer_dir}")

    suite_paths = sorted(layer_dir.glob("*.yml"))
    if not suite_paths:
        logger.warning(f"Aucune suite GE trouvée dans {layer_dir} — skip.")
        return

    logger.info(
        f"═══ Validation GE — couche {layer.upper()} "
        f"({len(suite_paths)} suites) ═══"
    )

    failures: list[str] = []    # suites avec au moins une expectation `error` cassée
    total_pass = 0
    total_warn = 0
    total_err  = 0

    for suite_path in suite_paths:
        result = run_suite(suite_path)

        n_pass = sum(1 for r in result.results if r.success)
        n_warn = sum(1 for r in result.results if not r.success and r.severity == "warn")
        n_err  = sum(1 for r in result.results if not r.success and r.severity == "error")

        total_pass += n_pass
        total_warn += n_warn
        total_err  += n_err

        mark    = "✓" if result.success else "✗"
        summary = f"{n_pass:3d}✓ / {n_warn:3d}⚠ / {n_err:3d}✗"
        logger.info(f"  {mark} {suite_path.name:55s}  {summary}")

        if not result.success:
            failures.append(suite_path.name)
            for r in result.results:
                if not r.success:
                    name = getattr(r, "name", "rule")
                    col  = getattr(r, "column", "table") or "table"
                    sev  = getattr(r, "severity", "error").upper()
                    obs  = getattr(r, "observed", {})

                    logger.warning(
                        f"      └─ [{sev}] {name} sur '{col}' | Observé : {obs}"
                    )

    total = total_pass + total_warn + total_err
    logger.info(
        f"═══ Bilan {layer.upper()} : {total_pass}/{total} ✓  |  "
        f"{total_warn} ⚠  |  {total_err} ✗ ═══"
    )

    if failures:
        raise RuntimeError(
            f"Validation GE {layer} : {len(failures)} suite(s) en échec — {failures}"
        )

    logger.success(f"✓ Validation GE {layer} — toutes les suites passent.")
