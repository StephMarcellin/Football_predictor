#!/usr/bin/env python3
"""
validate_by_season_dev.py — Valide la construction « saison par saison » sur la base de DEV.

Principe : les 20 modèles tagués `by_season` ont déjà une table dans football_dev.duckdb,
construite en une passe. On prend un instantané (nombre de lignes + empreinte de TOUTES
les colonnes), on reconstruit ces tables saison par saison avec la nouvelle boucle, puis
on compare. Une empreinte identique = mêmes lignes, mêmes valeurs.

À lire avant d'interpréter un écart : l'instantané date de la dernière construction de
dev. Si un modèle EN AMONT a changé depuis (ex. chaînes de possession re-calculées par
Spark), un écart peut venir de là et non du découpage. Le script le signale ; on tranche
au cas par cas.

Usage (depuis la racine du projet, base de dev libre, venv actif) :
    python tools/validate_by_season_dev.py
"""
import json
import subprocess
import sys
import time
from pathlib import Path

import duckdb

ROOT   = Path(__file__).resolve().parent.parent
DEV_DB = ROOT / "db" / "football_dev.duckdb"
DBT    = ROOT / "dbt_project"
SPARK  = (ROOT / "data" / "spark_out").as_posix()


def tagged_models() -> list[str]:
    return sorted(f.stem for f in (DBT / "models").rglob("*.sql")
                  if "tags=['by_season']" in f.read_text(encoding="utf-8", errors="replace"))


def snapshot(models: list[str]) -> dict:
    """{modèle: (nb_lignes, empreinte)} ; empreinte = somme des hash() de chaque ligne."""
    con = duckdb.connect(str(DEV_DB), read_only=True)
    try:
        out = {}
        for m in models:
            try:
                n, h = con.execute(
                    f"SELECT count(*), coalesce(sum(hash(t)::HUGEINT), 0) "
                    f"FROM intermediate.{m} t").fetchone()
                out[m] = (n, int(h))
            except duckdb.Error as e:
                out[m] = (None, str(e)[:80])
        return out
    finally:
        con.close()


def dev_seasons() -> list[str]:
    con = duckdb.connect(str(DEV_DB), read_only=True)
    try:
        return sorted(r[0] for r in con.execute(
            "SELECT DISTINCT str_season FROM intermediate.intermediate_whoscored_match_bridge"
        ).fetchall())
    finally:
        con.close()


def dbt_run(season: str, full_refresh: bool) -> float:
    cmd = ["dbt", "run", "--profiles-dir", str(DBT), "--target", "dev",
           "--select", "tag:by_season",
           "--vars", json.dumps({"spark_out_dir": SPARK, "only_season": season})]
    if full_refresh:
        cmd.append("--full-refresh")
    t0 = time.time()
    r = subprocess.run(cmd, cwd=DBT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    dt = time.time() - t0
    (ROOT / "logs").mkdir(exist_ok=True)
    (ROOT / "logs" / "validate_by_season_last.log").write_text(r.stdout + r.stderr,
                                                               encoding="utf-8")
    if r.returncode != 0:
        print(r.stdout[-3000:])
        sys.exit(f"dbt a échoué sur {season} (voir logs/validate_by_season_last.log)")
    return dt


def main() -> None:
    models = tagged_models()
    seasons = dev_seasons()
    print(f"{len(models)} modèles by_season ; saisons de la base de dev : {seasons}")

    avant = snapshot(models)
    print("instantané avant : OK")

    for i, s in enumerate(seasons):
        dt = dbt_run(s, full_refresh=(i == 0))
        print(f"  saison {s} : {dt:5.0f}s  (full-refresh={i == 0})")

    apres = snapshot(models)
    print(f"\n{'modèle':<42}{'avant':>12}{'après':>12}  verdict")
    ko = 0
    for m in models:
        a, b = avant[m], apres[m]
        if a[0] is None:
            verdict = "pas de table avant (rien à comparer)"
        elif a == b:
            verdict = "IDENTIQUE"
        elif a[0] == b[0]:
            verdict = "MÊME NOMBRE DE LIGNES, valeurs différentes"; ko += 1
        else:
            verdict = f"DIFFÉRENT ({b[0] - a[0]:+,} lignes)"; ko += 1
        print(f"{m[13:]:<42}{str(a[0]):>12}{str(b[0]):>12}  {verdict}")
    print(f"\n{len(models) - ko} identiques ou sans référence, {ko} à examiner.")


if __name__ == "__main__":
    main()
