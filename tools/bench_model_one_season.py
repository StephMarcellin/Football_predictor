#!/usr/bin/env python3
"""
bench_model_one_season.py — Chronomètre intermediate_shot_creating_actions (une saison, ALL, ou la base de prod).

Question posée : le modèle est-il lent à cause du SQL, ou de la taille traitée d'un coup ?
On exécute le SQL compilé par dbt sur les chaînes d'UNE seule saison, en mémoire,
avec les réglages de la prod (2 threads, 10 Go). Aucune base du projet n'est ouverte
ni modifiée.

Usage (depuis la racine du projet) :
    python tools/bench_model_one_season.py 2022-2023
    python tools/bench_model_one_season.py ALL
    python tools/bench_model_one_season.py --prod
"""
import sys
import tempfile
import time
from pathlib import Path

import duckdb

ROOT     = Path(__file__).resolve().parent.parent
COMPILED = (ROOT / "dbt_project/target/compiled/football_prediction/models/"
            "intermediate/intermediate_shot_creating_actions.sql")
CHAINS   = (ROOT / "data/spark_out/int_possession_chains").as_posix()
DB       = ROOT / "db" / "football.duckdb"


def main(mode: str) -> None:
    """mode = une saison (ex. 2022-2023) | ALL (18 saisons, en mémoire) | --prod.

    --prod : ouvre football.duckdb en LECTURE SEULE et exécute le SQL sur la vraie
    table des chaînes (59 M lignes, stockage natif de la base de 79 Go). Le résultat
    va dans une table temporaire en mémoire : la base n'est jamais modifiée.
    """
    sql = COMPILED.read_text(encoding="utf-8").strip().rstrip(";")
    if "{{" in sql or "{%" in sql:
        sys.exit("SQL compilé non rendu : relance `dbt compile` sur ce modèle.")

    con = duckdb.connect()
    tmp = tempfile.mkdtemp(prefix="bench_duckdb_")
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '10GB'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{Path(tmp).as_posix()}'")

    if mode == "--prod":
        con.execute(f"ATTACH '{DB.as_posix()}' AS football (READ_ONLY)")
        label, target = "base de prod (lecture seule)", "bench_out"
        t1 = time.time()
        con.execute(f"CREATE TEMP TABLE {target} AS ({sql})")
    else:
        # Le SQL compilé cite "football"."intermediate"."intermediate_possession_chains".
        con.execute("ATTACH ':memory:' AS football")
        con.execute("CREATE SCHEMA football.intermediate")
        where = "" if mode == "ALL" else f"WHERE str_season = '{mode}'"
        t0 = time.time()
        con.execute(f"""
            CREATE TABLE football.intermediate.intermediate_possession_chains AS
            SELECT * FROM read_parquet('{CHAINS}/**/*.parquet', hive_partitioning = 1)
            {where}""")
        n_in = con.execute(
            "SELECT count(*) FROM football.intermediate.intermediate_possession_chains").fetchone()[0]
        print(f"chargement des chaînes ({mode}) : {n_in:,} lignes en {time.time() - t0:.0f}s")
        label, target = f"en mémoire, {mode}", "football.intermediate.bench_out"
        t1 = time.time()
        con.execute(f"CREATE TABLE {target} AS ({sql})")

    dt = time.time() - t1
    n_out = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
    print(f"modèle SCA, {label} : {n_out:,} lignes en {dt:.0f}s ({dt / 60:.1f} min)")
    print("référence : le run complet a pris 117 min")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage : python tools/bench_model_one_season.py <2022-2023 | ALL | --prod>")
    main(sys.argv[1])
