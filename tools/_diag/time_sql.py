"""Chronometre un fichier .sql sur football_dev.duckdb avec les reglages de dbt.

Pourquoi ce script plutot qu'un client SQL : un client graphique ouvre souvent
une base en memoire dont le repertoire courant est invalide. DuckDB construit
alors son repertoire de debordement a partir de ce chemin vide et echoue avec
"Failed to create directory". Ici le chemin est absolu et cree a l'avance.

Usage :
    python tools/_diag/time_sql.py tools/_diag/test_possession_chains_200.sql

Le fichier .sql doit contenir UNE seule instruction, qui cree une table t_test.
"""

import pathlib
import sys
import time

import duckdb

# parents[2] = racine du depot (tools/_diag/time_sql.py -> tools/_diag -> tools -> racine)
ROOT = pathlib.Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "football_dev.duckdb"
TMP_DIR = ROOT / "db" / "duckdb_tmp"


def main(sql_path: str) -> None:
    """Execute le SQL et affiche la duree, meme si la requete echoue."""
    sql = pathlib.Path(sql_path).read_text(encoding="utf-8")
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    # Memes reglages que le profil dev de dbt, sinon le chronometre ne compare rien.
    con.execute("SET threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.execute("SET max_temp_directory_size = '40GB'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}'")
    con.execute("SET enable_progress_bar = true")
    con.execute("SET progress_bar_time = 10000")

    print(f"base      : {DB_PATH}")
    print(f"temp dir  : {TMP_DIR}")
    print(f"requete   : {sql_path}", flush=True)

    start = time.perf_counter()
    try:
        con.execute(sql)
    finally:
        print(f"\nduree : {time.perf_counter() - start:,.1f} s", flush=True)

    rows = con.execute("SELECT COUNT(*) FROM t_test").fetchone()[0]
    print(f"lignes produites : {rows:,}")
    con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage : python tools/_diag/time_sql.py <chemin.sql>")
    main(sys.argv[1])
