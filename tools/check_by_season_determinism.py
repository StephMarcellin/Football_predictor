#!/usr/bin/env python3
"""
check_by_season_determinism.py — Les 4 modèles « valeurs différentes » sont-ils déterministes ?

Contexte : après validate_by_season_dev.py, corner_profiles, freekick_profiles,
pass_network_team_match et player_match_event_stats ont le même nombre de lignes qu'avant
mais une empreinte différente. Deux explications possibles :
  (A) le modèle n'est pas déterministe (égalités départagées au hasard par row_number /
      arg_max / DISTINCT ON, ou somme de décimaux dont l'ordre d'addition varie) ;
  (B) l'instantané « avant » venait d'une version antérieure de l'amont.

Ce script les distingue : il reconstruit les 4 tables saison par saison (passe 1), copie le
résultat, les reconstruit une seconde fois (passe 2, code identique) et compare. Une
différence entre les deux passes = non-déterminisme (A) ; aucune = modèle stable (B).
Ainsi, une modification du SQL faite juste avant (ex. ajout d'un tie-break) est prise en
compte : on compare deux constructions du MÊME code, pas l'ancien état au nouveau.
Pour (A), la comparaison est refaite avec les décimaux arrondis à 6 chiffres : un écart
qui disparaît = bruit d'arrondi, sans conséquence.

Usage (base de dev libre, venv actif, depuis la racine) :
    python tools/check_by_season_determinism.py
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
MODELS = ["intermediate_corner_profiles", "intermediate_freekick_profiles",
          "intermediate_pass_network_team_match", "intermediate_player_match_event_stats"]
FLOATS = ("DOUBLE", "FLOAT", "DECIMAL")
if len(sys.argv) > 1:   # ex. : python tools/check_by_season_determinism.py pass_network_team_match
    MODELS = [m for m in MODELS if m[13:] in sys.argv[1:]]
KEY_CANDIDATES = ("str_match_id", "str_team_id")   # clés de jointure pour le détail par colonne


def detail(con, m: str, cols) -> None:
    """Pour un modèle non déterministe : par colonne, nb de lignes qui diffèrent et écart max."""
    names = [c[0] for c in cols]
    keys = [k for k in KEY_CANDIDATES if k in names]
    if not keys:
        print("    (pas de clé str_match_id/str_team_id : détail par colonne impossible)")
        return
    on = " AND ".join(f'a."{k}" IS NOT DISTINCT FROM b."{k}"' for k in keys)
    kcols = ", ".join(f'"{k}"' for k in keys)
    dup = con.execute(f"SELECT count(*) FROM (SELECT {kcols} FROM intermediate.{m} "
                      f"GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    if dup:
        print(f"    clé ({', '.join(keys)}) non unique ({dup:,} doublons) : détail non fiable, ignoré")
        return
    for name, typ, *_ in cols:
        if name in keys:
            continue
        numeric = any(typ.upper().startswith(f) for f in FLOATS + ("INT", "BIGINT", "HUGEINT"))
        diff = f'a."{name}" IS DISTINCT FROM b."{name}"'
        if any(typ.upper().startswith(f) for f in FLOATS):
            diff = f'round(a."{name}", 6) IS DISTINCT FROM round(b."{name}", 6)'
        mx = f', max(abs(a."{name}" - b."{name}"))' if numeric else ", NULL"
        n, amp = con.execute(f"""SELECT count(*) FILTER (WHERE {diff}){mx}
            FROM intermediate.zz_p1_{m} a JOIN intermediate.{m} b ON {on}""").fetchone()
        if n:
            print(f"    colonne {name:<28} {n:>7,} lignes, écart max = {amp}")




def rebuild(season: str, full_refresh: bool) -> None:
    cmd = ["dbt", "run", "--profiles-dir", str(DBT), "--target", "dev",
           "--select", *MODELS,
           "--vars", json.dumps({"spark_out_dir": SPARK, "only_season": season})]
    if full_refresh:
        cmd.append("--full-refresh")
    t0 = time.time()
    r = subprocess.run(cmd, cwd=DBT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    print(f"  saison {season} : {time.time() - t0:5.0f}s")
    if r.returncode != 0:
        print(r.stdout[-3000:])
        sys.exit(f"dbt a échoué sur {season}")


def main() -> None:
    con = duckdb.connect(str(DEV_DB))
    seasons = sorted(r[0] for r in con.execute(
        "SELECT DISTINCT str_season FROM intermediate.intermediate_whoscored_match_bridge").fetchall())
    con.close()

    print("passe 1 (code actuel)")
    for i, s in enumerate(seasons):
        rebuild(s, full_refresh=(i == 0))

    con = duckdb.connect(str(DEV_DB))
    for m in MODELS:
        con.execute(f"CREATE OR REPLACE TABLE intermediate.zz_p1_{m} AS SELECT * FROM intermediate.{m}")
    con.close()
    print("copie de la passe 1 : OK\npasse 2 (code identique)")
    for i, s in enumerate(seasons):
        rebuild(s, full_refresh=(i == 0))

    con = duckdb.connect(str(DEV_DB))
    print(f"\n{'modèle':<42}{'lignes ≠ (exact)':>18}{'lignes ≠ (arrondi 6)':>22}  verdict")
    for m in MODELS:
        cols = con.execute(f"DESCRIBE intermediate.{m}").fetchall()
        exact = ", ".join(f'"{c[0]}"' for c in cols)
        rounded = ", ".join(
            f'round("{c[0]}", 6) AS "{c[0]}"' if any(c[1].upper().startswith(f) for f in FLOATS)
            else f'"{c[0]}"' for c in cols)
        d_exact = con.execute(f"""SELECT count(*) FROM (
            SELECT {exact} FROM intermediate.zz_p1_{m} EXCEPT SELECT {exact} FROM intermediate.{m})""").fetchone()[0]
        d_round = con.execute(f"""SELECT count(*) FROM (
            SELECT {rounded} FROM intermediate.zz_p1_{m} EXCEPT SELECT {rounded} FROM intermediate.{m})""").fetchone()[0]
        n = con.execute(f"SELECT count(*) FROM intermediate.{m}").fetchone()[0]
        if d_exact == 0:
            v = "STABLE : reconstruction identique"
        elif d_round == 0:
            v = "bruit d'arrondi des décimaux (sans conséquence)"
        else:
            v = f"NON DÉTERMINISTE ({d_round:,} lignes sur {n:,} diffèrent au-delà de l'arrondi)"
        print(f"{m[13:]:<42}{d_exact:>18,}{d_round:>22,}  {v}")
        if d_exact:
            detail(con, m, cols)
        con.execute(f"DROP TABLE intermediate.zz_p1_{m}")
    con.close()


if __name__ == "__main__":
    main()
