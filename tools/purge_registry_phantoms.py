#!/usr/bin/env python3
"""
purge_registry_phantoms.py — Supprime de intermediate.match_registry les matchs « fantômes »
créés par un alias de team_mapping affecté au mauvais club.

Contexte : l'alias 'Frankfurt' pointait vers 'FSV Frankfurt' (id 293) au lieu de
'Eintracht Francfort' (id 24). Chaque match de l'Eintracht a donc été enregistré deux fois :
le vrai (id 24) et un fantôme (id 293). upsert_match_registry fait INSERT ... ON CONFLICT
DO NOTHING et ne supprime jamais rien : une fois l'alias corrigé, les fantômes restent.

Définition d'un fantôme (critère de sûreté) : une ligne du registre qui contient l'id fautif
(--wrong-id) ET pour laquelle il existe un JUMEAU : même date, même ligue, même saison, même
adversaire, avec l'id correct (--right-id) à la place de l'id fautif. Les vrais matchs du club
fautif (ex. les matchs de 2. Bundesliga du FSV Frankfurt) n'ont pas de jumeau et sont conservés.

Sûretés :
  - dry-run par défaut : base ouverte en LECTURE SEULE, rien n'est écrit ;
  - --expect N : en mode --apply, abandon si le nombre de fantômes trouvé n'est pas N ;
  - la suppression se fait dans une transaction, avec relecture du résultat avant COMMIT.

Usage (venv actif, depuis la racine, base libre) :
    python tools/purge_registry_phantoms.py --wrong-id 293 --right-id 24
    python tools/purge_registry_phantoms.py --wrong-id 293 --right-id 24 --expect 282 --apply

Après la purge : les tables intermediate/gold déjà construites contiennent encore les anciens
match_id ; elles ne changent qu'après un dbt run (--full-refresh pour les incrémentales).
"""
import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "db" / "football.duckdb"

# Paramètres, dans l'ordre : wrong, wrong, wrong, right, wrong, right
PHANTOM_SQL = """
SELECT p.match_id, p.league_source, p.season
FROM intermediate.match_registry p
WHERE (p.home_team_id = ? OR p.away_team_id = ?)
  AND EXISTS (
      SELECT 1 FROM intermediate.match_registry t
      WHERE t.match_id <> p.match_id
        AND t.match_date    = p.match_date
        AND t.league_source = p.league_source
        AND t.season        = p.season
        AND (   (p.home_team_id = ? AND t.home_team_id = ? AND t.away_team_id = p.away_team_id)
             OR (p.away_team_id = ? AND t.away_team_id = ? AND t.home_team_id = p.home_team_id))
  )
"""


def purge(con, wrong_id: int, right_id: int, expect: int | None, apply: bool) -> int:
    """Retourne 0 (ok / dry-run) ou 2 (abandon : rien n'a été supprimé)."""
    params = [wrong_id, wrong_id, wrong_id, right_id, wrong_id, right_id]
    con.execute("CREATE TEMP TABLE _phantoms AS " + PHANTOM_SQL, params)
    n = con.execute("SELECT COUNT(*) FROM _phantoms").fetchone()[0]
    total_before = con.execute("SELECT COUNT(*) FROM intermediate.match_registry").fetchone()[0]

    print(f"Registre : {total_before:,} matchs. Fantômes (id {wrong_id} avec jumeau {right_id}) : {n}")
    print("Répartition (ligue, nb) :")
    for lg, k in con.execute("SELECT league_source, COUNT(*) FROM _phantoms GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  {lg:15} {k}")

    kept = con.execute(
        "SELECT r.league_source, COUNT(*) FROM intermediate.match_registry r "
        "WHERE (r.home_team_id = ? OR r.away_team_id = ?) "
        "AND r.match_id NOT IN (SELECT match_id FROM _phantoms) GROUP BY 1 ORDER BY 1",
        [wrong_id, wrong_id],
    ).fetchall()
    print(f"Lignes avec l'id {wrong_id} CONSERVÉES (pas de jumeau, donc vrais matchs du club) :")
    for lg, k in kept:
        print(f"  {lg:15} {k}")

    if expect is not None and n != expect:
        print(f"ABANDON : {n} fantôme(s) trouvé(s), {expect} attendu(s) (--expect). Rien n'a été supprimé.")
        return 2
    if not apply:
        print("DRY-RUN : rien n'a été supprimé. Relance avec --apply (et --expect N) pour supprimer.")
        return 0
    if n == 0:
        print("Rien à supprimer.")
        return 0

    con.execute("BEGIN TRANSACTION")
    con.execute("DELETE FROM intermediate.match_registry WHERE match_id IN (SELECT match_id FROM _phantoms)")
    total_after = con.execute("SELECT COUNT(*) FROM intermediate.match_registry").fetchone()[0]
    left = con.execute(
        "SELECT COUNT(*) FROM _phantoms p JOIN intermediate.match_registry r USING (match_id)"
    ).fetchone()[0]
    if total_before - total_after != n or left != 0:
        con.execute("ROLLBACK")
        print(f"ECHEC : {total_before - total_after} ligne(s) supprimée(s) au lieu de {n}. ROLLBACK effectué.")
        return 2
    con.execute("COMMIT")
    print(f"OK : {n} fantôme(s) supprimé(s). Registre : {total_before:,} -> {total_after:,} matchs.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--wrong-id", type=int, required=True, help="id du club fautif (ex. 293)")
    ap.add_argument("--right-id", type=int, required=True, help="id du bon club (ex. 24)")
    ap.add_argument("--expect", type=int, default=None, help="nombre de fantômes attendu (garde-fou en --apply)")
    ap.add_argument("--apply", action="store_true", help="supprimer (sinon dry-run en lecture seule)")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="base DuckDB (défaut : db/football.duckdb)")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=not args.apply)
    try:
        sys.exit(purge(con, args.wrong_id, args.right_id, args.expect, args.apply))
    finally:
        con.close()
