#!/usr/bin/env python3
"""
fix_team_alias.py — Réaffecte UN alias de referentiel.team_mapping vers le bon club.

Contexte : l'alias FBref 'Frankfurt' (nom court de l'Eintracht dans la colonne adversaire)
pointait vers 'FSV Frankfurt' (team_id 293) au lieu de 'Eintracht Francfort' (team_id 24).
Résultat : 272 matchs de Bundesliga dédoublés en un match réel + un match fantôme.
team_mapping n'est jamais réamorcée depuis un CSV : la correction passe donc par cet UPDATE,
versionné dans git plutôt que tapé à la main dans la base.

Sûretés :
  - dry-run par défaut : sans --apply, la base est ouverte en LECTURE SEULE et rien n'est écrit ;
  - l'UPDATE n'est fait que si la ligne est dans l'état attendu (--from-club), sinon abandon ;
  - idempotent : si l'alias pointe déjà vers --to-club, le script ne fait rien ;
  - team_id et nom canonique du club cible sont LUS dans team_mapping, jamais codés en dur.

Usage (venv actif, depuis la racine, base libre) :
    python tools/fix_team_alias.py --alias Frankfurt --from-club "FSV Frankfurt" --to-club "Eintracht Francfort"
    python tools/fix_team_alias.py --alias Frankfurt --from-club "FSV Frankfurt" --to-club "Eintracht Francfort" --apply
"""
import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "db" / "football.duckdb"


def fix_alias(con, alias: str, from_club: str, to_club: str, apply: bool) -> int:
    """Retourne 0 (ok / rien à faire), 2 (état inattendu : rien n'a été modifié)."""
    current = con.execute(
        "SELECT alias, club_name, team_id FROM referentiel.team_mapping WHERE alias = ?", [alias]
    ).fetchall()
    if len(current) != 1:
        print(f"ABANDON : {len(current)} ligne(s) pour l'alias {alias!r} (1 attendue).")
        return 2
    _, cur_club, cur_id = current[0]

    # Club cible : nom canonique et team_id lus dans la table (DISTINCT : un club a plusieurs alias)
    target = con.execute(
        "SELECT DISTINCT club_name, team_id FROM referentiel.team_mapping "
        "WHERE club_name = ? AND team_id IS NOT NULL", [to_club]
    ).fetchall()
    if len(target) != 1:
        print(f"ABANDON : le club cible {to_club!r} donne {len(target)} couple(s) (club, team_id), 1 attendu.")
        return 2
    to_name, to_id = target[0]

    if cur_club == to_name and cur_id == to_id:
        print(f"Rien à faire : {alias!r} pointe déjà vers {to_name!r} (team_id {to_id}).")
        return 0
    if cur_club != from_club:
        print(f"ABANDON : {alias!r} pointe vers {cur_club!r} (team_id {cur_id}), "
              f"pas vers {from_club!r} comme attendu. Rien n'a été modifié.")
        return 2

    print(f"AVANT : {alias!r} -> {cur_club!r} (team_id {cur_id})")
    print(f"APRÈS : {alias!r} -> {to_name!r} (team_id {to_id})")
    if not apply:
        print("DRY-RUN : rien n'a été écrit. Relance avec --apply pour appliquer.")
        return 0

    con.execute(
        "UPDATE referentiel.team_mapping SET club_name = ?, team_id = ? WHERE alias = ? AND club_name = ?",
        [to_name, to_id, alias, from_club],
    )
    after = con.execute(
        "SELECT club_name, team_id FROM referentiel.team_mapping WHERE alias = ?", [alias]
    ).fetchone()
    ok = after == (to_name, to_id)
    print(f"{'OK' if ok else 'ECHEC'} : relu -> {after}")
    return 0 if ok else 2


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--alias", required=True, help="alias à corriger (ex. Frankfurt)")
    ap.add_argument("--from-club", required=True, help="club actuel attendu (ex. 'FSV Frankfurt')")
    ap.add_argument("--to-club", required=True, help="club cible (ex. 'Eintracht Francfort')")
    ap.add_argument("--apply", action="store_true", help="écrire dans la base (sinon dry-run en lecture seule)")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="base DuckDB (défaut : db/football.duckdb)")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=not args.apply)
    try:
        sys.exit(fix_alias(con, args.alias, args.from_club, args.to_club, args.apply))
    finally:
        con.close()
