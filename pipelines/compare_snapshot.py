# compare_snapshot.py
import duckdb, yaml, json
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

conn = duckdb.connect(CFG["paths"]["db"], read_only=True)
before = json.loads(Path("snapshot_before.json").read_text())

print("═══ Comparaison avant / après ═══\n")
for table, ref in before.items():
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        cols  = sorted([r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{table.split('.')[0]}' "
            f"AND table_name='{table.split('.')[1]}'"
        ).fetchall()])

        count_ok = count == ref["count"]
        cols_ok  = cols  == ref["cols"]

        print(f"{table}")
        print(f"  Lignes  : {ref['count']:,} → {count:,}  {'✅' if count_ok else '❌ DIFFÉRENCE'}")
        print(f"  Colonnes: {ref['n_cols']} → {len(cols)}  {'✅' if cols_ok else '❌ DIFFÉRENCE'}")

        if not cols_ok:
            added   = set(cols) - set(ref["cols"])
            removed = set(ref["cols"]) - set(cols)
            if added:   print(f"  + Ajoutées  : {sorted(added)}")
            if removed: print(f"  - Supprimées: {sorted(removed)}")
        print()
    except Exception as e:
        print(f"{table}: erreur ({e})\n")

conn.close()