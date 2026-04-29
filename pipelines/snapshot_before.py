# snapshot_before.py — à lancer UNE FOIS avant la migration
import duckdb, yaml, json
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

conn = duckdb.connect(CFG["paths"]["db"], read_only=True)

snapshot = {}

for table in ["gold.features_training", "gold.features_final", "gold.stg_backbone"]:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        cols  = [r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{table.split('.')[0]}' "
            f"AND table_name='{table.split('.')[1]}'"
        ).fetchall()]
        snapshot[table] = {"count": count, "n_cols": len(cols), "cols": sorted(cols)}
        print(f"{table}: {count:,} lignes, {len(cols)} colonnes")
    except Exception as e:
        print(f"{table}: inaccessible ({e})")

conn.close()
Path("snapshot_before.json").write_text(json.dumps(snapshot, indent=2))
print("\n✅  snapshot_before.json sauvegardé")