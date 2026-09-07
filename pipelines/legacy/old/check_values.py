# check_values.py
import duckdb, yaml
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

conn = duckdb.connect(CFG["paths"]["db"], read_only=True)

CHECKS = [
    # (table, colonne, tolérance relative en %)
    ("gold.features_final",    "xg_net_diff_5",      1.0),
    ("gold.features_final",    "sqr_diff_5",          1.0),
    ("gold.features_final",    "ppda_diff_5",         1.0),
    ("gold.features_final",    "win_rate_diff_5",     1.0),
    ("gold.features_final",    "h2h_win_rate",        1.0),
    ("gold.features_final",    "upset_risk_index",    1.0),
    ("gold.features_training", "ws_field_tilt_actions", 1.0),
    ("gold.features_training", "ws_momentum_delta",     1.0),
    ("gold.features_training", "f1_mutual_cancel_idx",  1.0),
    ("gold.features_training", "f19_tactical_lock_idx", 1.0),
    ("gold.features_training", "ws_late_equalizer_rate",1.0),
]

print("═══ Sanity check — valeurs moyennes ═══\n")
for table, col, tol in CHECKS:
    try:
        stats = conn.execute(f"""
            SELECT
                COUNT(*)       AS n_total,
                COUNT({col})   AS n_notnull,
                AVG({col})     AS mean,
                STDDEV({col})  AS std,
                MIN({col})     AS min_val,
                MAX({col})     AS max_val
            FROM {table}
            WHERE {col} IS NOT NULL
        """).fetchone()
        n_total, n_notnull, mean, std, min_v, max_v = stats
        pct = n_notnull / n_total * 100 if n_total else 0
        print(f"  {col:<40}")
        print(f"    coverage={pct:.1f}%  mean={mean:.4f}  std={std:.4f}  [{min_v:.4f}, {max_v:.4f}]")
    except Exception as e:
        print(f"  {col}: ❌ {e}")

conn.close()