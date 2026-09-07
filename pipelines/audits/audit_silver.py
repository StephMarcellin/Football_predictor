"""audit_silver.py — Rapport + quality check GLOBAL sur silver.* (joinabilite
croisee fbref x understat x whoscored). A lancer APRES process_team_stats + process_events."""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
from process_common import *  # noqa: F401,F403


def main() -> None:
    logger.info("=== AUDIT SILVER (rapport + quality check) ===")
    con = duckdb.connect(str(DB_PATH))
    _init_team_mapping(con); _init_team_mapping_ids(con)
    _init_competition_mapping(con); _init_transfermarkt(con); _init_match_registry(con)
    print_report(con)
    run_quality_check(con)
    con.close()
    logger.success("=== AUDIT SILVER termine ===")


if __name__ == "__main__":
    main()
