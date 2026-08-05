"""audit_silver.py — Rapport + quality check GLOBAL sur silver.* (joinabilite
croisee fbref x understat x whoscored). A lancer APRES process_team_stats + process_events."""

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
