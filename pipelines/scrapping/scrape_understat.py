"""
Scraper Understat — Schedule + Team Match Stats
================================================
Utilise soccerdata pour récupérer les données Understat.
Idempotent : skip si le CSV existe déjà.

Usage :
    python pipelines/scrape_understat.py
    python pipelines/scrape_understat.py --league "ITA-Serie A" --season 2022-2023
    python pipelines/scrape_understat.py --dry-run
"""

import argparse
from pathlib import Path

import yaml
import soccerdata as sd
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR  = Path(__file__).resolve().parent.parent
CFG_PATH  = ROOT_DIR / "config.yaml"
MAIN_CFG  = ROOT_DIR / "config.yaml"
LOG_DIR   = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_understat.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="10 MB",
    retention=5,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

with open(CFG_PATH, encoding="utf-8") as f:
    SCRAP_CFG = yaml.safe_load(f)

with open(MAIN_CFG, encoding="utf-8") as f:
    MAIN_CFG_DATA = yaml.safe_load(f)

OUT_DIR = Path(MAIN_CFG_DATA["paths"]["raw_data"]) / "understat" / "csv"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Mapping nom canonique → nom soccerdata
LEAGUE_MAP = {
    "Premier League": "ENG-Premier League",
    "Ligue 1":        "FRA-Ligue 1",
    "Bundesliga":     "GER-Bundesliga",
    "Serie A":        "ITA-Serie A",
    "La Liga":        "ESP-La Liga",
}


# ── Utilitaires ───────────────────────────────────────────────────────────────

def season_to_code(season: str) -> str:
    """'2022-2023' → '2223'"""
    parts = season.split("-")
    return f"{parts[0][-2:]}{parts[1][-2:]}"


def is_cached(league_sd: str, season_code: str) -> bool:
    """Vérifie si les deux CSVs existent déjà."""
    schedule = OUT_DIR / f"schedule_{league_sd}_{season_code}.csv"
    stats    = OUT_DIR / f"stats_{league_sd}_{season_code}.csv"
    return schedule.exists() and stats.exists()


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_league_season(
    league_canonical: str,
    season: str,
    dry_run: bool = False,
) -> str:
    """
    Scrape schedule + stats pour une ligue × saison.
    Retourne : 'ok' | 'skip' | 'error'
    """
    league_sd   = LEAGUE_MAP.get(league_canonical)
    season_code = season_to_code(season)

    if not league_sd:
        logger.warning(f"  Ligue non supportée par Understat : {league_canonical}")
        return "skip"

    if is_cached(league_sd, season_code):
        logger.debug(f"  ⏩ Cache : {league_canonical} {season}")
        return "skip"

    if dry_run:
        logger.info(f"  [DRY-RUN] À scraper : {league_canonical} {season}")
        return "ok"

    try:
        logger.info(f"  📡 {league_canonical} {season}...")
        understat = sd.Understat(
            leagues=league_sd,
            seasons=season,
            proxy="tor",
        )

        df_schedule = understat.read_schedule().reset_index()
        df_stats    = understat.read_team_match_stats().reset_index()

        path_schedule = OUT_DIR / f"schedule_{league_sd}_{season_code}.csv"
        path_stats    = OUT_DIR / f"stats_{league_sd}_{season_code}.csv"

        df_schedule.to_csv(path_schedule, index=False)
        df_stats.to_csv(path_stats, index=False)

        logger.success(
            f"  ✅ {league_canonical} {season} — "
            f"{len(df_schedule)} matchs schedule, {len(df_stats)} lignes stats"
        )
        return "ok"

    except Exception as e:
        logger.error(f"  ❌ Erreur {league_canonical} {season} : {e}")
        return "error"


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper Understat")
    parser.add_argument("--league",  default=None, help="Filtrer sur une ligue")
    parser.add_argument("--season",  default=None, help="Filtrer sur une saison")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche ce qui serait scrapé sans télécharger")
    args = parser.parse_args()

    seasons_cfg = set(SCRAP_CFG.get("seasons", []))
    leagues_cfg = set(SCRAP_CFG.get("leagues", []))

    # Filtres CLI
    if args.league:
        leagues_cfg = {args.league}
    if args.season:
        seasons_cfg = {args.season}

    # Restreindre aux ligues supportées par Understat (Big5 uniquement)
    leagues_cfg = {l for l in leagues_cfg if l in LEAGUE_MAP}

    stats = {"ok": 0, "skip": 0, "error": 0}

    for league in sorted(leagues_cfg):
        for season in sorted(seasons_cfg):
            result = scrape_league_season(league, season, dry_run=args.dry_run)
            stats[result] += 1

    logger.success(
        f"Scraping Understat terminé — "
        f"OK:{stats['ok']} SKIP:{stats['skip']} ERR:{stats['error']}"
    )


if __name__ == "__main__":
    main()