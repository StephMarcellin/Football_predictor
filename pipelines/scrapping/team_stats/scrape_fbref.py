"""
Scraper FBref — Match Logs par club et saison
=============================================
Utilise le référentiel DuckDB pour garantir la complétude :
chaque club × saison × ligue du référentiel est visité.

Usage :
# Tout scraper selon config.yaml
python pipelines/scrape_fbref.py

# Tester sans télécharger
python pipelines/scrape_fbref.py --dry-run

# Une seule ligue/saison
python pipelines/scrape_fbref.py --league Bundesliga --season 2017-2018

# Sans interface graphique
python pipelines/scrape_fbref.py --headless
"""

import argparse
import random
import sys
import time
from pathlib import Path

import duckdb
import yaml
from loguru import logger
from seleniumbase import Driver

# ── Config Centralisée ────────────────────────────────────────────────────────

ROOT_DIR = next(
    p for p in Path(__file__).resolve().parents
    if (p / "config.yaml").exists()
)

CONFIG_PATH = ROOT_DIR / "config.yaml"

def load_config() -> dict:
    """Charge la configuration globale unique."""
    if not CONFIG_PATH.exists():
        logger.critical(f"Fichier de configuration introuvable : {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)

CFG = load_config()

LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_fbref.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="10 MB",
    retention=5,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

DATA_DIR = Path(CFG["paths"]["raw_data"]) / "fbref" / "html"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(CFG["paths"]["duckdb"])

LEAGUE_CONFIG = {
    "Premier League": {"fbref_id": "9",  "fbref_slug": "ENG-Premier-League"},
    "Ligue 1":        {"fbref_id": "13", "fbref_slug": "FRA-Ligue-1"},
    "Bundesliga":     {"fbref_id": "20", "fbref_slug": "GER-Bundesliga"},
    "Serie A":        {"fbref_id": "11", "fbref_slug": "ITA-Serie-A"},
    "La Liga":        {"fbref_id": "12", "fbref_slug": "ESP-La-Liga"},
    "Championship":   {"fbref_id": "10", "fbref_slug": "ENG-Championship"},
    "Ligue 2":        {"fbref_id": "60", "fbref_slug": "FRA-Ligue-2"},
    "2. Bundesliga":  {"fbref_id": "33", "fbref_slug": "GER-Bundesliga-2"},
    "Serie B":        {"fbref_id": "18", "fbref_slug": "ITA-Serie-B"},
    "La Liga 2":      {"fbref_id": "17", "fbref_slug": "ESP-La-Liga-2"},
}


# ── Utilitaires ───────────────────────────────────────────────────────────────

def season_to_code(season: str) -> str:
    """'2017-2018' → '1718'"""
    parts = season.split("-")
    return f"{parts[0][-2:]}{parts[1][-2:]}"


def is_cached(team_slug: str, season_code: str, stat_type: str) -> bool:
    """Vérifie si le fichier HTML existe déjà."""
    filename = f"matchlogs_{team_slug}_{season_code}_{stat_type}.html"
    return (DATA_DIR / filename).exists()


def accept_cookies(driver) -> None:
    """Accepte le bandeau cookies FBref si présent."""
    try:
        btn = driver.find_element(
            "css selector",
            "button.osano-cm-accept-all, button.osano-cm-button--type_accept"
        )
        if btn:
            btn.click()
            logger.debug("  🍪 Cookies acceptés")
            time.sleep(1)
    except Exception:
        pass


def create_driver(headless: bool = False):
    """Instancie un driver Undetected ChromeDriver sécurisé."""
    logger.info("  🚀 Démarrage d'une nouvelle instance Chrome...")
    driver = Driver(uc=True, headless=headless)
    driver.uc_open_with_reconnect("https://fbref.com", 5)
    time.sleep(random.uniform(2, 3))
    accept_cookies(driver)
    return driver


def ensure_driver_alive(driver, headless: bool = False):
    """Vérifie l'état du driver et le recrée s'il a crashé."""
    try:
        _ = driver.current_url
        return driver
    except Exception as e:
        logger.warning(f"  🔄 Driver inactif ou crashé ({e}). Redémarrage...")
        try:
            driver.quit()
        except Exception:
            pass
        return create_driver(headless=headless)


def build_stat_urls(team_id: str, team_name_slug: str, season: str) -> dict[str, str]:
    """Construit les 4 URLs FBref pour un club × saison."""
    base = f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps"
    return {
        "schedule": f"{base}/schedule/{team_name_slug}-Scores-and-Fixtures-All-Competitions",
        "shooting": f"{base}/shooting/{team_name_slug}-Match-Logs-All-Competitions",
        "keeper":   f"{base}/keeper/{team_name_slug}-Match-Logs-All-Competitions",
        "misc":     f"{base}/misc/{team_name_slug}-Match-Logs-All-Competitions",
    }


def get_all_team_urls(driver, league: str, season: str) -> list[dict]:
    """Visite la page de stats FBref et retourne TOUS les clubs présents."""
    cfg        = LEAGUE_CONFIG[league]
    fbref_id   = cfg["fbref_id"]
    fbref_slug = cfg["fbref_slug"]

    url = (
        f"https://fbref.com/en/comps/{fbref_id}/{season}/"
        f"stats/{season}-{fbref_slug}-Stats"
    )

    logger.debug(f"  Navigation : {url}")
    driver.uc_open_with_reconnect(url, 5)
    accept_cookies(driver)
    time.sleep(random.uniform(2, 3))

    elements = driver.find_elements("css selector", 'th[data-stat="team"] a')

    if not elements:
        logger.warning(f"  Liste vide pour {league} {season} — retry")
        time.sleep(3)
        elements = driver.find_elements("css selector", 'th[data-stat="team"] a')

    teams = []
    for el in elements:
        href = el.get_attribute("href")
        if not href or "squads" not in href:
            continue
        url_parts      = href.rstrip("/").split("/")
        squads_idx     = url_parts.index("squads")
        team_id        = url_parts[squads_idx + 1]
        team_name_slug = url_parts[-1].replace("-Stats", "")
        teams.append({
            "team_slug": team_name_slug,
            "team_id":   team_id,
            "href":      href,
        })

    # Dédoublonnage sur team_id
    seen = set()
    unique_teams = []
    for t in teams:
        if t["team_id"] not in seen:
            seen.add(t["team_id"])
            unique_teams.append(t)

    logger.info(f"  {len(unique_teams)} équipes uniques trouvées : {league} {season}")
    return unique_teams


def scrape_stat_page(
    driver,
    team: str,
    season: str,
    stat_type: str,
    url: str,
    team_name_slug: str,
    season_code: str,
) -> tuple[str, object]:
    """Télécharge une page de statistiques et enregistre le HTML."""
    filename  = f"matchlogs_{team_name_slug}_{season_code}_{stat_type}.html"
    file_path = DATA_DIR / filename

    try:
        logger.info(f"  📡 {team_name_slug} {season} {stat_type}")
        driver.uc_open_with_reconnect(url, 4)
        accept_cookies(driver)
        time.sleep(random.uniform(1.5, 3.0))

        if "matchlogs_for" in driver.page_source:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.success(f"  ✅ {filename}")
            return "ok", driver
        else:
            logger.warning(f"  ⚠️ Table non trouvée : {team_name_slug} {season} {stat_type}")
            return "not_found", driver

    except Exception as e:
        logger.error(f"  ❌ Erreur lors du scraping de {team} {season} {stat_type} : {e}")
        return "error", driver


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper FBref Match Logs")
    parser.add_argument("--league",   default=None, help="Filtrer sur une ligue")
    parser.add_argument("--season",   default=None, help="Filtrer sur une saison")
    parser.add_argument("--dry-run",  action="store_true", help="Affiche ce qui serait scrapé sans télécharger")
    parser.add_argument("--headless", action="store_true", help="Lancer Chrome sans interface graphique")
    args = parser.parse_args()

    # Charger le référentiel pour construire la liste ligue × saison
    if not DB_PATH.exists():
        logger.error(f"Base de données DuckDB introuvable : {DB_PATH}")
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH))
    referentiel = conn.execute("""
        SELECT DISTINCT season, league
        FROM referentiel.transfermarkt_clubs
        ORDER BY league, season
    """).df()
    conn.close()

    # Filtres depuis config.yaml
    seasons_cfg = set(CFG.get("seasons", []))
    leagues_cfg = set(CFG.get("leagues", []))
    stat_types  = CFG.get("stat_types", ["schedule", "shooting", "keeper", "misc"])

    # Override avec les arguments CLI
    if args.league:
        leagues_cfg = {args.league}
    if args.season:
        seasons_cfg = {args.season}

    mask = (
        referentiel["league"].isin(leagues_cfg) &
        referentiel["season"].isin(seasons_cfg)
    )
    to_process = referentiel[mask].drop_duplicates().reset_index(drop=True)

    logger.info(f"Combinaisons ligue×saison à traiter : {len(to_process)}")

    if args.dry_run:
        logger.info("[DRY-RUN] Aucun fichier ne sera téléchargé")

    driver = None
    if not args.dry_run:
        driver = create_driver(headless=args.headless)

    stats = {"ok": 0, "skip": 0, "not_found": 0, "error": 0}

    try:
        for _, ls_row in to_process.iterrows():
            league      = ls_row["league"]
            season      = ls_row["season"]
            season_code = season_to_code(season)

            if league not in LEAGUE_CONFIG:
                logger.warning(f"  Ligue non configurée : {league}")
                continue

            logger.info(f"\n  === {league} {season} ===")

            if not args.dry_run:
                driver = ensure_driver_alive(driver, headless=args.headless)
                teams = get_all_team_urls(driver, league, season)
            else:
                teams = []

            if not teams and not args.dry_run:
                logger.warning(f"  Aucune équipe trouvée : {league} {season}")
                continue

            # Identification des fichiers manquants
            to_download = []
            for team in teams:
                team_slug = team["team_slug"]
                missing   = [st for st in stat_types if not is_cached(team_slug, season_code, st)]
                if missing:
                    to_download.append({"team": team, "missing_stats": missing})
                else:
                    stats["skip"] += len(stat_types)

            if not to_download and not args.dry_run:
                logger.info(f"  ⏩ Tout en cache : {league} {season}")
                continue

            # 3. Scraper uniquement ce qui manque
            for item in to_download:
                team_slug = item["team"]["team_slug"]
                team_id   = item["team"]["team_id"]
                stat_urls = build_stat_urls(team_id, team_slug, season)

                for stat_type in item["missing_stats"]:
                    max_retries = 2
                    for attempt in range(max_retries):
                        driver = ensure_driver_alive(driver, headless=args.headless)
                        
                        result, driver = scrape_stat_page(
                            driver, team_slug, season, stat_type,
                            url=stat_urls[stat_type],
                            team_name_slug=team_slug,
                            season_code=season_code,
                        )
                        
                        # Si succès ou fichier introuvable, on sort de la boucle de retry
                        if result in ["ok", "not_found"]:
                            stats[result] += 1
                            break
                        
                        # Si erreur (ex: WinError 10061/crash driver), ensure_driver_alive aura réparé le driver
                        # au début du prochain passage de boucle pour rejouer LA MÊME page.
                        if attempt < max_retries - 1:
                            logger.warning(f"  🔄 Nouvelle tentative pour {team_slug} {stat_type} (essai {attempt + 2}/{max_retries})...")
                            time.sleep(random.uniform(3, 5))
                        else:
                            logger.error(f"  ❌ Échec définitif pour {team_slug} {stat_type}")
                            stats["error"] += 1

                logger.info(f"  [{team_slug}] OK:{stats['ok']} SKIP:{stats['skip']} ERR:{stats['error']}")
                time.sleep(random.uniform(3, 6))

            time.sleep(random.uniform(4, 8))

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    logger.success(
        f"Scraping terminé — OK:{stats['ok']} SKIP:{stats['skip']} "
        f"NOT_FOUND:{stats['not_found']} ERR:{stats['error']}"
    )


if __name__ == "__main__":
    main()