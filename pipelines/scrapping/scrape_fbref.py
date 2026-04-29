"""
Scraper FBref — Match Logs par club et saison
=============================================
Utilise le référentiel DuckDB pour garantir la complétude :
chaque club × saison × ligue du référentiel est visité.

Usage :
# Tout scraper selon scraping_config.yaml
python pipelines/scrape_fbref.py

# Tester sans télécharger
python pipelines/scrape_fbref.py --dry-run

# Une seule ligue/saison
python pipelines/scrape_fbref.py --league Bundesliga --season 2017-2018

# Sans interface graphique
python pipelines/scrape_fbref.py --headless
"""

import os
import re
import sys
import time
import random
import argparse
from pathlib import Path
from itertools import groupby

import yaml
import duckdb
from loguru import logger
from seleniumbase import Driver

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR   = Path(__file__).resolve().parent.parent
CFG_PATH   = ROOT_DIR / "scraping_config.yaml"
MAIN_CFG   = ROOT_DIR / "config.yaml"
LOG_DIR    = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_fbref.log",
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

DATA_DIR = Path(MAIN_CFG_DATA["paths"]["raw_data"]) / "fbref" / "html"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(MAIN_CFG_DATA["paths"]["db"])

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
    """Vérifie si le fichier HTML existe déjà — recherche par slug exact."""
    filename = f"matchlogs_{team_slug}_{season_code}_{stat_type}.html"
    return (DATA_DIR / filename).exists()

def is_driver_alive(driver) -> bool:
    """Vérifie si le driver Chrome est encore actif."""
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def restart_driver(headless: bool = False):
    """Recrée un driver Chrome après un crash."""
    logger.warning("  🔄 Redémarrage du driver Chrome...")
    try:
        driver = Driver(uc=True, headless=headless)
        driver.uc_open_with_reconnect("https://fbref.com", 5)
        time.sleep(random.uniform(2, 3))
        accept_cookies(driver)
        logger.info("  ✅ Driver redémarré")
        return driver
    except Exception as e:
        logger.error(f"  ❌ Impossible de redémarrer le driver : {e}")
        raise


def find_team_url(driver, league: str, season: str, team: str) -> str | None:
    cfg        = LEAGUE_CONFIG[league]
    fbref_id   = cfg["fbref_id"]
    fbref_slug = cfg["fbref_slug"]

    url = (
        f"https://fbref.com/en/comps/{fbref_id}/{season}/"
        f"stats/{season}-{fbref_slug}-Stats"
    )

    logger.debug(f"  Navigation page saison : {url}")
    driver.uc_open_with_reconnect(url, 5)
    accept_cookies(driver)  # ← toujours appelé ici, pas seulement au démarrage
    time.sleep(random.uniform(2, 3))

    elements = driver.find_elements("css selector", 'th[data-stat="team"] a')

    if not elements:
        logger.warning(f"  Liste équipes vide pour {league} {season} — retry")
        time.sleep(3)
        elements = driver.find_elements("css selector", 'th[data-stat="team"] a')

    # Correspondance exacte d'abord
    for el in elements:
        link_text = el.text.strip()
        if team.lower() == link_text.lower():
            team_url = el.get_attribute("href")
            logger.debug(f"  Exact match : '{link_text}' → {team_url}")
            return team_url

    # Correspondance contenue
    for el in elements:
        link_text = el.text.strip()
        if team.lower() in link_text.lower() or link_text.lower() in team.lower():
            team_url = el.get_attribute("href")
            logger.debug(f"  Partial match : '{team}' ↔ '{link_text}' → {team_url}")
            return team_url

    # Correspondance par mots — SEUIL STRICT : tous les mots doivent matcher
    team_words = set(team.lower().split())
    for el in elements:
        link_words = set(el.text.strip().lower().split())
        if team_words.issubset(link_words) or link_words.issubset(team_words):
            team_url = el.get_attribute("href")
            logger.debug(f"  Word match : '{team}' ↔ '{el.text.strip()}' → {team_url}")
            return team_url

    logger.warning(f"  Aucune URL trouvée pour '{team}' dans {league} {season}")
    logger.debug(f"  Équipes disponibles : {[el.text.strip() for el in elements]}")
    return None

def build_stat_urls(team_id: str, team_name_slug: str, season: str) -> dict[str, str]:
    """Construit les 4 URLs FBref pour un club × saison."""
    base = f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps"
    return {
        "schedule": f"{base}/schedule/{team_name_slug}-Scores-and-Fixtures-All-Competitions",
        "shooting":  f"{base}/shooting/{team_name_slug}-Match-Logs-All-Competitions",
        "keeper":    f"{base}/keeper/{team_name_slug}-Match-Logs-All-Competitions",
        "misc":      f"{base}/misc/{team_name_slug}-Match-Logs-All-Competitions",
    }

def scrape_team_season(
    driver,
    team: str,
    season: str,
    league: str,
    stat_type: str,
    team_url: str | None = None,  # ← nouveau paramètre
    dry_run: bool = False,
) -> str:
    season_code = season_to_code(season)

    if is_cached(team, season_code, stat_type):
        logger.debug(f"  ⏩ Cache : {team} {season} {stat_type}")
        return "skip"

    if dry_run:
        logger.info(f"  [DRY-RUN] À scraper : {team} {season} {stat_type}")
        return "ok"

    # Utiliser l'URL passée en paramètre ou la résoudre
    if team_url is None:
        team_url = find_team_url(driver, league, season, team)
    if not team_url:
        logger.warning(f"  ⚠️ Club non trouvé : {team} dans {league} {season}")
        return "not_found"

    # Format : https://fbref.com/en/squads/{team_id}/{season}/{slug}-Stats
    url_parts      = team_url.rstrip("/").split("/")
    squads_idx     = url_parts.index("squads")
    team_id        = url_parts[squads_idx + 1]
    team_name_slug = url_parts[-1].replace("-Stats", "")

    logger.debug(f"  team_id extrait : {team_id}")
    logger.debug(f"  team_name_slug  : {team_name_slug}")

    target_url = (
        f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/"
        f"all_comps/{stat_type}/{team_name_slug}-Match-Logs-All-Competitions"
    )

    filename  = f"matchlogs_{team_name_slug}_{season_code}_{stat_type}.html"
    file_path = DATA_DIR / filename

    try:
        logger.info(f"  📡 {team_name_slug} {season} {stat_type}")
        driver.uc_open_with_reconnect(target_url, 4)
        driver.execute_script("window.scrollTo(0, 800);")

        # Attendre explicitement que la table soit chargée
        try:
            driver.wait_for_element('table#matchlogs_for', timeout=15)
        except Exception:
            pass  # On vérifie quand même le page_source après

        time.sleep(random.uniform(2, 4))  # petit délai supplémentaire

        logger.debug(f"  Page source (500 chars) : {driver.page_source[:500]}")
        logger.debug(f"  'matchlogs_for' présent : {'matchlogs_for' in driver.page_source}")
        logger.debug(f"  URL actuelle : {driver.current_url}")

        if "matchlogs_for" in driver.page_source:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.success(f"  ✅ {filename}")
            return "ok"
        else:
            logger.warning(f"  ⚠️ Table non trouvée : {team_name_slug} {season} {stat_type}")
            return "not_found"

    except Exception as e:
        logger.error(f"  ❌ Erreur {team} {season} {stat_type} : {e}")
        time.sleep(random.uniform(10, 15))
        return "error"

def accept_cookies(driver) -> None:
    """Accepte le bandeau cookies FBref si présent."""
    try:
        # Osano consent manager (utilisé par FBref)
        btn = driver.find_element(
            "css selector",
            "button.osano-cm-accept-all, button.osano-cm-button--type_accept"
        )
        if btn:
            btn.click()
            logger.debug("  🍪 Cookies acceptés")
            time.sleep(1)
    except Exception:
        pass  # Pas de bandeau, on continue

def get_all_team_urls(driver, league: str, season: str) -> list[dict]:
    """
    Visite la page de stats FBref et retourne TOUS les clubs présents.
    Retourne une liste de dicts {team_slug, team_id, team_url}
    """
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
        if not href:
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

        seen = set()
    unique_teams = []
    for t in teams:
        if t["team_id"] not in seen:
            seen.add(t["team_id"])
            unique_teams.append(t)

    logger.info(f"  {len(unique_teams)} équipes uniques trouvées : {league} {season}")
    logger.debug(f"  Slugs : {[t['team_slug'] for t in unique_teams]}")
    return unique_teams

def scrape_stat_page(
    driver,
    team: str,
    season: str,
    stat_type: str,
    url: str,
    team_name_slug: str,
    season_code: str,
) -> str:
    filename  = f"matchlogs_{team_name_slug}_{season_code}_{stat_type}.html"
    file_path = DATA_DIR / filename

    try:
        logger.info(f"  📡 {team_name_slug} {season} {stat_type}")
        driver.uc_open_with_reconnect(url, 4)
        accept_cookies(driver)
        time.sleep(random.uniform(1, 2))

        if "matchlogs_for" in driver.page_source:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.success(f"  ✅ {filename}")
            return "ok"
        else:
            logger.warning(f"  ⚠️ Table non trouvée : {team_name_slug} {season} {stat_type}")
            return "not_found"

    except Exception as e:
        logger.error(f"  ❌ Erreur {team} {season} {stat_type} : {e}")
        time.sleep(random.uniform(5, 10))
        return "error"
    
# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper FBref Match Logs")
    parser.add_argument("--league",   default=None, help="Filtrer sur une ligue")
    parser.add_argument("--season",   default=None, help="Filtrer sur une saison")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Affiche ce qui serait scrapé sans télécharger")
    parser.add_argument("--headless", action="store_true",
                        help="Lancer Chrome sans interface graphique")
    args = parser.parse_args()

    # Charger le référentiel pour construire la liste ligue × saison
    conn = duckdb.connect(str(DB_PATH))
    referentiel = conn.execute("""
        SELECT DISTINCT season, league FROM referentiel.club_D1
        UNION
        SELECT DISTINCT season, league FROM referentiel.club_D2
        ORDER BY league, season
    """).df()
    conn.close()

    # Filtres depuis scraping_config.yaml
    seasons_cfg = set(SCRAP_CFG.get("seasons", []))
    leagues_cfg = set(SCRAP_CFG.get("leagues", []))
    stat_types  = SCRAP_CFG.get("stat_types", ["schedule", "shooting", "keeper", "misc"])

    # Filtres CLI
    if args.league:
        leagues_cfg = {args.league}
    if args.season:
        seasons_cfg = {args.season}

    # Construire la liste des combinaisons ligue × saison à traiter
    mask = (
        referentiel["league"].isin(leagues_cfg) &
        referentiel["season"].isin(seasons_cfg)
    )
    to_process = referentiel[mask].drop_duplicates().reset_index(drop=True)

    logger.info(f"Combinaisons ligue×saison à traiter : {len(to_process)}")

    if args.dry_run:
        logger.info("[DRY-RUN] Aucun fichier ne sera téléchargé")

    # Initialiser le driver
    driver = None
    if not args.dry_run:
        driver = Driver(uc=True, headless=args.headless)
        driver.uc_open_with_reconnect("https://fbref.com", 5)
        time.sleep(random.uniform(1, 2))
        accept_cookies(driver)

    # Compteurs
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

            # 1. Récupérer tous les clubs depuis la page FBref — 1 seule visite
            teams = get_all_team_urls(driver, league, season)
            if not teams:
                logger.warning(f"  Aucune équipe trouvée : {league} {season}")
                continue

            # 2. Identifier ce qui manque avant de scraper
            to_download = []
            for team in teams:
                team_slug = team["team_slug"]
                missing   = [
                    st for st in stat_types
                    if not is_cached(team_slug, season_code, st)
                ]
                if missing:
                    to_download.append({"team": team, "missing_stats": missing})
                else:
                    logger.debug(f"  ⏩ Cache complet : {team_slug} {season}")
                    stats["skip"] += len(stat_types)

            if not to_download:
                logger.info(f"  ⏩ Tout en cache : {league} {season}")
                time.sleep(random.uniform(1, 2))
                continue

            logger.info(
                f"  {len(to_download)} club(s) à scraper sur {len(teams)} — "
                f"{sum(len(t['missing_stats']) for t in to_download)} fichiers manquants"
            )

            # 3. Scraper uniquement ce qui manque
            for item in to_download:
                team_slug = item["team"]["team_slug"]
                team_id   = item["team"]["team_id"]
                stat_urls = build_stat_urls(team_id, team_slug, season)

                for stat_type in item["missing_stats"]:
                    try:
                        result = scrape_stat_page(
                            driver, team_slug, season, stat_type,
                            url=stat_urls[stat_type],
                            team_name_slug=team_slug,
                            season_code=season_code,
                        )
                    except ConnectionError:
                        logger.warning("  Driver crash — redémarrage")
                        driver = restart_driver(args.headless)
                        result = "error"

                    stats[result] += 1

                logger.info(
                    f"  [{team_slug}] OK:{stats['ok']} SKIP:{stats['skip']} ERR:{stats['error']}"
                )
                time.sleep(random.uniform(3, 7))

            time.sleep(random.uniform(5, 10))
        

    finally:
        if driver:
            driver.quit()

    logger.success(
        f"Scraping terminé — OK:{stats['ok']} SKIP:{stats['skip']} "
        f"NOT_FOUND:{stats['not_found']} ERR:{stats['error']}"
    )


if __name__ == "__main__":
    main()