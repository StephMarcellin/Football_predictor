"""
Scraper WhoScored — Team Statistics
=====================================
Scrape les stats agrégées par équipe et saison depuis WhoScored.
Catégories : Defensive, Offensive, xG (For + Against) × Home + Away = 8 fichiers par ligue×saison.

Usage :
    python pipelines/scrape_whoscored.py
    python pipelines/scrape_whoscored.py --league Bundesliga --season 2022-2023
    python pipelines/scrape_whoscored.py --dry-run
"""

import os
import time
import random
import argparse
from pathlib import Path

import yaml
from loguru import logger
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent
CFG_PATH = ROOT_DIR / "scraping_config.yaml"
MAIN_CFG = ROOT_DIR / "config.yaml"
LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_whoscored.log",
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

OUT_DIR = Path(MAIN_CFG_DATA["paths"]["raw_data"]) / "whoscored" / "html"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LEAGUE_CONFIG = {
    "Premier League": {
        "url":  "https://www.whoscored.com/regions/252/tournaments/2/england-premier-league",
        "slug": "ENG-Premier-League",
    },
    "Ligue 1": {
        "url":  "https://www.whoscored.com/regions/74/tournaments/22/france-ligue-1",
        "slug": "FRA-Ligue-1",
    },
    "Bundesliga": {
        "url":  "https://www.whoscored.com/regions/81/tournaments/3/germany-bundesliga",
        "slug": "GER-Bundesliga",
    },
    "Serie A": {
        "url":  "https://www.whoscored.com/regions/108/tournaments/5/italy-serie-a",
        "slug": "ITA-Serie-A",
        # "substage": "Serie A",
    },
    "La Liga": {
        "url":  "https://www.whoscored.com/regions/206/tournaments/4/spain-laliga",
        "slug": "ESP-La-Liga",
    },
}

# 8 combinaisons attendues par ligue×saison
CATEGORIES = ["Defensive", "Offensive"]
XG_TYPES   = ["For", "Against"]
VENUES     = ["Home", "Away"]

EXPECTED_FILES = (
    [f"{cat}_{venue}" for cat in CATEGORIES for venue in VENUES] +
    [f"xG_{venue}_{t}" for venue in VENUES for t in XG_TYPES]
)  # 8 fichiers


# ── Utilitaires ───────────────────────────────────────────────────────────────

def season_to_code(season: str) -> str:
    """'2022-2023' → '2223'"""
    parts = season.split("-")
    return f"{parts[0][-2:]}{parts[1][-2:]}"


def season_to_whoscored(season: str) -> str:
    """'2022-2023' → '2022/2023'"""
    return season.replace("-", "/")


def is_file_cached(slug: str, season_code: str, combo: str) -> bool:
    filename = f"{combo}_{slug}_{season_code}.html"
    return (OUT_DIR / filename).exists()


def is_league_season_cached(slug: str, season_code: str) -> bool:
    return all(is_file_cached(slug, season_code, c) for c in EXPECTED_FILES)


def missing_combos(slug: str, season_code: str) -> list[str]:
    return [c for c in EXPECTED_FILES if not is_file_cached(slug, season_code, c)]


def human_delay(min_s: float = 2.0, max_s: float = 5.0):
    time.sleep(random.uniform(min_s, max_s))


def wait_for_loading(driver, timeout: int = 10):
    try:
        WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CLASS_NAME, "loading-mask"))
        )
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located((By.CLASS_NAME, "loading-mask"))
        )
    except Exception:
        time.sleep(2)


def safe_click(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(random.uniform(0.3, 0.7))
    ActionChains(driver).move_to_element(element).pause(
        random.uniform(0.2, 0.5)
    ).click().perform()


def handle_cookies(driver):
    time.sleep(4)
    try:
        btns = driver.find_elements(
            By.XPATH,
            "//button[contains(., 'Tout accepter') or contains(., 'Accept All')]"
        )
        if btns:
            safe_click(driver, btns[0])
            logger.debug("  🍪 Cookies acceptés (page principale)")
            return
    except Exception:
        pass

    for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.frame(iframe)
            btns = driver.find_elements(
                By.XPATH,
                "//button[contains(., 'Tout accepter') or contains(., 'Accept All')]"
            )
            if btns:
                safe_click(driver, btns[0])
                logger.debug("  🍪 Cookies acceptés (iframe)")
                driver.switch_to.default_content()
                return
            driver.switch_to.default_content()
        except Exception:
            driver.switch_to.default_content()


def ensure_all_teams_visible(driver):
    try:
        all_link = driver.find_element(By.LINK_TEXT, "All")
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", all_link
        )
        time.sleep(1)
        all_link.click()
        wait_for_loading(driver)
    except Exception:
        pass


def save_page(driver, filename: str):
    path = OUT_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    logger.success(f"  ✅ {filename}")


# ── Scraping ──────────────────────────────────────────────────────────────────

def select_season(driver, season_text: str) -> bool:
    """Sélectionne la saison dans le menu WhoScored. Retourne True si succès."""
    try:
        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "seasons"))
        )
        select = Select(select_elem)
        current = select.first_selected_option.text.strip()

        if season_text in current:
            logger.debug(f"  Saison {season_text} déjà active")
            return True

        logger.debug(f"  Passage de {current} → {season_text}")
        select.select_by_visible_text(season_text)
        wait_for_loading(driver)
        human_delay(4, 6)
        return True

    except Exception as e:
        logger.warning(f"  Impossible de sélectionner la saison {season_text} : {e}")
        return False


def scrape_league_season(
    driver,
    league: str,
    season: str,
    dry_run: bool = False,
) -> dict:
    """
    Scrape les 8 fichiers HTML pour une ligue × saison.
    Retourne un dict de compteurs.
    """
    cfg         = LEAGUE_CONFIG[league]
    slug        = cfg["slug"]
    season_code = season_to_code(season)
    season_ws   = season_to_whoscored(season)
    stats       = {"ok": 0, "skip": 0, "error": 0}

    # Vérification cache complète
    if is_league_season_cached(slug, season_code):
        logger.debug(f"  ⏩ Cache complet : {league} {season}")
        stats["skip"] += len(EXPECTED_FILES)
        return stats

    missing = missing_combos(slug, season_code)
    logger.info(f"  {len(missing)} fichier(s) manquant(s) : {missing}")

    if dry_run:
        for m in missing:
            logger.info(f"  [DRY-RUN] {m}_{slug}_{season_code}.html")
            stats["ok"] += 1
        stats["skip"] += len(EXPECTED_FILES) - len(missing)
        return stats

    # Navigation vers la ligue
    logger.info(f"  Navigation : {cfg['url']}")
    driver.get(cfg["url"])
    human_delay(5, 8)
    handle_cookies(driver)

    # Sélection de la saison
    if not select_season(driver, season_ws):
        stats["error"] += len(missing)
        return stats

    # 2. Sélection du substage AVANT Team Statistics
    substage = cfg.get("substage")
    if substage:
        try:
            stage_select = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "stages"))
            )
            select = Select(stage_select)
            
            # Log toutes les options disponibles pour diagnostic
            options = [o.text.strip() for o in select.options]
            logger.debug(f"  Stages disponibles : {options}")
            
            # Forcer la sélection sans vérifier l'état actuel
            select.select_by_visible_text(substage)
            wait_for_loading(driver)
            human_delay(2, 3)
            logger.debug(f"  Substage forcé : {substage}")

        except Exception as e:
            logger.warning(f"  Substage '{substage}' non trouvé : {e}")
            logger.debug(f"  Options disponibles : {[o.text for o in Select(driver.find_element(By.ID, 'stages')).options]}")
    # Navigation vers Team Statistics
    try:
        stats_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Team Statistics"))
        )
        safe_click(driver, stats_tab)
        wait_for_loading(driver)
    except Exception as e:
        logger.error(f"  Onglet Team Statistics introuvable : {e}")
        stats["error"] += len(missing)
        return stats

    

    # Scraper chaque combinaison manquante
    for combo in missing:
        parts = combo.split("_")  # ex: "Defensive_Home" ou "xG_Home_For"

        try:
            if parts[0] == "xG":
                # xG_Home_For → cat=xG, venue=Home, type=For
                _, venue, xg_type = parts
                # Cliquer sur l'onglet xG (WhoScored l'appelle "Detailed" ou "xG")
                try:
                    cat_btn = driver.find_element(By.LINK_TEXT, "xG")
                except Exception:
                    cat_btn = driver.find_element(By.LINK_TEXT, "Detailed")
                safe_click(driver, cat_btn)
                wait_for_loading(driver)

                venue_btn = driver.find_element(By.LINK_TEXT, venue)
                safe_click(driver, venue_btn)
                wait_for_loading(driver)

                type_btn = driver.find_element(By.LINK_TEXT, xg_type)
                safe_click(driver, type_btn)
                wait_for_loading(driver)

            else:
                # Defensive_Home ou Offensive_Away
                cat, venue = parts
                cat_btn = driver.find_element(By.LINK_TEXT, cat)
                safe_click(driver, cat_btn)
                wait_for_loading(driver)

                venue_btn = driver.find_element(By.LINK_TEXT, venue)
                safe_click(driver, venue_btn)
                wait_for_loading(driver)

            ensure_all_teams_visible(driver)
            filename = f"{combo}_{slug}_{season_code}.html"
            save_page(driver, filename)
            stats["ok"] += 1
            human_delay(2, 4)

        except Exception as e:
            logger.error(f"  ❌ Erreur {combo} : {e}")
            stats["error"] += 1

    stats["skip"] += len(EXPECTED_FILES) - len(missing)
    return stats


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper WhoScored")
    parser.add_argument("--league",   default=None, help="Filtrer sur une ligue")
    parser.add_argument("--season",   default=None, help="Filtrer sur une saison")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Affiche ce qui serait scrapé sans télécharger")
    parser.add_argument("--headless", action="store_true",
                        help="Lancer Chrome sans interface graphique")
    args = parser.parse_args()

    seasons_cfg = set(SCRAP_CFG.get("seasons", []))
    leagues_cfg = set(SCRAP_CFG.get("leagues", []))

    # Filtres CLI
    if args.league:
        leagues_cfg = {args.league}
    if args.season:
        seasons_cfg = {args.season}

    # Restreindre aux ligues Big5 supportées par WhoScored
    leagues_cfg = {l for l in leagues_cfg if l in LEAGUE_CONFIG}

    total_stats = {"ok": 0, "skip": 0, "error": 0}

    driver = None
    if not args.dry_run:
        driver = Driver(uc=True, headless=args.headless)

    try:
        for league in sorted(leagues_cfg):
            for season in sorted(seasons_cfg):
                logger.info(f"\n  === {league} {season} ===")
                result = scrape_league_season(
                    driver, league, season, dry_run=args.dry_run
                )
                for k, v in result.items():
                    total_stats[k] += v

                human_delay(20, 40)  # pause entre ligue×saison

    finally:
        if driver:
            driver.quit()

    logger.success(
        f"Scraping WhoScored terminé — "
        f"OK:{total_stats['ok']} SKIP:{total_stats['skip']} ERR:{total_stats['error']}"
    )


if __name__ == "__main__":
    main()