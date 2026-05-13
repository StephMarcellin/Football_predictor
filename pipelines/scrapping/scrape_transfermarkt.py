"""
Scraper Transfermarkt — Liste des clubs par championnat et saison
=================================================================
Récupère la liste des clubs participants pour chaque saison d'un
championnat donné depuis transfermarkt.fr.

Cible : table.items dans la page startseite d'une compétition.
Output : un CSV par ligue × saison dans raw_data/transfermarkt/csv/

Usage :
    python pipelines/scrape_transfermarkt.py
    python pipelines/scrape_transfermarkt.py --league "Ligue 1" --season 2022-2023
    python pipelines/scrape_transfermarkt.py --dry-run
    python pipelines/scrape_transfermarkt.py --headless
"""

import argparse
import random
import time
from pathlib import Path

import pandas as pd
import yaml
from bs4 import BeautifulSoup
from loguru import logger
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
CFG_PATH = ROOT_DIR / "config.yaml"
MAIN_CFG = ROOT_DIR / "config.yaml"
LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_transfermarkt.log",
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

OUT_DIR = ROOT_DIR / MAIN_CFG_DATA["paths"]["raw_data"] / "transfermarkt" / "csv"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Mapping ligue → URL Transfermarkt ────────────────────────────────────────
# Format : https://www.transfermarkt.fr/{slug}/startseite/wettbewerb/{code}/plus/?saison_id={year}
# {year} = première année de la saison (ex: 2022 pour 2022-2023)

LEAGUE_CONFIG = {
    "Premier League": {
        "slug": "premier-league",
        "code": "GB1",
        "tm_slug": "ENG-Premier-League",
    },
    "Ligue 1": {
        "slug": "ligue-1",
        "code": "FR1",
        "tm_slug": "FRA-Ligue-1",
    },
    "Bundesliga": {
        "slug": "bundesliga",
        "code": "L1",
        "tm_slug": "GER-Bundesliga",
    },
    "Serie A": {
        "slug": "serie-a",
        "code": "IT1",
        "tm_slug": "ITA-Serie-A",
    },
    "La Liga": {
        "slug": "laliga",
        "code": "ES1",
        "tm_slug": "ESP-La-Liga",
    },
    "Championship": {
        "slug": "championship",
        "code": "GB2",
        "tm_slug": "ENG-Championship",
    },
    "Ligue 2": {
        "slug": "ligue-2",
        "code": "FR2",
        "tm_slug": "FRA-Ligue-2",
    },
    "2. Bundesliga": {
        "slug": "2-bundesliga",
        "code": "L2",
        "tm_slug": "GER-Bundesliga-2",
    },
    "Serie B": {
        "slug": "serie-b",
        "code": "IT2",
        "tm_slug": "ITA-Serie-B",
    },
    "La Liga 2": {
        "slug": "laliga2",
        "code": "ES2",
        "tm_slug": "ESP-La-Liga-2",
    },
}

BASE_URL = "https://www.transfermarkt.fr"

# Headers pour simuler un navigateur réel (utilisé en fallback requests)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.transfermarkt.fr/",
}


# ── Utilitaires ───────────────────────────────────────────────────────────────

def season_to_year(season: str) -> int:
    """'2022-2023' → 2022 (première année, format Transfermarkt)"""
    return int(season.split("-")[0])


def season_to_code(season: str) -> str:
    """'2022-2023' → '2223'"""
    parts = season.split("-")
    return f"{parts[0][-2:]}{parts[1][-2:]}"


def build_url(league: str, season: str) -> str:
    """Construit l'URL Transfermarkt pour une ligue × saison."""
    cfg  = LEAGUE_CONFIG[league]
    year = season_to_year(season)
    return (
        f"{BASE_URL}/{cfg['slug']}/startseite/wettbewerb/{cfg['code']}"
        f"/plus/?saison_id={year}"
    )


def is_cached(league: str, season_code: str) -> bool:
    """Vérifie si le CSV existe déjà."""
    slug = LEAGUE_CONFIG[league]["tm_slug"]
    path = OUT_DIR / f"clubs_{slug}_{season_code}.csv"
    return path.exists()


def human_delay(min_s: float = 2.0, max_s: float = 5.0):
    time.sleep(random.uniform(min_s, max_s))


def accept_cookies(driver) -> None:
    """Accepte le bandeau cookies Transfermarkt si présent."""
    try:
        # Transfermarkt utilise un iframe pour la CMP (Consent Management Platform)
        for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
            try:
                driver.switch_to.frame(iframe)
                btn = driver.find_element(
                    By.XPATH,
                    "//button[contains(., 'Tout accepter') or "
                    "contains(., 'Accept all') or "
                    "contains(., 'Akzeptieren')]"
                )
                btn.click()
                logger.debug("  🍪 Cookies acceptés (iframe)")
                driver.switch_to.default_content()
                time.sleep(1)
                return
            except Exception:
                driver.switch_to.default_content()

        # Tentative directe si pas dans un iframe
        btn = driver.find_element(
            By.XPATH,
            "//button[contains(., 'Tout accepter') or contains(., 'Accept all')]"
        )
        btn.click()
        logger.debug("  🍪 Cookies acceptés (page principale)")
        time.sleep(1)
    except Exception:
        pass  # Pas de bandeau, on continue


# ── Scraping ──────────────────────────────────────────────────────────────────

def fetch_page_html(driver, url: str) -> str | None:
    """
    Navigue vers l'URL cible et retourne le page source HTML.
    Attend que la table.items soit chargée avant de retourner.
    Retourne None si la table est absente après chargement.
    """
    try:
        logger.debug(f"  Navigation : {url}")
        driver.uc_open_with_reconnect(url, 5)
        accept_cookies(driver)
        human_delay(2, 4)

        # Attendre que la table principale soit présente
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.items"))
            )
        except Exception:
            logger.warning(f"  ⚠️ Table .items non trouvée après chargement : {url}")
            return None

        return driver.page_source

    except Exception as e:
        logger.error(f"  ❌ Erreur de navigation vers {url} : {e}")
        return None


def parse_clubs_table(html: str, league: str, season: str) -> pd.DataFrame:
    """
    Parse la table.items depuis le HTML Transfermarkt.
    
    Structure DOM ciblée :
        <table class="items">
          <tbody>
            <tr class="odd">  ou  <tr class="even">
              <td class="hauptlink no-border-links">
                <a href="/club-name/startseite/verein/123">Club Name</a>
              </td>
              ... autres colonnes (pays, valeur marchande, etc.)
            </tr>
          </tbody>
        </table>

    Retourne un DataFrame avec les colonnes :
        club_name, club_url, club_tm_id, league, season, scraped_at
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="items")

    if not table:
        logger.warning("  Table .items introuvable dans le HTML parsé")
        return pd.DataFrame()

    rows = []
    scraped_at = pd.Timestamp.now().isoformat(timespec="seconds")

    # Les lignes alternent entre class="odd" et class="even"
    for tr in table.find_all("tr", class_=["odd", "even"]):
        # Cellule principale : nom du club
        td_name = tr.find("td", class_="hauptlink no-border-links")
        if not td_name:
            continue

        link = td_name.find("a", href=True)
        if not link:
            continue

        club_name = link.get_text(strip=True)
        club_href = link["href"]  # ex: "/paris-saint-germain/startseite/verein/583"

        # Extraire l'ID Transfermarkt depuis l'URL
        # Format : /club-slug/startseite/verein/{tm_id}
        tm_id = None
        parts = club_href.strip("/").split("/")
        if "verein" in parts:
            verein_idx = parts.index("verein")
            if verein_idx + 1 < len(parts):
                try:
                    tm_id = int(parts[verein_idx + 1])
                except ValueError:
                    pass

        club_url = BASE_URL + club_href if club_href.startswith("/") else club_href

        # Valeur marchande totale (optionnelle — dernière colonne td.rechts)
        # On la récupère si disponible pour enrichir le dataset
        market_value = None
        tds = tr.find_all("td")
        for td in reversed(tds):
            text = td.get_text(strip=True)
            if text and any(c in text for c in ["Mrd.", "Mio.", "Tsd.", "€", "m", "bn", "k"]):
                market_value = text
                break

        rows.append({
            "club_name":     club_name,
            "club_url":      club_url,
            "club_tm_id":    tm_id,
            "market_value":  market_value,
            "league":        league,
            "season":        season,
            "scraped_at":    scraped_at,
        })

    logger.debug(f"  {len(rows)} club(s) parsés depuis la table")
    return pd.DataFrame(rows)


# ── Scraping d'une ligue × saison ─────────────────────────────────────────────

def scrape_league_season(
    driver,
    league: str,
    season: str,
    dry_run: bool = False,
) -> str:
    """
    Scrape la liste des clubs pour une ligue × saison.
    Retourne : 'ok' | 'skip' | 'not_found' | 'error'
    """
    season_code = season_to_code(season)

    if is_cached(league, season_code):
        logger.debug(f"  ⏩ Cache : {league} {season}")
        return "skip"

    url = build_url(league, season)
    logger.info(f"  📡 {league} {season} → {url}")

    if dry_run:
        logger.info(f"  [DRY-RUN] À scraper : {league} {season}")
        return "ok"

    # Récupérer le HTML
    html = fetch_page_html(driver, url)
    if not html:
        return "not_found"

    # Parser la table
    df = parse_clubs_table(html, league, season)
    if df.empty:
        logger.warning(f"  ⚠️ Aucun club extrait : {league} {season}")
        return "not_found"

    # Sauvegarder en CSV
    slug     = LEAGUE_CONFIG[league]["tm_slug"]
    filename = f"clubs_{slug}_{season_code}.csv"
    out_path = OUT_DIR / filename

    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.success(f"  ✅ {filename} — {len(df)} clubs")

    return "ok"


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scraper Transfermarkt — Liste des clubs par ligue et saison"
    )
    parser.add_argument("--league",   default=None,
                        help="Filtrer sur une ligue (ex: 'Ligue 1')")
    parser.add_argument("--season",   default=None,
                        help="Filtrer sur une saison (ex: 2022-2023)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Affiche ce qui serait scrapé sans télécharger")
    parser.add_argument("--headless", action="store_true",
                        help="Lancer Chrome sans interface graphique (déconseillé)")
    args = parser.parse_args()

    # Charger la config de scraping
    seasons_cfg = set(SCRAP_CFG.get("seasons", []))
    leagues_cfg = set(SCRAP_CFG.get("leagues", []))

    # Filtres CLI — prioritaires sur le YAML
    if args.league:
        leagues_cfg = {args.league}
    if args.season:
        seasons_cfg = {args.season}

    # Restreindre aux ligues connues de Transfermarkt
    leagues_cfg = {l for l in leagues_cfg if l in LEAGUE_CONFIG}

    if not leagues_cfg:
        logger.warning(
            "  Aucune ligue compatible avec Transfermarkt. "
            "Vérifie config.yaml ou --league."
        )
        return

    # Construire la liste des tâches (ligue × saison) dans l'ordre
    tasks = sorted(
        [(league, season) for league in leagues_cfg for season in seasons_cfg]
    )

    logger.info(f"=== Scraper Transfermarkt — {len(tasks)} tâche(s) ===")
    for league, season in tasks:
        logger.info(f"  → {league} {season}")

    # Initialiser le driver (headless=False obligatoire pour contourner les anti-bots)
    driver = None
    if not args.dry_run:
        # headless=False : Transfermarkt détecte Selenium en headless
        driver = Driver(uc=True, headless=False)
        # Pré-charger la page d'accueil pour établir la session
        driver.uc_open_with_reconnect(BASE_URL, 5)
        human_delay(2, 3)
        accept_cookies(driver)

    # Compteurs
    stats = {"ok": 0, "skip": 0, "not_found": 0, "error": 0}

    try:
        for league, season in tasks:
            logger.info(f"\n  === {league} {season} ===")

            try:
                result = scrape_league_season(
                    driver, league, season, dry_run=args.dry_run
                )
            except Exception as e:
                logger.error(f"  ❌ Exception non gérée {league} {season} : {e}")
                result = "error"

            stats[result] += 1

            # Pause polie entre les requêtes pour éviter le ban
            if result != "skip":
                human_delay(3, 7)

    finally:
        if driver:
            driver.quit()

    logger.success(
        f"Scraping Transfermarkt terminé — "
        f"OK:{stats['ok']} SKIP:{stats['skip']} "
        f"NOT_FOUND:{stats['not_found']} ERR:{stats['error']}"
    )
    logger.info(f"  Fichiers sauvegardés dans : {OUT_DIR}")


if __name__ == "__main__":
    main()