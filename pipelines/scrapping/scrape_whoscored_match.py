"""
Scraper WhoScored — Indexation des URLs de Match Reports
=========================================================
Phase 1 : Collecte et stockage des URLs uniquement (pas de scraping HTML).
Phase 2 : Le scraping HTML est délégué à un script séparé qui lit is_scraped=False.

FONCTIONNALITÉS
────────────────
  ✅ Retry avec Exponential Backoff (3 tentatives : 2s, 4s, 8s)
  ✅ Suivi CSV : Ligue / Saison / Mois / Statut / Timestamp / URLs trouvées
  ✅ DuckDB : table silver.stg_whoscored_urls avec colonne is_scraped
  ✅ Séparation indexation (ce script) / scraping HTML (script suivant)
  ✅ Audit de couverture basé sur la dernière run par mois

USAGE
──────
  python pipelines/scrape_whoscored_match.py
  python pipelines/scrape_whoscored_match.py --league "Serie A" --season 2023-2024
  python pipelines/scrape_whoscored_match.py --dry-run
  python pipelines/scrape_whoscored_match.py --reset
  python pipelines/scrape_whoscored_match.py --audit
  python pipelines/scrape_whoscored_match.py --headless
"""

import re
import time
import random
import argparse
import threading
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from loguru import logger
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR     = Path(__file__).resolve().parent.parent.parent
CFG_PATH     = ROOT_DIR / "scraping_config.yaml"
MAIN_CFG     = ROOT_DIR / "config.yaml"
LOG_DIR      = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_whoscored_match.log",
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

DB_PATH      = ROOT_DIR / MAIN_CFG_DATA["paths"]["duckdb"]
TRACKING_CSV = ROOT_DIR / "logs" / "whoscored_indexing_tracking.csv"

# ── Constantes ────────────────────────────────────────────────────────────────

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
        "substage_seasons": {
            "2022-2023": "Serie A",  # saison problématique : substage requis
                            },
    },
    "La Liga": {
        "url":  "https://www.whoscored.com/regions/206/tournaments/4/spain-laliga",
        "slug": "ESP-La-Liga",
    }
}

WS_BASE = "https://www.whoscored.com"

# Mois d'une saison football août→mai
# (year_offset, month_num, abbr_EN)
SEASON_MONTHS = [
    (0, 8,  "Aug"),
    (0, 9,  "Sept"),
    (0, 10, "Oct"),
    (0, 11, "Nov"),
    (0, 12, "Dec"),
    (1, 1,  "Jan"),
    (1, 2,  "Feb"),
    (1, 3,  "Mar"),
    (1, 4,  "Apr"),
    (1, 5,  "May"),
    (1, 6,  "Jun"),
]

MAX_RETRIES  = 3
BACKOFF_BASE = 2   # délais : 2s, 4s, 8s

RE_STATS_BTN_ID = re.compile(r"statsBtn-(\d+)")

_db_lock  = threading.Lock()
_csv_lock = threading.Lock()


# ── Schéma DuckDB ─────────────────────────────────────────────────────────────

CREATE_URLS_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_urls (
    ws_match_id     VARCHAR  NOT NULL,
    url             VARCHAR  NOT NULL,
    league_source   VARCHAR  NOT NULL,
    season          VARCHAR  NOT NULL,
    month_abbr      VARCHAR,
    match_year      INTEGER,
    is_scraped      BOOLEAN  DEFAULT FALSE,
    indexed_at      VARCHAR,
    scraped_at      VARCHAR,
    skip_reason     VARCHAR,
    PRIMARY KEY (ws_match_id)
);
"""


def init_db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute(CREATE_URLS_TABLE)
    return conn


def upsert_urls(rows: list[dict]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    with _db_lock:
        conn = init_db()
        existing = set(
            r[0] for r in conn.execute(
                "SELECT ws_match_id FROM silver.stg_whoscored_urls"
            ).fetchall()
        )
        df_new = df[~df["ws_match_id"].isin(existing)]
        if df_new.empty:
            conn.close()
            return 0
        conn.register("df_new_urls", df_new)
        cols = ", ".join(df_new.columns.tolist())
        conn.execute(f"""
            INSERT INTO silver.stg_whoscored_urls ({cols})
            SELECT {cols} FROM df_new_urls
        """)
        n = len(df_new)
        conn.close()
    return n


def is_month_indexed(league: str, season: str, month_abbr: str) -> bool:
    """Retourne True si ce mois a déjà des URLs en base."""
    try:
        with _db_lock:
            conn = duckdb.connect(str(DB_PATH), read_only=True)
            n = conn.execute("""
                SELECT COUNT(*) FROM silver.stg_whoscored_urls
                WHERE league_source = ? AND season = ? AND month_abbr = ?
            """, [league, season, month_abbr]).fetchone()[0]
            conn.close()
        return n > 0
    except Exception:
        return False


# ── Suivi CSV ─────────────────────────────────────────────────────────────────

TRACKING_COLS = [
    "league", "season", "month_abbr", "year",
    "status", "urls_found", "timestamp", "error",
]


def init_tracking_csv():
    if not TRACKING_CSV.exists():
        pd.DataFrame(columns=TRACKING_COLS).to_csv(TRACKING_CSV, index=False)
        logger.info(f"  Fichier de suivi créé : {TRACKING_CSV}")


def append_tracking(row: dict):
    with _csv_lock:
        df_row = pd.DataFrame([{c: row.get(c, "") for c in TRACKING_COLS}])
        df_row.to_csv(TRACKING_CSV, mode="a", header=False, index=False)


def print_audit():
    if not TRACKING_CSV.exists():
        logger.warning("  Pas de fichier de suivi trouvé.")
        return

    df = pd.read_csv(TRACKING_CSV)

    # Garder uniquement la DERNIÈRE entrée par (league, season, month_abbr)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = (
        df.sort_values("timestamp")
          .groupby(["league", "season", "month_abbr"], as_index=False)
          .last()
    )

    logger.info("\n══════════════════════════════════════════")
    logger.info("  AUDIT — COUVERTURE INDEXATION WHOSCORED ")
    logger.info("══════════════════════════════════════════")

    for (league, season), grp in df.groupby(["league", "season"]):
        total     = len(grp)
        successes = (grp["status"] == "success").sum()
        failures  = (grp["status"] == "failure").sum()
        skipped   = (grp["status"] == "skipped").sum()
        urls      = grp["urls_found"].sum()
        icon      = "✅" if failures == 0 else "⚠️ "
        logger.info(
            f"  {icon} {league:<20} {season} | "
            f"{successes}/{total} mois OK | "
            f"{failures} échecs | "
            f"{int(urls)} URLs | "
            f"{skipped} skippés"
        )
        if failures > 0:
            failed = grp[grp["status"] == "failure"]["month_abbr"].tolist()
            logger.warning(f"      Mois en échec : {failed}")

    logger.info("──────────────────────────────────────────")
    logger.info(f"  Total URLs indexées : {int(df['urls_found'].sum())}")
    logger.info(f"  Mois en échec       : {(df['status'] == 'failure').sum()}")

    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        n_total   = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_urls"
        ).fetchone()[0]
        n_scraped = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_urls WHERE is_scraped = TRUE"
        ).fetchone()[0]
        conn.close()
        logger.info(
            f"  DuckDB : {n_total} URLs | "
            f"{n_scraped} scrapées | "
            f"{n_total - n_scraped} en attente"
        )
    except Exception:
        logger.warning("  DuckDB non accessible pour l'audit")


# ── Utilitaires Selenium ──────────────────────────────────────────────────────

def season_to_whoscored(season: str) -> str:
    return season.replace("-", "/")


def season_to_years(season: str) -> tuple[int, int]:
    parts = season.split("-")
    return int(parts[0]), int(parts[1])


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


def select_season(driver, season_text: str) -> bool:
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
        human_delay(1, 2)
        return True
    except Exception as e:
        logger.warning(f"  Impossible de sélectionner la saison {season_text} : {e}")
        return False


# ── Navigation vers le calendrier des fixtures ────────────────────────────────

def select_substage(driver, substage: str) -> bool:
    """
    Sélectionne un substage dans le menu #stages si présent.
    Utilisé pour les cas particuliers comme Serie A 2022-2023
    qui expose un menu de phase avant les fixtures.
    Retourne True si succès ou si le menu n'existe pas (non bloquant).
    """
    try:
        stage_select = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.ID, "stages"))
        )
        select = Select(stage_select)
        options = [o.text.strip() for o in select.options]
        logger.debug(f"  Stages disponibles : {options}")

        if substage not in options:
            logger.warning(
                f"  Substage '{substage}' absent des options {options} — skip"
            )
            return True  # non bloquant : on continue sans substage

        select.select_by_visible_text(substage)
        wait_for_loading(driver)
        human_delay(2, 3)
        logger.info(f"  Substage sélectionné : {substage}")
        return True

    except Exception:
        # Menu #stages absent = cas normal pour la plupart des saisons
        logger.debug("  Pas de menu #stages sur cette page — substage ignoré")
        return True
    
def navigate_to_fixtures(driver, league: str, season: str) -> bool:
    cfg       = LEAGUE_CONFIG[league]
    season_ws = season_to_whoscored(season)

    logger.info(f"  Navigation : {cfg['url']}")
    driver.uc_open_with_reconnect(cfg["url"], 5)
    human_delay(1,3)
    handle_cookies(driver)

    if not select_season(driver, season_ws):
        return False

    # ── Substage conditionnel (ex : Serie A 2022-2023) ────────────────────────
    substage_seasons = cfg.get("substage_seasons", {})
    substage = substage_seasons.get(season)
    if substage:
        logger.info(f"  Substage requis pour {league} {season} : '{substage}'")
        if not select_substage(driver, substage):
            logger.error(f"  Échec sélection substage '{substage}' — abandon")
            return False

    try:
        fixtures_tab = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Fixtures"))
        )
        safe_click(driver, fixtures_tab)
        wait_for_loading(driver)
        human_delay(2, 3)
        logger.debug("  Onglet Fixtures actif")
        return True
    except Exception as e:
        logger.error(f"  Onglet Fixtures introuvable : {e}")
        return False


# ── Sélection année et mois ───────────────────────────────────────────────────

def select_year(driver, year: int) -> bool:
    """
    Ouvre le panel via button#toggleCalendar,
    ouvre le sélecteur d'années via DatePicker-module_buttonOff,
    puis clique sur l'année dans yearsTbody.
    Le panel reste ouvert avec monthsTbody visible.
    """
    try:
        toggle = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "toggleCalendar"))
        )
        safe_click(driver, toggle)
        human_delay(1.0, 1.5)

        year_toggle = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[contains(@class, 'DatePicker-module_buttonOff')]"
            ))
        )
        safe_click(driver, year_toggle)
        human_delay(0.5, 1.0)

        year_option = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((
                By.XPATH,
                f"//tbody[contains(@class, 'DatePicker-module_yearsTbody')]"
                f"//td[normalize-space(text())='{year}']"
            ))
        )
        safe_click(driver, year_option)
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//tbody[contains(@class, 'DatePicker-module_monthsTbody')]"
                "//td[contains(@class, 'datePicker_selectable')]"
            ))
        )
        human_delay(0.5, 1.0)
        logger.debug(f"  Année {year} sélectionnée")
        return True

    except Exception as e:
        logger.warning(f"  Impossible de sélectionner l'année {year} : {e}")
        return False


def select_month_only(driver, month_abbr: str) -> bool:
    """
    Clique sur le mois dans monthsTbody.
    Le panel doit être déjà ouvert et l'année déjà sélectionnée.
    """
    try:
        month_elem = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((
                By.XPATH,
                f"//tbody[contains(@class, 'DatePicker-module_monthsTbody')]"
                f"//td[contains(@class, 'datePicker_selectable') "
                f"and normalize-space(text())='{month_abbr}']"
            ))
        )
        safe_click(driver, month_elem)
        wait_for_loading(driver)
        human_delay(2, 3)
        logger.debug(f"  Mois sélectionné : {month_abbr}")
        return True

    except Exception as e:
        logger.warning(f"  Échec clic mois {month_abbr} : {e}")
        return False


def select_month_with_retry(driver, month_abbr: str, year: int) -> bool:
    """
    Tente de cliquer sur le mois avec Exponential Backoff.
    En cas d'échec, rouvre le panel et resélectionne l'année avant de réessayer.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if select_month_only(driver, month_abbr):
                return True
            raise Exception("select_month_only returned False")

        except Exception as e:
            wait = BACKOFF_BASE ** attempt  # 2, 4, 8
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"  ⚠️  Tentative {attempt}/{MAX_RETRIES} — "
                    f"{month_abbr} {year} — retry dans {wait}s"
                )
                time.sleep(wait)
                # Rouvrir le panel et resélectionner l'année
                select_year(driver, year)
            else:
                logger.error(
                    f"  ❌ Échec définitif {month_abbr} {year} "
                    f"après {MAX_RETRIES} tentatives : {e}"
                )
    return False


# ── Extraction des URLs depuis la page courante ───────────────────────────────

def extract_match_urls_from_page(driver) -> dict[str, str]:
    """
    Extrait tous les <a id="statsBtn-{id}"> de la page courante.
    Retourne {ws_match_id: url_absolue}.
    """
    urls: dict[str, str] = {}
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[id^='statsBtn-']")
            )
        )
    except Exception:
        logger.debug("  Aucun statsBtn-* sur cette page (mois vide)")
        return urls

    btns = driver.find_elements(By.CSS_SELECTOR, "a[id^='statsBtn-']")
    logger.debug(f"  {len(btns)} statsBtn trouvés")

    for btn in btns:
        try:
            btn_id = btn.get_attribute("id") or ""
            href   = btn.get_attribute("href") or ""
            m = RE_STATS_BTN_ID.search(btn_id)
            if not m:
                continue
            ws_id = m.group(1)
            url = href if href.startswith("http") else WS_BASE + href
            if url:
                urls[ws_id] = url
        except Exception as e:
            logger.debug(f"  Erreur statsBtn : {e}")

    return urls


# ── Collecte complète d'une ligue × saison ────────────────────────────────────

def collect_and_index_league_season(
    driver,
    league: str,
    season: str,
    reset: bool = False,
    dry_run: bool = False,
    already_on_page: bool = False,
) -> dict:
    """
    Navigue mois par mois, collecte les URLs et les insère dans DuckDB.
    Groupe les mois par année pour minimiser les clics sur le sélecteur.

    already_on_page=True : on est déjà sur la page fixtures de cette ligue,
    il suffit de changer la saison via #seasons sans recharger la page de base.
    """
    year1, _ = season_to_years(season)
    summary  = {"total_urls": 0, "months_ok": 0, "months_failed": 0}

    if already_on_page:
        # Déjà sur la page de la ligue — changer uniquement la saison
        season_ws = season_to_whoscored(season)
        logger.info(f"  Changement de saison → {season_ws} (sans rechargement)")
        if not select_season(driver, season_ws):
            logger.error(f"  Impossible de changer la saison vers {season}")
            return summary
        # Re-cliquer sur Fixtures au cas où le changement de saison reset l'onglet
        try:
            fixtures_tab = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.LINK_TEXT, "Fixtures"))
            )
            safe_click(driver, fixtures_tab)
            wait_for_loading(driver)
            human_delay(2, 3)
        except Exception:
            pass  # déjà sur Fixtures, pas de problème
    else:
        if not navigate_to_fixtures(driver, league, season):
            logger.error(f"  Impossible d'accéder aux fixtures {league} {season}")
            return summary

    # Grouper les mois par année — UNE sélection d'année pour N mois
    months_by_year: dict[int, list[str]] = {}
    for year_offset, _, month_abbr in SEASON_MONTHS:
        year = year1 + year_offset
        months_by_year.setdefault(year, []).append(month_abbr)

    

    for year, months in months_by_year.items():

        # Vérifier si tous les mois de cette année sont déjà indexés
        all_skipped = not reset and all(
            is_month_indexed(league, season, m) for m in months
        )
        if all_skipped:
            logger.debug(f"  ⏩ Année {year} entièrement indexée — skip groupe")
            for month_abbr in months:
                append_tracking({
                    "league": league, "season": season,
                    "month_abbr": month_abbr, "year": year,
                    "status": "skipped", "urls_found": 0,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "error": "",
                })
            continue

        # Sélectionner l'année une seule fois pour tout le groupe
        logger.info(f"  Sélection de l'année {year}...")
        if not select_year(driver, year):
            for month_abbr in months:
                summary["months_failed"] += 1
                append_tracking({
                    "league": league, "season": season,
                    "month_abbr": month_abbr, "year": year,
                    "status": "failure", "urls_found": 0,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "error": f"select_year({year}) failed",
                })
            continue

        for month_abbr in months:

            # Skip si déjà indexé
            if not reset and is_month_indexed(league, season, month_abbr):
                logger.debug(f"  ⏩ {month_abbr} {year} déjà indexé — skip")
                append_tracking({
                    "league": league, "season": season,
                    "month_abbr": month_abbr, "year": year,
                    "status": "skipped", "urls_found": 0,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "error": "",
                })
                continue

            logger.info(f"  Mois : {month_abbr} {year}")
            ok = select_month_with_retry(driver, month_abbr, year)

            if not ok:
                summary["months_failed"] += 1
                append_tracking({
                    "league": league, "season": season,
                    "month_abbr": month_abbr, "year": year,
                    "status": "failure", "urls_found": 0,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "error": f"select_month_with_retry failed after {MAX_RETRIES}",
                })
                # Rouvrir le panel pour le mois suivant
                select_year(driver, year)
                continue

            # Extraction des URLs
            month_urls = extract_match_urls_from_page(driver)
            n_found    = len(month_urls)
            logger.info(f"    → {n_found} URL(s) trouvée(s)")

            if not dry_run and month_urls:
                rows = [
                    {
                        "ws_match_id":   ws_id,
                        "url":           url,
                        "league_source": league,
                        "season":        season,
                        "month_abbr":    month_abbr,
                        "match_year":    year,
                        "is_scraped":    False,
                        "indexed_at":    datetime.now().isoformat(timespec="seconds"),
                        "scraped_at":    None,
                        "skip_reason":   None,
                    }
                    for ws_id, url in month_urls.items()
                ]
                n_inserted = upsert_urls(rows)
                logger.info(f"    → {n_inserted} nouvelles URLs insérées en base")

            summary["months_ok"]  += 1
            summary["total_urls"] += n_found

            append_tracking({
                "league": league, "season": season,
                "month_abbr": month_abbr, "year": year,
                "status": "success", "urls_found": n_found,
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "error": "",
            })

            # Rouvrir le panel pour le mois suivant si nécessaire
            current_idx      = months.index(month_abbr)
            remaining        = months[current_idx + 1:]
            next_needs_click = any(
                not (not reset and is_month_indexed(league, season, m))
                for m in remaining
            )
            if remaining and next_needs_click:
                select_year(driver, year)

            human_delay(1, 2)

    return summary


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="WhoScored — Indexation des URLs de Match Reports"
    )
    parser.add_argument("--league",   default=None,
                        help="Ligue (ex: 'Serie A')")
    parser.add_argument("--season",   default=None,
                        help="Saison (ex: 2023-2024)")
    parser.add_argument("--reset",    action="store_true",
                        help="Réindexer même les mois déjà en base")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Collecter sans écrire en base")
    parser.add_argument("--headless", action="store_true",
                        help="Chrome sans interface graphique")
    parser.add_argument("--audit",    action="store_true",
                        help="Afficher le rapport de couverture et quitter")
    args = parser.parse_args()

    if args.audit:
        print_audit()
        return

    # Initialisation
    init_db()
    init_tracking_csv()

    # Construction de la liste de tâches
    seasons_cfg = set(SCRAP_CFG.get("seasons", []))
    leagues_cfg = set(SCRAP_CFG.get("leagues", []))

    if args.league:
        leagues_cfg = {args.league}
    if args.season:
        seasons_cfg = {args.season}

    leagues_cfg = {l for l in leagues_cfg if l in LEAGUE_CONFIG}

    tasks = sorted(
        [(league, season) for league in leagues_cfg for season in seasons_cfg]
    )

    if not tasks:
        logger.warning("  Aucune tâche. Vérifie scraping_config.yaml.")
        return

    logger.info("=== WhoScored — Indexation des URLs ===")
    logger.info(f"  {len(tasks)} tâche(s) à traiter")
    logger.info(f"  Tâches : {tasks}")

    driver = Driver(uc=True, headless=args.headless)
    total  = {"total_urls": 0, "months_ok": 0, "months_failed": 0}

    # Grouper les tâches par ligue pour éviter les rechargements inutiles
    # ex: [('Ligue 1', '2021'), ('Ligue 1', '2022'), ('Serie A', '2021')]
    # → Ligue 1 : navigate une fois, puis change saison
    # → Serie A  : navigate une fois (nouvelle ligue)
    from itertools import groupby
    tasks_by_league = {
        league: [season for _, season in group]
        for league, group in groupby(tasks, key=lambda t: t[0])
    }

    try:
        for league, seasons in tasks_by_league.items():
            logger.info(f"\n{'='*50}")
            logger.info(f"  LIGUE : {league} ({len(seasons)} saison(s))")

            for i, season in enumerate(sorted(seasons)):
                logger.info(f"\n=== {league} {season} ===")

                # Première saison de la ligue → navigation complète
                # Saisons suivantes → changement de saison uniquement
                already_on_page = (i > 0)

                result = collect_and_index_league_season(
                    driver, league, season,
                    reset=args.reset,
                    dry_run=args.dry_run,
                    already_on_page=already_on_page,
                )
                for k in total:
                    total[k] += result[k]

                # Pause courte entre saisons de la même ligue
                # Pause longue entre ligues (gérée après la boucle interne)
                if i < len(seasons) - 1:
                    human_delay(5, 10)

            # Pause longue entre ligues
            human_delay(5, 10)

    finally:
        driver.quit()

    logger.success(
        f"=== Indexation terminée — "
        f"{total['total_urls']} URLs | "
        f"{total['months_ok']} mois OK | "
        f"{total['months_failed']} mois en échec ==="
    )
    logger.info(f"  Suivi CSV : {TRACKING_CSV}")
    print_audit()


if __name__ == "__main__":
    main()
