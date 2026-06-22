"""
Scraper WhoScored — Extraction des données atomiques de Match Report
====================================================================
Phase 2 du pipeline de collecte WhoScored.
Lit les URLs non scrapées depuis silver.stg_whoscored_urls (is_scraped=False)
et extrait les données tactiques depuis chaque Match Report.

ARCHITECTURE D'EXTRACTION
──────────────────────────
La stratégie principale repose sur l'objet JS `allStats` injecté par WhoScored
dans un <script> tag du source HTML. Cet objet contient TOUTES les données
du match (Situation, Positional, Incidents) en JSON structuré.

  Avantages vs scraping CSS/Canvas :
    ✅ Données brutes non arrondies (ex: 33.7% au lieu de 34%)
    ✅ Résistant aux changements de layout CSS
    ✅ Fonctionne en headless sans rendu Canvas
    ✅ Extraction en une seule passe (pas de clics sur les onglets)

  Fallback : si allStats est absent, scraping DOM sur les containers
  #live-goals, #live-passes, #live-aggression, #live-pitch-stats.

DONNÉES EXTRAITES
──────────────────
  Situation Report :
    - Attempt Types  : total shots, open play, set piece, counter attack
    - Pass Types     : through balls, long balls, key passes
    - Card Situations: yellow cards, red cards (home + away)

  Positional Report :
    - Attack Sides   : left %, center %, right % (home + away)
    - Action Zones   : defensive third %, middle third %, offensive third %
    - Shot Zones     : (optionnel — si disponible dans allStats)

  Context :
    - Score mi-temps (home / away)
    - Arbitre

OUTPUT
───────
  silver.stg_whoscored_match_details (upsert sur ws_match_id)
  Mise à jour de is_scraped=TRUE + scraped_at dans stg_whoscored_urls

USAGE
──────
  python pipelines/scrape_whoscored_details.py
  python pipelines/scrape_whoscored_details.py --limit 50
  python pipelines/scrape_whoscored_details.py --ws-id 1901082
  python pipelines/scrape_whoscored_details.py --headless
  python pipelines/scrape_whoscored_details.py --dry-run
"""

import json
import re
import time
import random
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import yaml
from loguru import logger
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
MAIN_CFG = ROOT_DIR / "config.yaml"
LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "scrape_whoscored_details.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="10 MB",
    retention=5,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

with open(MAIN_CFG, encoding="utf-8") as f:
    MAIN_CFG_DATA = yaml.safe_load(f)

DB_PATH      = ROOT_DIR / MAIN_CFG_DATA["paths"]["duckdb"]
TRACKING_CSV = ROOT_DIR / "logs" / "whoscored_details_tracking.csv"

WS_BASE      = "https://www.whoscored.com"
MAX_RETRIES  = 3
BACKOFF_BASE = 2   # délais retry : 2s, 4s, 8s

# Rotation User-Agents (même logique que le scraper d'indexation)
USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4.1 Safari/605.1.15"
    ),
]

# Regex pour extraire allStats depuis le source JS
# WhoScored injecte : var allStats = {...};
RE_ALL_STATS = re.compile(
    r"var\s+allStats\s*=\s*(\{.*?\});",
    re.DOTALL,
)

# Regex fallback pour matchCentreData (ancienne structure WhoScored)
RE_MATCH_CENTRE = re.compile(
    r"matchCentreData\s*[=:]\s*(\{.*?\})\s*,\s*\n\s*matchCentreEventTypeJson",
    re.DOTALL,
)

# Colonnes du CSV de suivi
TRACKING_COLS = [
    "ws_match_id", "league_source", "season",
    "status", "method",  # "allStats" | "dom_fallback" | "failed"
    "timestamp", "error",
]


# ── Schéma DuckDB ─────────────────────────────────────────────────────────────

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_events (
    -- ── Clés ──────────────────────────────────────────────────────────────
    ws_match_id       VARCHAR  NOT NULL,
    event_id          INTEGER  NOT NULL,
    league_source     VARCHAR,
    season            VARCHAR,

    -- ── Contexte temporel ─────────────────────────────────────────────────
    minute            INTEGER,
    second            INTEGER,
    expanded_minute   INTEGER,
    period            INTEGER,   -- 1=FirstHalf 2=SecondHalf 5=ExtraTime...

    -- ── Acteurs ───────────────────────────────────────────────────────────
    team_id           INTEGER,
    player_id         INTEGER,

    -- ── Position ──────────────────────────────────────────────────────────
    x                 DOUBLE,
    y                 DOUBLE,
    end_x             DOUBLE,
    end_y             DOUBLE,

    -- ── Type d'action ─────────────────────────────────────────────────────
    type_id           INTEGER,   -- ex: 13=MissedShot, 16=SavedShot, 10=Goal
    type_name         VARCHAR,
    outcome_id        INTEGER,   -- 1=Successful, 0=Unsuccessful
    outcome_name      VARCHAR,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_touch          BOOLEAN,
    is_shot           BOOLEAN,

    -- ── Qualifiers bruts (JSON) — rien ne se perd ─────────────────────────
    qualifiers_json   VARCHAR,

    -- ── Méta ──────────────────────────────────────────────────────────────
    scraped_at        VARCHAR,
    row_num           INTEGER  NOT NULL,


    PRIMARY KEY (ws_match_id, row_num)
);
"""

CREATE_MATCH_INDEX_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_match_index (
    ws_match_id     VARCHAR NOT NULL PRIMARY KEY,
    match_date      DATE,
    home_team_id    INTEGER,
    home_team_name  VARCHAR,
    away_team_id    INTEGER,
    away_team_name  VARCHAR,
    league_source   VARCHAR,
    season          VARCHAR,
    scraped_at      VARCHAR
);
"""
# ── DuckDB ────────────────────────────────────────────────────────────────────

def init_db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute(CREATE_MATCH_INDEX_TABLE)
    conn.execute(CREATE_EVENTS_TABLE)
    return conn


def load_pending_urls(limit: Optional[int] = None) -> list[dict]:
    """
    Charge les URLs non encore scrapées depuis silver.stg_whoscored_urls.
    """
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        limit_clause = f"LIMIT {limit}" if limit else ""
        rows = conn.execute(f"""
            SELECT ws_match_id, url, league_source, season
            FROM silver.stg_whoscored_urls
            WHERE is_scraped = FALSE
            ORDER BY season, ws_match_id
            {limit_clause}
        """).fetchall()
        conn.close()
        return [
            {"ws_match_id": r[0], "url": r[1],
             "league_source": r[2], "season": r[3]}
            for r in rows
        ]
    except Exception as e:
        logger.error(f"  Impossible de charger les URLs : {e}")
        return []


def upsert_events(events: list[dict]) -> bool:
    """
    Upsert des events d'un match dans stg_whoscored_events.
    Stratégie : DELETE tous les events du match + INSERT les nouveaux.
    Idempotent : re-scraper un match remplace proprement ses events.
    """
    if not events:
        return False
    try:
        conn = init_db()
        ws_id = events[0]["ws_match_id"]

        # Supprimer les events existants pour ce match
        conn.execute(
            "DELETE FROM silver.stg_whoscored_events WHERE ws_match_id = ?",
            [ws_id]
        )

        df = pd.DataFrame(events)

        # Cast types
        int_cols   = ["row_num","event_id", "minute", "second", "expanded_minute",
                      "period", "team_id", "player_id", "type_id", "outcome_id"]
        float_cols = ["x", "y", "end_x", "end_y"]
        bool_cols  = ["is_touch", "is_shot"]

        for c in int_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        for c in float_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in bool_cols:
            if c in df.columns:
                df[c] = df[c].astype("boolean")
        df = df.astype({"season": str, "league_source": str, 
                        "ws_match_id": str, "type_name": str,
                        "outcome_name": str, "qualifiers_json": str,
                        "scraped_at": str})

        cols = ", ".join(df.columns.tolist())
        conn.register("df_events", df)
        conn.execute(
            f"INSERT INTO silver.stg_whoscored_events ({cols}) SELECT {cols} FROM df_events"
        )
        n = len(df)
        conn.close()
        logger.debug(f"  {n} events insérés pour {ws_id}")
        return True
    except Exception as e:
        logger.error(f"  Erreur upsert events {events[0].get('ws_match_id')} : {e}")
        return False

def upsert_match_index(row: dict) -> bool:
    try:
        conn = init_db()
        conn.execute(
            "DELETE FROM silver.stg_whoscored_match_index WHERE ws_match_id = ?",
            [row["ws_match_id"]]
        )
        df = pd.DataFrame([row])
        conn.register("df_idx", df)
        conn.execute(
            "INSERT INTO silver.stg_whoscored_match_index SELECT * FROM df_idx"
        )
        conn.close()
        return True
    except Exception as e:
        logger.error(f"  Erreur upsert match_index {row.get('ws_match_id')} : {e}")
        return False
    

def mark_scraped(ws_match_id: str):
    """Met à jour is_scraped=TRUE dans stg_whoscored_urls."""
    try:
        conn = duckdb.connect(str(DB_PATH))
        conn.execute("""
            UPDATE silver.stg_whoscored_urls
            SET is_scraped = TRUE,
                scraped_at = ?
            WHERE ws_match_id = ?
        """, [datetime.now().isoformat(timespec="seconds"), ws_match_id])
        conn.close()
    except Exception as e:
        logger.warning(f"  Impossible de marquer {ws_match_id} comme scrapé : {e}")


def mark_paywall(ws_match_id: str):
    """
    Marque un match comme bloqué par le paywall WhoScored+.
    - is_scraped = TRUE  → exclut du prochain load_pending_urls()
    - skip_reason = 'paywall' → permet de les identifier / réessayer plus tard

    Ajoute la colonne skip_reason si elle n'existe pas encore (migration auto).
    """
    try:
        conn = duckdb.connect(str(DB_PATH))
        # Migration auto : ajouter skip_reason si absente
        cols = [r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='silver' AND table_name='stg_whoscored_urls'"
        ).fetchall()]
        if "skip_reason" not in cols:
            conn.execute(
                "ALTER TABLE silver.stg_whoscored_urls ADD COLUMN skip_reason VARCHAR"
            )
            logger.debug("  Colonne skip_reason ajoutée à stg_whoscored_urls")

        conn.execute("""
            UPDATE silver.stg_whoscored_urls
            SET is_scraped  = TRUE,
                scraped_at  = ?,
                skip_reason = 'paywall'
            WHERE ws_match_id = ?
        """, [datetime.now().isoformat(timespec="seconds"), ws_match_id])
        conn.close()
        logger.info(f"  📌 Match {ws_match_id} marqué paywall dans stg_whoscored_urls")
    except Exception as e:
        logger.warning(f"  Impossible de marquer {ws_match_id} comme paywall : {e}")

def mark_no_data(ws_match_id: str):
    """
    Marque un match comme sans données (matchCentreData=null).
    - is_scraped = TRUE  → exclut du prochain load_pending_urls()
    - skip_reason = 'no_data' → identifiable pour stats / audit
    """
    try:
        conn = duckdb.connect(str(DB_PATH))
        cols = [r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='silver' AND table_name='stg_whoscored_urls'"
        ).fetchall()]
        if "skip_reason" not in cols:
            conn.execute(
                "ALTER TABLE silver.stg_whoscored_urls ADD COLUMN skip_reason VARCHAR"
            )
        conn.execute("""
            UPDATE silver.stg_whoscored_urls
            SET is_scraped  = TRUE,
                scraped_at  = ?,
                skip_reason = 'no_data'
            WHERE ws_match_id = ?
        """, [datetime.now().isoformat(timespec="seconds"), ws_match_id])
        conn.close()
        logger.info(f"  📭 Match {ws_match_id} marqué no_data dans stg_whoscored_urls")
    except Exception as e:
        logger.warning(f"  Impossible de marquer {ws_match_id} comme no_data : {e}")
# ── Tracking CSV ──────────────────────────────────────────────────────────────

def init_tracking_csv():
    if not TRACKING_CSV.exists():
        pd.DataFrame(columns=TRACKING_COLS).to_csv(TRACKING_CSV, index=False)


def append_tracking(row: dict):
    df_row = pd.DataFrame([{c: row.get(c, "") for c in TRACKING_COLS}])
    df_row.to_csv(TRACKING_CSV, mode="a", header=False, index=False)


# ── Utilitaires Selenium ──────────────────────────────────────────────────────

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


def wait_container_visible(driver, css_selector: str, timeout: int = 10) -> bool:
    """
    Attend qu'un container soit présent ET visible (display != none).
    Retourne True si trouvé, False si timeout.
    """
    try:
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector))
        )
        return True
    except Exception:
        return False


def handle_cookies(driver):
    time.sleep(3)
    try:
        btns = driver.find_elements(
            By.XPATH,
            "//button[contains(., 'Tout accepter') or contains(., 'Accept All')]"
        )
        if btns:
            safe_click(driver, btns[0])
            logger.debug("  🍪 Cookies acceptés")
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
                driver.switch_to.default_content()
                return
            driver.switch_to.default_content()
        except Exception:
            driver.switch_to.default_content()


# ══════════════════════════════════════════════════════════════════════════════
# EXTRACTION PRINCIPALE — allStats JS
# ══════════════════════════════════════════════════════════════════════════════

def extract_all_stats_from_source(page_source: str) -> Optional[dict]:
    """
    Extrait l'objet JS allStats depuis le source HTML.
    C'est la méthode principale — fiable, précise, headless-compatible.

    WhoScored injecte dans le HTML :
        var allStats = { "home": {...}, "away": {...} };

    Structure attendue (peut varier selon la version WhoScored) :
        allStats.home.stats.attemptTypes.{openPlay, setpiece, counter}
        allStats.home.stats.passingStats.{keyPasses, throughBalls, longBalls}
        allStats.home.stats.aggression.{yellowCard, redCard}
        allStats.home.stats.attackZones.{left, center, right}          ← Attack Sides
        allStats.home.stats.fieldZones.{ownHalf, midfield, opposition} ← Action Zones
    """
    # Tentative 1 : allStats
    match = RE_ALL_STATS.search(page_source)
    if match:
        try:
            data = json.loads(match.group(1))
            logger.debug("  allStats extrait avec succès")
            return {"source": "allStats", "data": data}
        except json.JSONDecodeError as e:
            logger.debug(f"  allStats JSON invalide : {e}")

    # Tentative 2 : matchCentreData (structure alternative WhoScored)
    match = RE_MATCH_CENTRE.search(page_source)
    if match:
        try:
            data = json.loads(match.group(1))
            logger.debug("  matchCentreData extrait en fallback")
            return {"source": "matchCentreData", "data": data}
        except json.JSONDecodeError as e:
            logger.debug(f"  matchCentreData JSON invalide : {e}")

    # Tentative 3 : chercher tout objet JSON contenant "attemptTypes"
    # WhoScored peut renommer la variable selon les versions
    pattern_generic = re.compile(
        r'(?:var\s+\w+|window\.\w+)\s*=\s*(\{[^<]{100,}\});',
        re.DOTALL,
    )
    for m in pattern_generic.finditer(page_source):
        try:
            candidate = json.loads(m.group(1))
            if "home" in candidate and "away" in candidate:
                logger.debug("  JSON générique home/away trouvé")
                return {"source": "generic_json", "data": candidate}
        except Exception:
            continue

    logger.warning("  Aucun objet JS de stats trouvé dans le source")
    return None


def parse_allstats(payload: dict) -> dict:
    """
    Parse l'objet allStats (ou matchCentreData) et retourne un dict plat
    aligné sur le schéma stg_whoscored_match_details.

    La structure interne de WhoScored varie selon les saisons.
    On utilise .get() partout avec None comme défaut — aucune exception levée.
    """
    source = payload.get("source", "allStats")
    data   = payload.get("data", {})
    result = {}

    # ── Normalisation de la structure ────────────────────────────────────────
    # WhoScored peut organiser les données de plusieurs façons selon la version
    # Structure v1 : data["home"] / data["away"] avec sous-clés stats
    # Structure v2 : data["homeTeam"] / data["awayTeam"]
    home_raw = (
        data.get("home") or
        data.get("homeTeam") or
        data.get("home_team") or
        {}
    )
    away_raw = (
        data.get("away") or
        data.get("awayTeam") or
        data.get("away_team") or
        {}
    )

    # Les stats peuvent être imbriquées sous une clé "stats" ou directement
    home = home_raw.get("stats", home_raw)
    away = away_raw.get("stats", away_raw)

    # ── Attempt Types ────────────────────────────────────────────────────────
    def get_attempts(side: dict) -> dict:
        at = (
            side.get("attemptTypes") or
            side.get("attempt_types") or
            side.get("shotTypes") or
            {}
        )
        return {
            "total":       _safe_int(side.get("totalAttempts") or side.get("shots") or at.get("total")),
            "on_target":   _safe_int(side.get("onTarget") or at.get("onTarget") or at.get("on_target")),
            "open_play":   _safe_int(at.get("openPlay") or at.get("open_play")),
            "set_piece":   _safe_int(at.get("setpiece") or at.get("set_piece") or at.get("setPiece")),
            "counter":     _safe_int(at.get("counter") or at.get("counterAttack")),
        }

    h_att = get_attempts(home)
    a_att = get_attempts(away)

    result["home_shots_total"]     = h_att["total"]
    result["away_shots_total"]     = a_att["total"]
    result["home_shots_on_target"] = h_att["on_target"]
    result["away_shots_on_target"] = a_att["on_target"]
    result["home_shot_open_play"]  = h_att["open_play"]
    result["away_shot_open_play"]  = a_att["open_play"]
    result["home_shot_set_piece"]  = h_att["set_piece"]
    result["away_shot_set_piece"]  = a_att["set_piece"]
    result["home_shot_counter"]    = h_att["counter"]
    result["away_shot_counter"]    = a_att["counter"]

    # ── Pass Types ───────────────────────────────────────────────────────────
    def get_passes(side: dict) -> dict:
        ps = (
            side.get("passingStats") or
            side.get("passing_stats") or
            side.get("passTypes") or
            {}
        )
        return {
            "key_passes":    _safe_int(ps.get("keyPasses") or ps.get("key_passes")),
            "through_balls": _safe_int(ps.get("throughBalls") or ps.get("through_balls")),
            "long_balls":    _safe_int(ps.get("longBalls") or ps.get("long_balls")),
        }

    h_pass = get_passes(home)
    a_pass = get_passes(away)

    result["home_key_passes"]    = h_pass["key_passes"]
    result["away_key_passes"]    = a_pass["key_passes"]
    result["home_through_balls"] = h_pass["through_balls"]
    result["away_through_balls"] = a_pass["through_balls"]
    result["home_long_balls"]    = h_pass["long_balls"]
    result["away_long_balls"]    = a_pass["long_balls"]

    # ── Card Situations ───────────────────────────────────────────────────────
    def get_cards(side: dict) -> dict:
        ag = (
            side.get("aggression") or
            side.get("cards") or
            side.get("cardSituations") or
            {}
        )
        return {
            "yellow": _safe_int(ag.get("yellowCard") or ag.get("yellow") or ag.get("yellowCards")),
            "red":    _safe_int(ag.get("redCard")    or ag.get("red")    or ag.get("redCards")),
        }

    h_cards = get_cards(home)
    a_cards = get_cards(away)

    result["home_yellow_cards"] = h_cards["yellow"]
    result["away_yellow_cards"] = a_cards["yellow"]
    result["home_red_cards"]    = h_cards["red"]
    result["away_red_cards"]    = a_cards["red"]

    # ── Attack Sides (%) ─────────────────────────────────────────────────────
    def get_attack_sides(side: dict) -> dict:
        az = (
            side.get("attackZones") or
            side.get("attack_zones") or
            side.get("attackSides") or
            side.get("attack_sides") or
            {}
        )
        return {
            "left":   _safe_float(az.get("left")   or az.get("Left")),
            "center": _safe_float(az.get("center") or az.get("Centre") or az.get("Center")),
            "right":  _safe_float(az.get("right")  or az.get("Right")),
        }

    h_as = get_attack_sides(home)
    a_as = get_attack_sides(away)

    result["home_attack_left"]   = h_as["left"]
    result["home_attack_center"] = h_as["center"]
    result["home_attack_right"]  = h_as["right"]
    result["away_attack_left"]   = a_as["left"]
    result["away_attack_center"] = a_as["center"]
    result["away_attack_right"]  = a_as["right"]

    # ── Action Zones (%) — LEAD ARCHITECT SOLUTION ────────────────────────────
    # WhoScored encode les zones d'action dans fieldZones ou territorialSummary.
    # Les clés varient : ownHalf/midfield/opposition OU def/mid/att OU thirds.
    # On tente plusieurs clés connues dans l'ordre de fiabilité.
    def get_action_zones(side: dict) -> dict:
        fz = (
            side.get("fieldZones") or
            side.get("field_zones") or
            side.get("territorialSummary") or
            side.get("territorial") or
            side.get("actionZones") or
            side.get("thirds") or
            {}
        )
        # Mapping des clés connues → defensive / middle / offensive
        def_val = (
            fz.get("ownHalf") or fz.get("own_half") or
            fz.get("defensive") or fz.get("def") or
            fz.get("defensiveThird") or fz.get("Defensive")
        )
        mid_val = (
            fz.get("midfield") or fz.get("middle") or
            fz.get("mid") or fz.get("middleThird") or
            fz.get("Middle")
        )
        att_val = (
            fz.get("opposition") or fz.get("offensive") or
            fz.get("att") or fz.get("attackingThird") or
            fz.get("Offensive") or fz.get("Attack")
        )
        return {
            "defensive": _safe_float(def_val),
            "middle":    _safe_float(mid_val),
            "offensive": _safe_float(att_val),
        }

    h_az = get_action_zones(home)
    a_az = get_action_zones(away)

    result["home_zone_defensive"] = h_az["defensive"]
    result["home_zone_middle"]    = h_az["middle"]
    result["home_zone_offensive"] = h_az["offensive"]
    result["away_zone_defensive"] = a_az["defensive"]
    result["away_zone_middle"]    = a_az["middle"]
    result["away_zone_offensive"] = a_az["offensive"]

    # ── Context : Score mi-temps ──────────────────────────────────────────────
    ht = (
        data.get("halftimeScore") or
        data.get("htScore") or
        data.get("halfTimeScore") or
        {}
    )
    if isinstance(ht, dict):
        result["ht_score_home"] = _safe_int(ht.get("home") or ht.get("Home"))
        result["ht_score_away"] = _safe_int(ht.get("away") or ht.get("Away"))
    elif isinstance(ht, str) and "-" in ht:
        parts = ht.split("-")
        result["ht_score_home"] = _safe_int(parts[0].strip())
        result["ht_score_away"] = _safe_int(parts[1].strip())
    else:
        result["ht_score_home"] = None
        result["ht_score_away"] = None

    # ── Context : Arbitre ─────────────────────────────────────────────────────
    result["referee"] = (
        data.get("referee") or
        data.get("Referee") or
        data.get("refereeId")  # parfois c'est l'ID, pas le nom
    )

    result["extraction_method"] = source
    return result


# ── Helpers de conversion ─────────────────────────────────────────────────────

def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(float(str(val).replace("%", "").strip()))
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(str(val).replace("%", "").strip())
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# FALLBACK — Scraping DOM
# ══════════════════════════════════════════════════════════════════════════════

def click_match_report_tab(driver) -> bool:
    """
    Navigue vers l'onglet Match Report depuis la page Live/Preview du match.
    Cible : <a href*="matchreport">
    """
    try:
        report_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//a[contains(@href, 'matchreport') or "
                "contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                "'abcdefghijklmnopqrstuvwxyz'), 'match report')]"
            ))
        )
        safe_click(driver, report_link)
        wait_for_loading(driver)
        human_delay(2, 3)
        logger.debug("  Onglet Match Report cliqué")
        return True
    except Exception as e:
        logger.warning(f"  Onglet Match Report introuvable : {e}")
        return False


def click_tab_with_retry(driver, tab_text: str, container_css: str) -> bool:
    """
    Clique sur un sous-onglet et attend que son container soit visible.
    Retry avec Exponential Backoff si le container reste caché.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            tab = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    f"//a[normalize-space(text())='{tab_text}'] | "
                    f"//li[normalize-space(text())='{tab_text}'] | "
                    f"//span[normalize-space(text())='{tab_text}']/parent::a"
                ))
            )
            safe_click(driver, tab)
            if wait_container_visible(driver, container_css, timeout=8):
                logger.debug(f"  Tab '{tab_text}' → container visible")
                return True
            raise Exception(f"Container {container_css} non visible après clic")

        except Exception as e:
            wait = BACKOFF_BASE ** attempt
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"  ⚠️  Tab '{tab_text}' tentative {attempt}/{MAX_RETRIES} "
                    f"— retry dans {wait}s : {e}"
                )
                time.sleep(wait)
            else:
                logger.error(f"  ❌ Tab '{tab_text}' échec définitif : {e}")
    return False


def scrape_pulsable_values(driver, container_id: str) -> dict[str, list]:
    """
    Extrait les valeurs span.pulsable d'un container donné.
    Retourne {"home": [v1, v2, ...], "away": [v1, v2, ...]}
    basé sur la position dans le DOM (home = gauche, away = droite).
    """
    result = {"home": [], "away": []}
    try:
        container = driver.find_element(By.ID, container_id)
        # WhoScored structure : home values à gauche, away à droite
        # Les span.pulsable sont organisés en paires dans des .stat-item
        stat_items = container.find_elements(
            By.XPATH, ".//div[contains(@class,'stat') or contains(@class,'Stat')]"
        )
        for item in stat_items:
            values = item.find_elements(By.CSS_SELECTOR, "span.pulsable")
            if len(values) >= 2:
                result["home"].append(_safe_int(values[0].text))
                result["away"].append(_safe_int(values[1].text))
            elif len(values) == 1:
                result["home"].append(_safe_int(values[0].text))
    except Exception as e:
        logger.debug(f"  scrape_pulsable [{container_id}] : {e}")
    return result


def scrape_positional_from_dom(driver) -> dict:
    """
    Fallback pour les données positionnelles quand allStats est absent.
    Tente d'extraire depuis #live-pitch-stats via JavaScript execute.
    """
    result = {}
    try:
        # Tenter d'extraire depuis l'attribut data-highcharts-chart
        # WhoScored stocke parfois les données Highcharts dans cet attribut
        charts = driver.find_elements(
            By.CSS_SELECTOR,
            "[data-highcharts-chart]"
        )
        for chart in charts:
            chart_id = chart.get_attribute("data-highcharts-chart")
            try:
                # Extraire les données Highcharts via JS
                chart_data = driver.execute_script(
                    f"return Highcharts.charts[{chart_id}] ? "
                    f"JSON.stringify(Highcharts.charts[{chart_id}].series.map("
                    f"s => ({{name: s.name, data: s.data.map(d => d.y)}}))) : null;"
                )
                if chart_data:
                    series = json.loads(chart_data)
                    logger.debug(f"  Highcharts chart {chart_id} : {len(series)} series")
                    # Parser selon le type de chart (Attack Sides vs Action Zones)
                    for s in series:
                        name = (s.get("name") or "").lower()
                        data = s.get("data") or []
                        if "left" in name and data:
                            result[f"attack_left"] = _safe_float(data[0])
                        elif "right" in name and data:
                            result[f"attack_right"] = _safe_float(data[0])
                        elif "center" in name or "middle" in name and data:
                            result[f"attack_center"] = _safe_float(data[0])
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"  scrape_positional_from_dom : {e}")

    return result


def scrape_dom_fallback(driver) -> dict:
    """
    Fallback complet via scraping DOM quand allStats est absent.
    Navigue vers Match Report et scrape chaque section.
    Retourne un dict partiel (certaines valeurs peuvent être None).
    """
    result = {"extraction_method": "dom_fallback"}

    if not click_match_report_tab(driver):
        return result

    # ── Situation Report ──────────────────────────────────────────────────────

    # Attempt Types
    if click_tab_with_retry(driver, "Attempt Types", "#live-goals"):
        vals = scrape_pulsable_values(driver, "live-goals")
        # Mapping positionnel : [total, open_play, set_piece, counter, ...]
        for i, (h_key, a_key) in enumerate([
            ("home_shots_total",    "away_shots_total"),
            ("home_shot_open_play", "away_shot_open_play"),
            ("home_shot_set_piece", "away_shot_set_piece"),
            ("home_shot_counter",   "away_shot_counter"),
        ]):
            if i < len(vals["home"]):
                result[h_key] = vals["home"][i]
                result[a_key] = vals["away"][i] if i < len(vals["away"]) else None

    # Pass Types
    if click_tab_with_retry(driver, "Pass Types", "#live-passes"):
        vals = scrape_pulsable_values(driver, "live-passes")
        for i, (h_key, a_key) in enumerate([
            ("home_key_passes",    "away_key_passes"),
            ("home_through_balls", "away_through_balls"),
            ("home_long_balls",    "away_long_balls"),
        ]):
            if i < len(vals["home"]):
                result[h_key] = vals["home"][i]
                result[a_key] = vals["away"][i] if i < len(vals["away"]) else None

    # Card Situations
    if click_tab_with_retry(driver, "Card Situations", "#live-aggression"):
        vals = scrape_pulsable_values(driver, "live-aggression")
        for i, (h_key, a_key) in enumerate([
            ("home_yellow_cards", "away_yellow_cards"),
            ("home_red_cards",    "away_red_cards"),
        ]):
            if i < len(vals["home"]):
                result[h_key] = vals["home"][i]
                result[a_key] = vals["away"][i] if i < len(vals["away"]) else None

    # ── Positional Report ─────────────────────────────────────────────────────
    if click_tab_with_retry(driver, "Attack Sides", "#live-pitch-stats"):
        positional = scrape_positional_from_dom(driver)
        result.update(positional)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════


def parse_events(data: dict, ws_match_id: str, league: str, season: str) -> list[dict]:
    """
    Convertit matchCentreData["events"] en liste de dicts plats
    alignés sur silver.stg_whoscored_events.
    Chaque qualifier est sérialisé en JSON dans qualifiers_json.
    """
    raw_events = data.get("events", [])
    scraped_at = datetime.now().isoformat(timespec="seconds")
    rows = []

    for idx, ev in enumerate(raw_events):
        # Type et outcome — structure dict {value, displayName}
        t_raw = ev.get("type", {})
        type_id   = t_raw.get("value")   if isinstance(t_raw, dict) else t_raw
        type_name = t_raw.get("displayName", "") if isinstance(t_raw, dict) else ""

        o_raw = ev.get("outcomeType", {})
        outcome_id   = o_raw.get("value")   if isinstance(o_raw, dict) else o_raw
        outcome_name = o_raw.get("displayName", "") if isinstance(o_raw, dict) else ""

        # Period
        p_raw = ev.get("period", {})
        period = p_raw.get("value") if isinstance(p_raw, dict) else p_raw

        # Qualifiers → JSON string
        qualifiers_json = json.dumps(ev.get("qualifiers", []), ensure_ascii=False)

        rows.append({
            "ws_match_id":     ws_match_id,
            "row_num":         idx,
            "event_id":        ev.get("eventId") or int(ev["id"]) if ev.get("id") else None,
            "league_source":   league,
            "season":          season,
            "minute":          ev.get("minute"),
            "second":          ev.get("second"),
            "expanded_minute": ev.get("expandedMinute"),
            "period":          period,
            "team_id":         ev.get("teamId"),
            "player_id":       ev.get("playerId"),
            "x":               ev.get("x"),
            "y":               ev.get("y"),
            "end_x":           ev.get("endX"),
            "end_y":           ev.get("endY"),
            "type_id":         type_id,
            "type_name":       type_name,
            "outcome_id":      outcome_id,
            "outcome_name":    outcome_name,
            "is_touch":        ev.get("isTouch", False),
            "is_shot":         ev.get("isShot", False),
            "qualifiers_json": qualifiers_json,
            "scraped_at":      scraped_at,
        })

    # Extraire les infos du match
    match_date = data.get("startDate", "")
    if match_date:
        match_date = match_date[:10]  # "2017-08-12T00:00:00" → "2017-08-12"

    match_index = {
        "ws_match_id":    ws_match_id,
        "match_date":     match_date or None,
        "home_team_id":   data.get("home", {}).get("teamId"),
        "home_team_name": data.get("home", {}).get("name"),
        "away_team_id":   data.get("away", {}).get("teamId"),
        "away_team_name": data.get("away", {}).get("name"),
        "league_source":  league,
        "season":         season,
        "scraped_at":     scraped_at,
    }

    return rows, match_index


def scrape_match(driver, match: dict) -> Optional[dict]:
    """
    Scrape un match report WhoScored.
    Stratégie :
      1. Navigation vers l'URL du match (page Live ou Preview)
      2. Tentative d'extraction allStats depuis le source JS
      3. Si allStats absent → clic Match Report + scraping DOM fallback
      4. Retourne un dict aligné sur stg_whoscored_match_details
    """
    ws_id        = match["ws_match_id"]
    url          = match["url"]
    league       = match["league_source"]
    season       = match["season"]

    logger.info(f"  Match {ws_id} — {url}")

    try:
        driver.uc_open_with_reconnect(url, 5)
        handle_cookies(driver)
        human_delay(1, 3)

        # Vérification anti-ban + paywall WhoScored+
        current_url = driver.current_url
        title       = driver.title.lower()

        if "/plus" in current_url or "utm_campaign=BAU_POPUP_WS" in current_url:
            logger.warning(f"  💳 Paywall WhoScored+ détecté — match {ws_id} skippé (skip_reason=paywall)")
            mark_paywall(ws_id)
            return None

        if any(x in title for x in ["403", "blocked", "access denied", "captcha"]):
            logger.warning(f"  🚫 Blocage détecté — pause 60s")
            time.sleep(60)
            driver.uc_open_with_reconnect(url, 5)
            human_delay(3, 5)
            # Re-vérifier après retry
            if "/plus" in driver.current_url:
                logger.warning(f"  💳 Toujours sur /plus après retry — skip {ws_id}")
                mark_paywall(ws_id)
                return None

        page_source = driver.page_source



        # ── Extraction matchCentreData ────────────────────────────────────────

        if "matchCentreData: null" in page_source or "matchCentreData:null" in page_source:
            logger.warning(f"  📭 matchCentreData=null pour {ws_id} — données absentes")
            mark_no_data(ws_id)
            return None

        payload = extract_all_stats_from_source(page_source)
        if not payload:
            logger.warning(f"  ⚠️  Aucune donnée extractible pour {ws_id}")
            mark_no_data(ws_id)  # ← aussi ici pour les autres cas sans data
            return None
        
        # payload = extract_all_stats_from_source(page_source)
        # if not payload:
        #     logger.warning(f"  ⚠️  Aucune donnée extractible pour {ws_id}")
        #     return None

        method = payload.get("source", "unknown")
        data   = payload.get("data", {})

        # ── Conversion events bruts → liste de dicts ──────────────────────────
        events, match_index = parse_events(data, ws_id, league, season)

        if not events:
            logger.warning(f"  ⚠️  0 event parsé pour {ws_id}")
            return None

        logger.debug(f"  {len(events)} events extraits (méthode: {method})")
        return events, match_index

    except Exception as e:
        logger.error(f"  ❌ Erreur scraping {ws_id} : {e}")
        return None


def run_scraping(
    limit: Optional[int] = None,
    single_id: Optional[str] = None,
    dry_run: bool = False,
    headless: bool = False,
    ua_index: int = 0,
) -> dict:
    """
    Pipeline principal de scraping des match details.
    """
    # Charger les URLs à scraper
    if single_id:
        pending = [m for m in load_pending_urls() if m["ws_match_id"] == single_id]
        if not pending:
            # Chercher même si déjà scrapé (pour re-scraper un match spécifique)
            conn = duckdb.connect(str(DB_PATH), read_only=True)
            rows = conn.execute(
                "SELECT ws_match_id, url, league_source, season "
                "FROM silver.stg_whoscored_urls WHERE ws_match_id = ?",
                [single_id]
            ).fetchall()
            conn.close()
            pending = [{"ws_match_id": r[0], "url": r[1],
                        "league_source": r[2], "season": r[3]} for r in rows]
    else:
        pending = load_pending_urls(limit=limit)

    total   = len(pending)
    summary = {"ok": 0, "failed": 0, "total": total}

    if total == 0:
        logger.info("  Aucun match à scraper (is_scraped=False)")
        return summary

    logger.info(f"  {total} match(s) à scraper")

    if dry_run:
        for m in pending[:5]:
            logger.info(f"  [DRY-RUN] {m['ws_match_id']} — {m['url']}")
        if total > 5:
            logger.info(f"  ... et {total-5} autres")
        return summary

    # Initialisation driver avec profil Chrome réel
    # Le profil contient les cookies WhoScored → évite le paywall WhoScored+
    CHROME_USER_DATA = r"C:\Users\marce\AppData\Local\Google\Chrome\User Data"

    # Initialisation driver + injection cookies WhoScored
    COOKIE_FILE = ROOT_DIR / "config" / "whoscored_cookies.json"

    driver = Driver(uc=True, headless=False)

    # Injection des cookies pour éviter le paywall WhoScored+
    if COOKIE_FILE.exists():
        driver.get("https://www.whoscored.com")
        human_delay(1, 3)
        handle_cookies(driver)
        with open(COOKIE_FILE, encoding="utf-8") as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                # Selenium n'accepte pas certains champs non standard
                cookie.pop("sameSite", None)
                cookie.pop("storeId", None)
                cookie.pop("hostOnly", None)
                driver.add_cookie(cookie)
            except Exception:
                pass
        try:
            driver.refresh()
            human_delay(2, 3)
        except Exception:
            logger.warning("  ⚠️ Refresh timeout — on continue sans refresh")
            human_delay(2, 3)
        logger.info("  🍪 Cookies WhoScored injectés")
    else:
        logger.warning("  ⚠️ Fichier cookies absent — risque paywall")

    RESTART_EVERY    = 50  # Restart préventif tous les 50 matchs
    MAX_CONSECUTIVE_FAILS = 3  # Restart si 3 échecs consécutifs

    consecutive_fails = 0

    try:
        for i, match in enumerate(pending):

            # Restart préventif ou curatif
            need_restart = (
                (i > 0 and i % RESTART_EVERY == 0) or
                (consecutive_fails >= MAX_CONSECUTIVE_FAILS)
            )

            if need_restart:
                if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                    logger.warning(f"  🔄 {consecutive_fails} échecs consécutifs — restart session...")
                else:
                    logger.info(f"  🔄 Restart préventif après {RESTART_EVERY} matchs...")
                
                driver.quit()
                time.sleep(120)
                consecutive_fails = 0
                driver = Driver(uc=True, headless=False)
                if COOKIE_FILE.exists():
                    driver.get("https://www.whoscored.com")
                    human_delay(3, 5)
                    handle_cookies(driver)
                    with open(COOKIE_FILE, encoding="utf-8") as f:
                        cookies = json.load(f)
                    for cookie in cookies:
                        cookie.pop("sameSite", None)
                        cookie.pop("storeId", None)
                        cookie.pop("hostOnly", None)
                        try:
                            driver.add_cookie(cookie)
                        except Exception:
                            pass
                    try:
                        driver.refresh()
                        human_delay(2, 3)
                    except Exception:
                        human_delay(2, 3)
                    logger.info("  🍪 Cookies réinjectés après restart")

            ws_id = match["ws_match_id"]
            logger.info(f"  [{i+1}/{total}] {ws_id}")

            row = scrape_match(driver, match)

            if row:
                events, match_index = row
                consecutive_fails = 0  # Reset compteur
                if upsert_events(events) and upsert_match_index(match_index):
                    mark_scraped(ws_id)
                    summary["ok"] += 1
                    append_tracking({
                        "ws_match_id":   ws_id,
                        "league_source": match["league_source"],
                        "season":        match["season"],
                        "status":        "success",
                        "method":        "matchCentreData",
                        "timestamp":     datetime.now().isoformat(timespec="seconds"),
                        "error":         "",
                    })
                else:
                    consecutive_fails += 1
                    summary["failed"] += 1
            else:
                summary["failed"] += 1
                consecutive_fails += 1
                append_tracking({
                    "ws_match_id":   ws_id,
                    "league_source": match["league_source"],
                    "season":        match["season"],
                    "status":        "no_data",
                    "method":        "none",
                    "timestamp":     datetime.now().isoformat(timespec="seconds"),
                    "error":         "scrape_match returned None",
                })

            # Pause anti-ban toutes les 10 pages
            if (i + 1) % 20 == 0:
                pause = random.uniform(20, 30)
                logger.info(f"  ⏸  Pause longue {pause:.0f}s...")
                time.sleep(pause)
            else:
                human_delay(3, 7)

    finally:
        driver.quit()

    return summary


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="WhoScored — Scraping des Match Report Details"
    )
    parser.add_argument("--limit",    type=int, default=None,
                        help="Nombre maximum de matchs à scraper")
    parser.add_argument("--ws-id",    default=None,
                        help="Scraper un seul match par ws_match_id")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Afficher sans scraper")
    parser.add_argument("--ua",       type=int, default=0, choices=[0, 1],
                        help="Index du User-Agent à utiliser (0=Windows, 1=Mac)")
    args = parser.parse_args()

    init_db()
    init_tracking_csv()

    logger.info("=== WhoScored — Scraping Match Details ===")

    summary = run_scraping(
        limit=args.limit,
        single_id=args.ws_id,
        dry_run=args.dry_run,
        headless=args.headless,
        ua_index=args.ua,
    )

    logger.success(
        f"=== Terminé — "
        f"{summary['ok']}/{summary['total']} OK | "
        f"{summary['failed']} échecs ==="
    )

    # Rapport rapide de couverture DuckDB
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        n_details = conn.execute(
            "SELECT COUNT(DISTINCT ws_match_id) FROM silver.stg_whoscored_events"
        ).fetchone()[0]
        n_urls = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_urls"
        ).fetchone()[0]
        n_pending = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_urls WHERE is_scraped = FALSE"
        ).fetchone()[0]
        conn.close()
        logger.info(
            f"  DuckDB : {n_details} matchs avec events | "
            f"{n_urls - n_pending}/{n_urls} URLs scrapées | "
            f"{n_pending} en attente"
        )
    except Exception:
        pass


if __name__ == "__main__":
    main()
