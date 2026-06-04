"""
Pipeline 01 — Ingest (Multi-source → Parquet)
===============================================
Ingère trois sources de données vers des fichiers Parquet Bronze.
Utilise Polars avec lazy execution pour la performance multi-cœur.

SOURCES SUPPORTÉES
──────────────────
1. FBref (HTML Selenium)
   Pattern  : matchlogs_{club}_{annee}_{categorie}.html
   Sortie   : data/raw/fbref/parquet/{categorie}/{club}_{saison}.parquet
   Grain    : 1 ligne = 1 match du point de vue d'un club

2. Understat (CSV)
   Pattern  : schedule_{league}_{annee}.csv  → résultat + xG par match
              stats_{league}_{annee}.csv     → stats avancées (npxG, ppda...)
   Sortie   : data/raw/understat/parquet/{type}/{league}_{saison}.parquet
   Grain    : 1 ligne = 1 match (format wide home/away)

3. WhoScored (HTML)
   Pattern  : {Defensive|Offensive|xG}_{Home|Away}[_{For|Against}]_{league}_{annee}.html
   Sortie   : data/raw/whoscored/parquet/{league}_{saison}.parquet
   Grain    : 1 ligne = 1 équipe × saison (stats agrégées)
   Les 8 fichiers d'une même ligue/saison sont joinés via Master List.

POLARS & LAZY EXECUTION
────────────────────────
Polars utilise le multi-threading natif sur tous les cœurs disponibles.
Le pattern lazy (.lazy() → transformations → .collect()) permet à Polars
d'optimiser le plan d'exécution avant de toucher les données.

TRAÇABILITÉ
────────────
Chaque Parquet contient deux colonnes systématiques :
  - source      : "fbref" | "understat" | "whoscored"
  - scraped_at  : mtime du fichier source (= heure réelle du scraping Selenium)

NORMALISATION DES ÉQUIPES
──────────────────────────
Toute équipe non trouvée dans team_mapping (config.yaml) déclenche un
WARNING mais n'interrompt pas l'ingestion. Compléter le mapping au fur
et à mesure des warnings.

IDEMPOTENCE
────────────
Mode incrémental par défaut : skip si le Parquet de destination existe.
--reset : supprime et recrée tous les Parquets.

Usage :
    python pipelines/01_ingest.py
    python pipelines/01_ingest.py --reset
    python pipelines/01_ingest.py --source fbref
    python pipelines/01_ingest.py --file path/to/file
"""

import os
import re
import sys
import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional
from itertools import groupby
from thefuzz import process, fuzz

import polars as pl
import pyarrow.parquet as pq
import yaml
from bs4 import BeautifulSoup
from loguru import logger
import duckdb as _duckdb
import dotenv

from gcs_utils import upload_to_gcs

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

RAW_DIR = ROOT_DIR / CFG["paths"]["raw_data"]
DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]

DIRS = {
    "fbref":     {"html":    RAW_DIR / "fbref"     / "html",
                  "parquet": RAW_DIR / "fbref"     / "parquet"},
    "understat": {"csv":     RAW_DIR / "understat" / "csv",
                  "parquet": RAW_DIR / "understat" / "parquet"},
    "whoscored": {"html":    RAW_DIR / "whoscored" / "html",
                  "parquet": RAW_DIR / "whoscored" / "parquet"},
}

dotenv.load_dotenv()

# Mapping de normalisation des noms d'équipes (chargé depuis config.yaml)
# Clé = nom brut tel qu'il apparaît dans les fichiers sources
# Valeur = nom canonique utilisé dans tout le pipeline

def _load_team_mapping() -> dict[str, str]:
    """
    Charge le team_mapping depuis referentiel.team_mapping dans DuckDB.
    Retourne un dict vide (avec WARNING) si la table est absente,
    pour ne pas bloquer l'ingestion si DuckDB n'est pas encore initialisé.
    """
    try:
        con = _duckdb.connect(str(DB_PATH), read_only=True)
        rows = con.execute(
            "SELECT club_name, alias FROM referentiel.team_mapping"
        ).fetchall()
        con.close()
        return {alias: club_name  for club_name , alias in rows}
    except Exception as e:
        logger.warning(f"[ingest] Impossible de charger referentiel.team_mapping : {e}")
        return {}

TEAM_MAPPING: dict[str, str] = _load_team_mapping()

# ── Logs ──────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.remove()  # supprime le sink terminal par défaut
logger.add(
    "logs/ingest.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)


# ── Utilitaires ───────────────────────────────────────────────────────────────

def parse_season(code: str) -> str:
    """'2122' → '2021-2022'"""
    if len(code) == 4 and code.isdigit():
        return f"20{code[:2]}-20{code[2:]}"
    return code


def normalize_team(name: str, source: str) -> str:
    if not name or not isinstance(name, str):
        return name

    clean_name = name.strip()

    # 1. Lookup exact en mémoire
    if clean_name in TEAM_MAPPING:
        return TEAM_MAPPING[clean_name]

    # 2. Fuzzy matching sur les clés existantes (seuil 90%)
    known_keys = list(TEAM_MAPPING.keys())
    if known_keys:
        match, score = process.extractOne(clean_name, known_keys, scorer=fuzz.token_sort_ratio)
        if score >= 90:
            logger.info(f"[{source}] Fuzzy match : '{clean_name}' → '{match}' ({score}%)")
            return TEAM_MAPPING[match]

    # 3. Nouvelle équipe — INSERT dans referentiel.team_mapping
    logger.warning(
        f"[{source}] Nouvelle équipe détectée : '{clean_name}'. "
        f"Ajout dans referentiel.team_mapping (raw=canonical par défaut)."
    )
    try:
        con = _duckdb.connect(str(DB_PATH))
        con.execute("""
            INSERT INTO referentiel.team_mapping (club_name, alias)
            SELECT ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM referentiel.team_mapping WHERE alias = ?
            )
        """, [clean_name, clean_name, clean_name])
        con.close()
        TEAM_MAPPING[clean_name] = clean_name  # sync mémoire pour la session
    except Exception as e:
        logger.error(f"[{source}] Impossible d'écrire dans referentiel.team_mapping : {e}")

    return clean_name

def scraped_at(path: Path) -> str:
    """
    Retourne la mtime du fichier source au format ISO.
    Représente l'heure réelle à laquelle Selenium a écrit le fichier,
    plus fiable que datetime.now() (heure d'exécution du script).
    """
    mtime = path.stat().st_mtime
    return datetime.fromtimestamp(mtime).isoformat(timespec="seconds")


def add_traceability(lf: pl.LazyFrame, source: str, path: Path) -> pl.LazyFrame:
    """Ajoute source et scraped_at à tout LazyFrame."""
    return lf.with_columns([
        pl.lit(source).alias("source"),
        pl.lit(scraped_at(path)).alias("scraped_at"),
    ])


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — FBref HTML
# ══════════════════════════════════════════════════════════════════════════════

_FBREF_RE = re.compile(r"^matchlogs_(.+?)_(\d{4})_(.+)$")


def parse_fbref_filename(path: Path) -> Optional[dict]:
    m = _FBREF_RE.match(path.stem)
    if not m:
        return None
    club, annee, categorie = m.group(1), m.group(2), m.group(3)
    club = normalize_team(club, "fbref")
    return {"club": club, "season": parse_season(annee),
            "categorie": categorie, "source": "fbref"}


def parse_fbref_html(path: Path, meta: dict) -> Optional[pl.DataFrame]:
    """
    Parse via data-stat (attributs HTML FBref).
    Utilise lxml comme parser BeautifulSoup (3-5x plus rapide que html.parser).
    """
    try:
        soup = BeautifulSoup(
            path.read_text(encoding="utf-8", errors="ignore"), "lxml"
        )
        table = soup.find("table", {"id": "matchlogs_for"})
        if not table:
            logger.warning(f"  matchlogs_for absent : {path.name}")
            return None

        header_row = table.find("thead").find_all("tr")[-1]
        cols = [th.get("data-stat") for th in header_row.find_all("th")]

        rows = []
        for tr in table.find("tbody").find_all("tr"):
            if "thead" in tr.get("class", []):
                continue
            row = {}
            for td in tr.find_all(["th", "td"]):
                stat = td.get("data-stat")
                if stat:
                    text = td.get_text(strip=True)
                    m_pen = re.match(r"^(\d+)\s*\(\d+\)", text)
                    row[stat] = m_pen.group(1) if m_pen else text
            if row:
                rows.append(row)

        if not rows:
            return None

        valid_cols = [c for c in cols if c]

        df = (
            pl.DataFrame(rows)
            .lazy()
            .select(valid_cols)
            .with_columns([
                pl.lit(meta["club"]).alias("team"),
                pl.lit(meta["season"]).alias("season"),
                pl.lit(meta["categorie"]).alias("stat_category"),
            ])
            .filter(pl.col("date").is_not_null() & (pl.col("date") != ""))
            .with_columns(
                pl.col("date").str.to_date(format="%Y-%m-%d", strict=False)
            )
            .filter(pl.col("date").is_not_null())
            .pipe(add_traceability, source="fbref", path=path)
            .collect()
        )

        logger.info(f"  {path.name} → {len(df)} lignes")
        return df

    except Exception as e:
        logger.error(f"  Erreur FBref {path.name} : {e}")
        return None


def ingest_fbref(files: list[Path], force: bool) -> dict:
    ok = err = skip = 0
    prq_dir = DIRS["fbref"]["parquet"]

    for path in files:
        meta = parse_fbref_filename(path)
        if not meta:
            err += 1
            continue

        season_safe = meta["season"].replace("-", "_")
        out = prq_dir / meta["categorie"] / f"{meta['club']}_{season_safe}.parquet"

        if not force and out.exists():
            logger.debug(f"  Skip FBref : {path.name}")
            skip += 1
            continue

        df = parse_fbref_html(path, meta)
        if df is None or df.is_empty():
            err += 1
            continue

        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out, compression="snappy")
        # Upload GCS — non bloquant si GCS indisponible
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if bucket_name:
            upload_to_gcs(out, bucket_name)
        logger.debug(f"  Écrit FBref : {out.name} ({out.stat().st_size // 1024} Ko)")
        ok += 1

    return {"ok": ok, "err": err, "skip": skip}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — Understat CSV
# ══════════════════════════════════════════════════════════════════════════════

_UNDERSTAT_RE = re.compile(r"^(schedule|stats)_(.+?)_(\d{4})$")

_SCHEDULE_RENAME = {
    "league":      "league_source",
    "game_id":     "match_id",
    "home_team":   "home_team",
    "away_team":   "away_team",
    "home_goals":  "home_goals",
    "away_goals":  "away_goals",
    "home_xg":     "home_xg",
    "away_xg":     "away_xg",
    "is_result":   "is_result",
    "url":         "match_url",
}

_STATS_RENAME = {
    "game_id":               "match_id",
    "home_expected_points":  "home_xpts",
    "away_expected_points":  "away_xpts",
    "home_np_xg":            "home_np_xg",
    "away_np_xg":            "away_np_xg",
    "home_np_xg_difference": "home_np_xg_diff",
    "away_np_xg_difference": "away_np_xg_diff",
    "home_ppda":             "home_ppda",
    "away_ppda":             "away_ppda",
    "home_deep_completions": "home_deep",
    "away_deep_completions": "away_deep",
}


def parse_understat_filename(path: Path) -> Optional[dict]:
    m = _UNDERSTAT_RE.match(path.stem)
    if not m:
        return None
    file_type, league, annee = m.group(1), m.group(2), m.group(3)
    return {"type": file_type, "league": league,
            "season": parse_season(annee), "source": "understat"}


def parse_understat_csv(path: Path, meta: dict) -> Optional[pl.DataFrame]:
    """
    Lazy scan CSV Understat.
    Correction vs version précédente : on infère les colonnes disponibles
    en une seule lecture (collect_schema), pas deux scan_csv successifs.
    """
    try:
        rename_map = _SCHEDULE_RENAME if meta["type"] == "schedule" else _STATS_RENAME

        # Inférer les colonnes disponibles sans lire toutes les données
        available_cols = pl.scan_csv(path).collect_schema().names()
        cols_to_select = [c for c in rename_map if c in available_cols]

        df = (
            pl.scan_csv(path, try_parse_dates=True)
            .select(cols_to_select)
            .rename({k: v for k, v in rename_map.items() if k in cols_to_select})
            .with_columns([
                pl.lit(meta["season"]).alias("season"),
                pl.lit(meta["league"]).alias("league_source"),
                pl.lit(meta["type"]).alias("file_type"),
            ])
            .pipe(add_traceability, source="understat", path=path)
            .collect()
        )

        # Normaliser les noms d'équipes home/away
        for col in ["home_team", "away_team"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).map_elements(
                        lambda name: normalize_team(name, "understat"),
                        return_dtype=pl.Utf8,
                    ).alias(col)
                )

        logger.info(f"  {path.name} → {len(df)} lignes")
        return df

    except Exception as e:
        logger.error(f"  Erreur Understat {path.name} : {e}")
        return None


def ingest_understat(files: list[Path], force: bool) -> dict:
    ok = err = skip = 0
    prq_dir = DIRS["understat"]["parquet"]

    for path in files:
        meta = parse_understat_filename(path)
        if not meta:
            err += 1
            continue

        season_safe = meta["season"].replace("-", "_")
        out = prq_dir / meta["type"] / f"{meta['league']}_{season_safe}.parquet"

        if not force and out.exists():
            logger.debug(f"  Skip Understat : {path.name}")
            skip += 1
            continue

        df = parse_understat_csv(path, meta)
        if df is None or df.is_empty():
            err += 1
            continue

        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out, compression="snappy")
        # Upload GCS — non bloquant si GCS indisponible
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if bucket_name:
            upload_to_gcs(out, bucket_name)
        logger.debug(f"  Écrit Understat : {out.name}")
        ok += 1

    return {"ok": ok, "err": err, "skip": skip}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 3 — WhoScored HTML
# ══════════════════════════════════════════════════════════════════════════════

_WHOSCORED_RE = re.compile(
    r"^(Defensive|Offensive|xG)"
    r"_(Home|Away)"
    r"(?:_(For|Against))?"
    r"_(.+?)"
    r"_(\d{4})$"
)

# Identification des tables par headers caractéristiques
# Plus robuste qu'un index fixe si WhoScored modifie la mise en page
_WHOSCORED_TARGET_HEADERS = {
    "Defensive":   {"Tackles pg", "Interceptions pg"},
    "Offensive":   {"Shots OT pg", "Dribbles pg"},
    "xG_For":      {"xG", "xGDiff"},
    "xG_Against":  {"xG", "xGDiff"},
}

_WHOSCORED_COL_MAPS = {
    "Defensive": {
        "Shots pg":         "ws_shots_conceded_pg",
        "Tackles pg":       "ws_tackles_pg",
        "Interceptions pg": "ws_interceptions_pg",
        "Fouls pg":         "ws_fouls_pg",
        "Offsides pg":      "ws_offsides_pg",
        "Rating":           "ws_def_rating",
    },
    "Offensive": {
        "Shots pg":     "ws_shots_pg",
        "Shots OT pg":  "ws_shots_ot_pg",
        "Dribbles pg":  "ws_dribbles_pg",
        "Fouled pg":    "ws_fouled_pg",
        "Rating":       "ws_att_rating",
    },
    "xG_For": {
        "xG":       "ws_xg_for",
        "Goals*":   "ws_goals_for",
        "xGDiff":   "ws_xg_diff_for",
        "Shots":    "ws_shots_for",
        "xG/Shots": "ws_xg_per_shot_for",
        "Rating":   "ws_xg_for_rating",
    },
    "xG_Against": {
        "xG":       "ws_xg_against",
        "Goals*":   "ws_goals_against",
        "xGDiff":   "ws_xg_diff_against",
        "Shots":    "ws_shots_against",
        "xG/Shots": "ws_xg_per_shot_against",
        "Rating":   "ws_xg_against_rating",
    },
}

# 8 combinaisons attendues par ligue/saison
_EXPECTED_COMBINATIONS = frozenset({
    "Defensive_Home", "Defensive_Away",
    "Offensive_Home", "Offensive_Away",
    "xG_For_Home",    "xG_For_Away",
    "xG_Against_Home","xG_Against_Away",
})


def parse_whoscored_filename(path: Path) -> Optional[dict]:
    m = _WHOSCORED_RE.match(path.stem)
    if not m:
        return None
    cat_raw, venue, direction, league, annee = m.groups()
    category = f"xG_{direction}" if cat_raw == "xG" else cat_raw
    return {"category": category, "venue": venue,
            "league": league, "season": parse_season(annee),
            "source": "whoscored"}


def _find_whoscored_table(soup: BeautifulSoup, category: str):
    """
    Trouve la table cible par signature de headers.
    Retourne None si introuvable ou vide.
    """
    target = _WHOSCORED_TARGET_HEADERS.get(category, set())
    for table in soup.find_all("table"):
        headers = {th.get_text(strip=True) for th in table.find_all("th")}
        n_rows = len([r for r in table.find_all("tr") if r.find("td")])
        if target.issubset(headers) and n_rows >= 10:
            return table
    return None


def _parse_whoscored_table(
    table, category: str, venue: str, meta: dict
) -> Optional[pl.DataFrame]:
    """
    Parse une table WhoScored.
    - Nettoie le rang "1. Team Name" → "Team Name"
    - Normalise via TEAM_MAPPING
    - Préfixe les colonnes avec venue (home_ / away_)
    """
    col_map = _WHOSCORED_COL_MAPS.get(category, {})
    if not col_map:
        return None

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    team_idx = next((i for i, h in enumerate(headers) if h.lower() == "team"), None)
    if team_idx is None:
        return None

    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cells or len(cells) < len(headers):
            continue

        raw_name = re.sub(r"^\d+\.\s*", "", cells[team_idx]).strip()
        team = normalize_team(raw_name, "whoscored")

        record = {"team": team}
        for i, header in enumerate(headers):
            if header in col_map and i < len(cells):
                # Préfixer venue : ws_shots_pg → ws_home_shots_pg
                new_col = col_map[header].replace("ws_", f"ws_{venue.lower()}_")
                record[new_col] = cells[i]
        rows.append(record)

    if not rows:
        return None

    return (
        pl.DataFrame(rows)
        .lazy()
        .with_columns([
            pl.lit(meta["season"]).alias("season"),
            pl.lit(meta["league"]).alias("league_source"),
        ])
        .collect()
    )


def _build_master_list(dfs_by_cat: dict[str, pl.DataFrame],
                       meta_league: str, meta_season: str) -> pl.DataFrame:
    """
    Construit la liste maître des équipes en faisant l'UNION de toutes
    les équipes présentes dans tous les fichiers du groupe.

    Pourquoi : un left join en chaîne depuis un seul fichier de base peut
    perdre des équipes si ce fichier est incomplet (rétrogradation,
    données WhoScored manquantes). L'UNION garantit qu'aucune équipe
    n'est perdue même si elle n'apparaît que dans un seul fichier.
    """
    all_teams = set()
    for df in dfs_by_cat.values():
        all_teams.update(df["team"].to_list())

    return pl.DataFrame({
        "team":          sorted(all_teams),
        "season":        meta_season,
        "league_source": meta_league,
    })


def ingest_whoscored(files: list[Path], force: bool) -> dict:
    """
    Groupe les fichiers par (league, season).
    Pour chaque groupe :
      1. Construit une Master List des équipes (UNION de tous les fichiers)
      2. Left-join chaque catégorie sur la Master List via lazy chain
      3. Écrit un seul Parquet consolidé par ligue/saison
    """
    ok = err = skip = 0
    prq_dir = DIRS["whoscored"]["parquet"]

    parsed = []
    for path in files:
        meta = parse_whoscored_filename(path)
        if meta:
            parsed.append((path, meta))
        else:
            logger.warning(f"  Pattern WhoScored non reconnu : {path.name}")

    def group_key(item):
        return (item[1]["league"], item[1]["season"])

    parsed.sort(key=group_key)

    for (league, season), group in groupby(parsed, key=group_key):
        group = list(group)
        season_safe = season.replace("-", "_")
        out = prq_dir / f"{league}_{season_safe}.parquet"

        if not force and out.exists():
            logger.debug(f"  Skip WhoScored : {league} {season}")
            skip += len(group)
            continue

        # Vérifier la complétude du groupe
        present = {f"{m['category']}_{m['venue']}" for _, m in group}
        missing = _EXPECTED_COMBINATIONS - present
        if missing:
            logger.warning(
                f"  WhoScored {league} {season} : {len(missing)} fichier(s) "
                f"manquant(s) {missing} — groupe ignoré"
            )
            skip += len(group)
            continue

        logger.info(f"  WhoScored {league} {season} : {len(group)} fichiers...")

        # Parser chaque fichier
        dfs_by_cat: dict[str, pl.DataFrame] = {}
        scraped_timestamps: list[str] = []
        failed = False

        for path, meta in group:
            soup = BeautifulSoup(
                path.read_text(encoding="utf-8", errors="ignore"), "lxml"
            )
            table = _find_whoscored_table(soup, meta["category"])
            if table is None:
                logger.error(f"  Table introuvable : {path.name}")
                failed = True
                break

            df_cat = _parse_whoscored_table(
                table, meta["category"], meta["venue"], meta
            )
            if df_cat is None or df_cat.is_empty():
                logger.error(f"  Parsing échoué : {path.name}")
                failed = True
                break

            key = f"{meta['category']}_{meta['venue']}"
            dfs_by_cat[key] = df_cat
            scraped_timestamps.append(scraped_at(path))

        if failed:
            err += len(group)
            continue

        # ── Master List → left-join lazy chain ───────────────────────────────
        master = _build_master_list(dfs_by_cat, league, season)
        result_lf = master.lazy()
        join_on = ["team", "season", "league_source"]

        for df_other in dfs_by_cat.values():
            stat_cols = [c for c in df_other.columns if c not in join_on]
            result_lf = result_lf.join(
                df_other.lazy().select(join_on + stat_cols),
                on=join_on,
                how="left",
            )

        # Traçabilité : timestamp le plus récent des 8 fichiers
        latest_scraped = max(scraped_timestamps)
        result = (
            result_lf
            .with_columns([
                pl.lit("whoscored").alias("source"),
                pl.lit(latest_scraped).alias("scraped_at"),
            ])
            .collect()
        )

        out.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(out, compression="snappy")
        # Upload GCS — non bloquant si GCS indisponible
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if bucket_name:
            upload_to_gcs(out, bucket_name)
        logger.success(
            f"  WhoScored {league} {season} : {len(result)} équipes, "
            f"{len(result.columns)} colonnes → {out.name}"
        )
        ok += len(group)

    return {"ok": ok, "err": err, "skip": skip}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE_HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_HANDLERS = {
    "fbref": {
        "ingest": ingest_fbref,
        "glob":   "matchlogs_*.html",
        "dir":    DIRS["fbref"]["html"],
    },
    "understat": {
        "ingest": ingest_understat,
        "glob":   "*.csv",
        "dir":    DIRS["understat"]["csv"],
    },
    "whoscored": {
        "ingest": ingest_whoscored,
        "glob":   "*.html",
        "dir":    DIRS["whoscored"]["html"],
    },
}


# ── Rapport ───────────────────────────────────────────────────────────────────

def print_report():
    logger.info("── Rapport Parquet ──────────────────────────────────────")
    total_rows = 0
    for source, cfg in DIRS.items():
        prq = cfg["parquet"]
        if not prq.exists():
            continue
        files = list(prq.rglob("*.parquet"))
        if not files:
            continue
        rows = sum(pq.read_metadata(f).num_rows for f in files)
        logger.info(f"  {source:<12} {len(files):3d} fichiers  {rows:>8,} lignes")
        total_rows += rows
    logger.info(f"  {'TOTAL':<12}                {total_rows:>8,} lignes")


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main(reset: bool = False, source: Optional[str] = None, file: Optional[str] = None):
    # parser = argparse.ArgumentParser(description="Ingest multi-source → Parquet")
    # parser.add_argument("--reset",  action="store_true",
    #                     help="Supprime tous les Parquets et recrée")
    # parser.add_argument("--source", default=None,
    #                     choices=list(SOURCE_HANDLERS.keys()),
    #                     help="Traiter une seule source")
    # parser.add_argument("--file",   default=None,
    #                     help="Traiter un seul fichier")
    # args = parser.parse_args()

    logger.info("=== Démarrage ingest ===")

    if reset:
        for source, cfg in DIRS.items():
            prq = cfg["parquet"]
            if prq.exists():
                shutil.rmtree(prq)
                logger.info(f"  Parquets {source} supprimés")

    if file:
        path = Path(file)
        if _FBREF_RE.match(path.stem):       source = "fbref"
        elif _UNDERSTAT_RE.match(path.stem): source = "understat"
        elif _WHOSCORED_RE.match(path.stem): source = "whoscored"
        else:
            logger.error(f"Source non détectée pour : {path.name}")
            sys.exit(1)
        stats = SOURCE_HANDLERS[source]["ingest"]([path], force=True)
        logger.info(f"  {source} — {stats}")
        return

    sources = [source] if source else list(SOURCE_HANDLERS.keys())
    total_ok = total_err = 0

    for source in sources:
        handler = SOURCE_HANDLERS[source]
        src_dir = handler["dir"]
        if not src_dir.exists():
            logger.info(f"  {source} : dossier absent ({src_dir}), ignoré")
            continue

        files = sorted(src_dir.rglob(handler["glob"]))
        if not files:
            logger.info(f"  {source} : aucun fichier trouvé")
            continue

        logger.info(f"  {source} : {len(files)} fichier(s)")
        stats = handler["ingest"](files, force=reset)
        logger.info(
            f"  {source} → OK:{stats['ok']} "
            f"ERR:{stats['err']} SKIP:{stats['skip']}"
        )
        total_ok  += stats["ok"]
        total_err += stats["err"]

    logger.info(f"  Total — OK:{total_ok} ERR:{total_err}")
    print_report()
    logger.success("=== Ingest terminé ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest multi-source → Parquet")
    parser.add_argument("--reset",  action="store_true",
                        help="Supprime tous les Parquets et recrée")
    parser.add_argument("--source", default=None,
                        choices=list(SOURCE_HANDLERS.keys()),
                        help="Traiter une seule source")
    parser.add_argument("--file",   default=None,
                        help="Traiter un seul fichier")
    args = parser.parse_args()
    main(reset=args.reset, source=args.source, file=args.file)