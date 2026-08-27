"""
Pipeline 01b — Odds
==================
Charge les fichiers CSV football-data.co.uk depuis data/raw/bets/,
normalise les noms d'équipes via team_mapping du config.yaml,
calcule les probabilités implicites Pinnacle + Average,
et stocke dans silver.odds dans DuckDB.

Pattern fichiers : {LEAGUE_SLUG}_{SEASON_CODE}.csv
Ex : ENG-Premier League_1718.csv

Usage :
    python pipelines/01b_odds.py
    python pipelines/01b_odds.py --reset
"""

# --- bootstrap : rend les modules partages (racine pipelines/) importables ---
import sys as _sys
from pathlib import Path as _Path
for _p in (str(_Path(__file__).resolve().parent), str(_Path(__file__).resolve().parents[1])):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
# ----------------------------------------------------------------------------
import argparse
import re
from pathlib import Path
from thefuzz import process, fuzz

import duckdb as _duckdb  # pour éviter conflit avec le paramètre duckdb dans les fonctions
import pandas as pd
import yaml
from loguru import logger
from logging_config import setup_logging

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH      = ROOT_DIR / CFG["paths"]["duckdb"]
BETS_DIR     = ROOT_DIR / CFG["paths"]["raw_data"]/ "bets"


# ── Mapping fichier → league_source canonical ─────────────────────────────────

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

# Clé = préfixe du fichier (avant le _XXXX.csv)
# Valeur = league_source tel qu'il existe dans gold.features_final
FILE_SLUG_TO_LEAGUE = {
    "ENG-Premier League": "Premier League",
    "ESP-La Liga":        "La Liga",
    "FRA-Ligue 1":        "Ligue 1",
    "GER-Bundesliga":     "Bundesliga",
    "ITA-Serie A":        "Serie A",
}

# Mapping code saison fichier → format YYYY-YYYY
def parse_season(code: str) -> str:
    """'2122' → '2021-2022'"""
    if len(code) == 4 and code.isdigit():
        return f"20{code[:2]}-20{code[2:]}"
    return code


# ── Normalisation équipes ─────────────────────────────────────────────────────
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


# ── Probabilités implicites ───────────────────────────────────────────────────

def implied_proba(h: float, d: float, a: float) -> tuple[float, float, float]:
    """
    Convertit 3 cotes en probabilités implicites normalisées.
    Retire la marge bookmaker via normalisation.
    Retourne (p_home, p_draw, p_away) ou (None, None, None) si cotes invalides.
    """
    try:
        p_h = 1.0 / h
        p_d = 1.0 / d
        p_a = 1.0 / a
        total = p_h + p_d + p_a
        if total <= 0:
            return None, None, None
        return p_h / total, p_d / total, p_a / total
    except (TypeError, ZeroDivisionError):
        return None, None, None


# ── Décodage des colonnes football-data → snake_case ──────────────────────────
#
# Les CSV football-data.co.uk ont un schéma variable selon la saison (jusqu'à
# 180 colonnes distinctes). On décode chaque colonne par MOTIF plutôt qu'à la
# main, selon la grammaire :  {bookmaker}[C]{marché+issue}
#   ex. B365CAHH = bet365 + Close + Asian Handicap Home → bet365_close_ah_home
#
# Trois blocs :
#   - STATS    : statistiques de match (buts, tirs, cartons…) — renommage direct
#   - BOOK     : préfixes bookmakers (testés du plus long au plus court)
#   - SPECIAL  : cas irréguliers (compteurs Betbrain, tailles de handicap,
#                ambiguïté Betfair `BFD` vs Betfred `BFD*`)

# Colonnes d'identité gérées explicitement dans parse_odds_file (jamais décodées)
_IDENTITY_RAW = {"Div", "Date", "Time", "HomeTeam", "AwayTeam", "FTR"}

# Statistiques de match → nom lisible
STATS_MAP = {
    "FTHG": "ft_home_goals", "FTAG": "ft_away_goals",
    "HTHG": "ht_home_goals", "HTAG": "ht_away_goals", "HTR": "ht_result",
    "Referee": "referee",
    "HS": "home_shots", "AS": "away_shots",
    "HST": "home_shots_target", "AST": "away_shots_target",
    "HF": "home_fouls", "AF": "away_fouls",
    "HC": "home_corners", "AC": "away_corners",
    "HY": "home_yellows", "AY": "away_yellows",
    "HR": "home_reds", "AR": "away_reds",
}

# Préfixes bookmakers → nom canonique
BOOK_MAP = {
    "B365": "bet365", "BFD": "betfred", "BMGM": "betmgm", "1XB": "onexbet",
    "BFE": "betfair_exch", "BF": "betfair", "BW": "betwin", "IW": "interwetten",
    "LB": "ladbrokes", "PS": "pinnacle", "WH": "william_hill", "VC": "vcbet",
    "BV": "betvictor", "CL": "coral", "GB": "gamebookers", "SB": "sportingbet",
    "SJ": "stanjames", "SY": "stanleybet", "BS": "bluesquare", "SO": "sporting_odds",
    "Max": "market_max", "Avg": "market_avg", "P": "pinnacle", "Bb": "betbrain",
}
# Test des préfixes du plus long au plus court (évite que "P" capture "PSH")
_BOOK_ORDER = sorted(BOOK_MAP, key=len, reverse=True)

_OUTCOME = {"H": "home", "D": "draw", "A": "away"}

# Cas irréguliers : compteurs, tailles de handicap, et BFD seul = Betfair draw
SPECIAL_MAP = {
    "BFD": "betfair_1x2_draw",          # 2425 : BFH/BFD/BFA = Betfair (≠ Betfred BFD*)
    "Bb1X2": "betbrain_n_1x2",          # nb de bookmakers agrégés (1X2)
    "BbOU": "betbrain_n_ou25",          # nb de bookmakers agrégés (over/under)
    "BbAH": "betbrain_n_ah",            # nb de bookmakers agrégés (handicap)
    "BbAHh": "betbrain_ah_line",        # taille du handicap Betbrain
    "AHh": "ah_line",                   # taille du handicap marché (ouverture)
    "AHCh": "ah_line_close",            # taille du handicap marché (clôture)
    "GBAH": "gamebookers_ah_line",
    "LBAH": "ladbrokes_ah_line",
    "B365AH": "bet365_ah_line",
}

# Colonnes décodées qui restent du TEXTE (le reste = cotes numériques DOUBLE)
_TEXT_DECODED = {"referee", "ht_result"}


def _market_tag(rest: str) -> str | None:
    """Traduit le suffixe marché+issue d'une colonne odds. None si non reconnu."""
    if rest in _OUTCOME:
        return f"1x2_{_OUTCOME[rest]}"
    if rest == ">2.5":
        return "ou25_over"
    if rest == "<2.5":
        return "ou25_under"
    if rest == "AHH":
        return "ah_home"
    if rest == "AHA":
        return "ah_away"
    # Agrégats Betbrain : Mx = maximum, Av = average, suivis de l'issue
    m = re.match(r"^(Mx|Av)(H|D|A|AHH|AHA|>2\.5|<2\.5)$", rest)
    if m:
        agg = {"Mx": "max", "Av": "avg"}[m.group(1)]
        sub = m.group(2)
        if sub in _OUTCOME:
            return f"{agg}_1x2_{_OUTCOME[sub]}"
        return {
            "AHH": f"{agg}_ah_home", "AHA": f"{agg}_ah_away",
            ">2.5": f"{agg}_ou25_over", "<2.5": f"{agg}_ou25_under",
        }[sub]
    return None


def decode_column(col: str) -> str | None:
    """
    Traduit un nom de colonne source en snake_case, ou None si non décodable
    (colonne d'identité, ou colonne inconnue à ignorer).
    """
    if col in STATS_MAP:
        return STATS_MAP[col]
    if col in SPECIAL_MAP:
        return SPECIAL_MAP[col]
    for prefix in _BOOK_ORDER:
        if col.startswith(prefix):
            rest = col[len(prefix):]
            close = False
            if rest.startswith("C") and rest != "C":   # 'C' = cote de clôture
                close = True
                rest = rest[1:]
            tag = _market_tag(rest)
            if tag is None:
                return None
            return f"{BOOK_MAP[prefix]}{'_close' if close else ''}_{tag}"
    return None


# Colonnes legacy conservées à l'identique pour ne pas casser backbone.sql / gold
# (propagation ultérieure : gold consommera les colonnes riches puis on supprimera
#  ces doublons). Chaque legacy pointe vers la colonne décodée équivalente.
LEGACY_ALIASES = {
    "odds_pinnacle_h": "pinnacle_1x2_home",
    "odds_pinnacle_d": "pinnacle_1x2_draw",
    "odds_pinnacle_a": "pinnacle_1x2_away",
    "odds_avg_h":      "market_avg_1x2_home",
    "odds_avg_d":      "market_avg_1x2_draw",
    "odds_avg_a":      "market_avg_1x2_away",
    "odds_max_h":      "market_max_1x2_home",
    "odds_max_d":      "market_max_1x2_draw",
    "odds_max_a":      "market_max_1x2_away",
}


# ── Parsing d'un fichier CSV ──────────────────────────────────────────────────

def parse_odds_file(filepath: Path, league_source: str, season: str) -> pd.DataFrame:
    """
    Charge un CSV football-data.co.uk et retourne un DataFrame wide-union :
      - bloc identité (date, season, league_source, home_team, away_team, …)
      - bloc statistiques de match (buts, tirs, cartons, arbitre…)
      - TOUTES les cotes du fichier, renommées en snake_case (decode_column)
      - colonnes legacy (compat backbone.sql) + probabilités implicites dérivées

    Les colonnes absentes d'un fichier ne sont pas créées ici : le remplissage
    à NULL se fait à la concaténation dans load_all_odds (union des colonnes).
    """
    # utf-8-sig retire le BOM présent sur les fichiers récents (﻿Div → Div)
    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding="latin-1", low_memory=False)

    if df.empty:
        logger.warning(f"  Fichier vide : {filepath.name}")
        return pd.DataFrame()

    df.columns = [c.strip() for c in df.columns]

    # Parsing date — football-data utilise dd/mm/yy ou dd/mm/yyyy
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date", "HomeTeam", "AwayTeam"])

    # ── Bloc identité ─────────────────────────────────────────────────────────
    out = pd.DataFrame(index=df.index)
    out["date"]          = df["Date"]
    out["season"]        = season
    out["league_source"] = league_source
    out["home_team"]     = df["HomeTeam"].apply(normalize_team, source="odds")
    out["away_team"]     = df["AwayTeam"].apply(normalize_team, source="odds")
    out["result_fdc"]    = df["FTR"].map({"H": "H", "D": "D", "A": "A"})
    if "Div"  in df.columns:
        out["div"]  = df["Div"]
    if "Time" in df.columns:
        out["time"] = df["Time"]

    # ── Décodage de toutes les autres colonnes (stats + cotes) ────────────────
    for raw in df.columns:
        if raw in _IDENTITY_RAW:
            continue
        snake = decode_column(raw)
        if snake is None:
            continue  # colonne inconnue → ignorée
        if snake in _TEXT_DECODED:
            out[snake] = df[raw].astype("string")
        else:
            out[snake] = pd.to_numeric(df[raw], errors="coerce")

    # ── Colonnes legacy (compat backbone.sql) ─────────────────────────────────
    # Pointent vers la colonne décodée équivalente si présente, sinon NaN.
    for legacy, decoded in LEGACY_ALIASES.items():
        # float('nan') (et non pd.NA) : garde un dtype float64 même si la colonne
        # décodée est absente, ce qui évite un FutureWarning au concat (colonnes
        # tout-NA de type object).
        out[legacy] = out[decoded] if decoded in out.columns else float("nan")

    # ── Probabilités implicites dérivées (Pinnacle + moyenne marché) ──────────
    pin = out.apply(
        lambda r: implied_proba(r.get("pinnacle_1x2_home"),
                                r.get("pinnacle_1x2_draw"),
                                r.get("pinnacle_1x2_away")), axis=1
    )
    out["pinnacle_prob_h"] = [x[0] for x in pin]
    out["pinnacle_prob_d"] = [x[1] for x in pin]
    out["pinnacle_prob_a"] = [x[2] for x in pin]

    avg = out.apply(
        lambda r: implied_proba(r.get("market_avg_1x2_home"),
                                r.get("market_avg_1x2_draw"),
                                r.get("market_avg_1x2_away")), axis=1
    )
    out["market_prob_h"] = [x[0] for x in avg]
    out["market_prob_d"] = [x[1] for x in avg]
    out["market_prob_a"] = [x[2] for x in avg]

    return out.dropna(subset=["date", "home_team", "away_team"])


# ── Chargement de tous les fichiers ──────────────────────────────────────────

def load_all_odds() -> pd.DataFrame:
    """Parcourt data/raw/bets/ et charge tous les CSV Big5."""
    all_dfs = []
    stats   = {"ok": 0, "skip": 0, "error": 0}

    for filepath in sorted(BETS_DIR.glob("*.csv")):
        if filepath.name == "Notes.txt":
            continue

        # Parsing du nom de fichier : ENG-Premier League_1718.csv
        stem = filepath.stem  # ENG-Premier League_1718
        parts = stem.rsplit("_", 1)
        if len(parts) != 2:
            logger.warning(f"  Pattern inattendu : {filepath.name} — skip")
            stats["skip"] += 1
            continue

        slug, season_code = parts[0], parts[1]

        league_source = FILE_SLUG_TO_LEAGUE.get(slug)
        if not league_source:
            logger.warning(f"  Ligue inconnue : '{slug}' — skip")
            stats["skip"] += 1
            continue

        season = parse_season(season_code)

        logger.debug(f"  Chargement : {filepath.name} → {league_source} {season}")

        try:
            df = parse_odds_file(filepath, league_source, season)
            if not df.empty:
                all_dfs.append(df)
                stats["ok"] += 1
                logger.debug(f"    {len(df)} matchs chargés")
        except Exception as e:
            logger.error(f"  Erreur sur {filepath.name} : {e}")
            stats["error"] += 1

    logger.info(f"  Fichiers : {stats['ok']} OK | {stats['skip']} skip | {stats['error']} erreurs")

    if not all_dfs:
        raise RuntimeError("Aucun fichier chargé — vérifier BETS_DIR")

    # Union des colonnes : concat aligne par nom, NaN là où un fichier ne
    # possède pas la colonne (bookmaker inexistant à cette saison).
    df_all = pd.concat(all_dfs, ignore_index=True)

    # Ordre de colonnes déterministe (schéma stable d'un run à l'autre) :
    #   identité → legacy → probas dérivées → reste (stats + cotes) trié
    identity = ["date", "season", "league_source", "home_team", "away_team",
                "result_fdc", "div", "time"]
    legacy   = list(LEGACY_ALIASES.keys())
    derived  = ["pinnacle_prob_h", "pinnacle_prob_d", "pinnacle_prob_a",
                "market_prob_h", "market_prob_d", "market_prob_a"]
    fixed    = identity + legacy + derived
    rest     = sorted(c for c in df_all.columns if c not in fixed)
    df_all   = df_all.reindex(columns=fixed + rest)

    logger.info(f"  Total : {len(df_all):,} matchs | {df_all['league_source'].nunique()} ligues "
                f"| {len(df_all.columns)} colonnes")
    return df_all


# ── Écriture dans DuckDB ──────────────────────────────────────────────────────

# Colonnes texte du DDL (le reste = DOUBLE ; 'date' = DATE)
_DDL_TEXT_COLS = {
    "season", "league_source", "home_team", "away_team", "result_fdc",
    "div", "time", "referee", "ht_result",
}


def _ddl_type(col: str) -> str:
    """Type DuckDB d'une colonne selon son nom (schéma wide auto-généré)."""
    if col == "date":
        return "DATE"
    if col in _DDL_TEXT_COLS:
        return "VARCHAR"
    return "DOUBLE"


def write_to_duckdb(df: pd.DataFrame, conn: _duckdb.DuckDBPyConnection, reset: bool = False):
    """
    (Re)crée silver.odds avec un DDL généré dynamiquement depuis les colonnes du
    DataFrame, puis insère les données.

    Le schéma est reconstruit à chaque run (DROP + CREATE) : silver.odds est une
    table de full-refresh entièrement dérivée des CSV, et le jeu de colonnes peut
    évoluer si football-data ajoute un bookmaker. `reset` est donc redondant mais
    conservé pour compatibilité de l'interface.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # Schéma wide : on repart d'une table neuve pour épouser les colonnes courantes
    conn.execute("DROP TABLE IF EXISTS silver.odds")

    cols_ddl = ",\n            ".join(f'"{c}" {_ddl_type(c)}' for c in df.columns)
    conn.execute(f"CREATE TABLE silver.odds (\n            {cols_ddl}\n        )")

    conn.register("df_odds", df)
    # BY NAME : insertion robuste à l'ordre des colonnes
    conn.execute("INSERT INTO silver.odds BY NAME SELECT * FROM df_odds")

    n = conn.execute("SELECT COUNT(*) FROM silver.odds").fetchone()[0]
    logger.info(f"  silver.odds : {n:,} lignes | {len(df.columns)} colonnes")


# ── Audit jointure ────────────────────────────────────────────────────────────

def audit_join(conn: _duckdb.DuckDBPyConnection):
    """
    Vérifie le taux de jointure entre silver.odds et gold.features_final.
    On joint sur (date, home_team = team WHERE venue = Home, league_source, season).
    """
    logger.info("── Audit jointure silver.odds ↔ gold.features_final ─────")

    result = conn.execute("""
        WITH gold_home AS (
            SELECT
                date::DATE   AS date,
                season,
                league_source,
                team         AS home_team,
                opponent     AS away_team
            FROM gold.features_final
            WHERE venue          = 'Home'
              AND comp_category  = 'Big5'
              AND result_1n2 IS NOT NULL
        ),
        joined AS (
            SELECT
                o.date, o.season, o.league_source,
                o.home_team, o.away_team,
                g.home_team IS NOT NULL AS matched
            FROM silver.odds o
            LEFT JOIN gold_home g
                ON  o.date          = g.date
                AND o.home_team     = g.home_team
                AND o.away_team     = g.away_team
                AND o.league_source = g.league_source
                AND o.season        = g.season
        )
        SELECT
            COUNT(*)                                           AS total_odds,
            SUM(CASE WHEN matched THEN 1 ELSE 0 END)          AS matched,
            ROUND(100.0 * SUM(CASE WHEN matched THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_match
        FROM joined
    """).fetchone()

    total, matched, pct = result
    logger.info(f"  Total cotes     : {total:,}")
    logger.info(f"  Matchés         : {matched:,}")
    logger.info(f"  Taux de match   : {pct:.2f}%")

    if pct < 90:
        logger.warning("  ⚠ Taux < 90% — vérifier les noms d'équipes non mappés")

    # Détail des non-matchés
    unmatched = conn.execute("""
        WITH gold_home AS (
            SELECT date::DATE AS date, season, league_source,
                   team AS home_team, opponent AS away_team
            FROM gold.features_final
            WHERE venue = 'Home' AND comp_category = 'Big5'
        )
        SELECT o.league_source, o.season, o.home_team, o.away_team, COUNT(*) AS nb
        FROM silver.odds o
        LEFT JOIN gold_home g
            ON  o.date          = g.date
            AND o.home_team     = g.home_team
            AND o.away_team     = g.away_team
            AND o.league_source = g.league_source
            AND o.season        = g.season
        WHERE g.home_team IS NULL
        GROUP BY o.league_source, o.season, o.home_team, o.away_team
        ORDER BY nb DESC
        LIMIT 20
    """).df()

    if not unmatched.empty:
        logger.warning(f"  Top non-matchés :\n{unmatched.to_string(index=False)}")


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main(reset: bool = False):
    logger.info("=== Démarrage pipeline 05 — Odds ===")

    df = load_all_odds()

    conn = _duckdb.connect(str(DB_PATH))
    write_to_duckdb(df, conn, reset=reset)
    audit_join(conn)
    conn.close()

    logger.success("=== Pipeline 05 terminé ===")


if __name__ == "__main__":
    setup_logging("odds")
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true",
                        help="Supprime et recrée silver.odds")
    args = parser.parse_args()
    main(reset=args.reset)