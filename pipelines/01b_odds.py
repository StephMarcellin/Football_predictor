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

import argparse
import re
from pathlib import Path
from thefuzz import process, fuzz

import duckdb as _duckdb  # pour éviter conflit avec le paramètre duckdb dans les fonctions
import pandas as pd
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH      = ROOT_DIR / CFG["paths"]["duckdb"]
BETS_DIR     = ROOT_DIR / CFG["paths"]["raw_data"]/ "bets"


# ── Logs ──────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/odds.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

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


# ── Parsing d'un fichier CSV ──────────────────────────────────────────────────

def parse_odds_file(filepath: Path, league_source: str, season: str) -> pd.DataFrame:
    """
    Charge un CSV football-data.co.uk et retourne un DataFrame normalisé avec :
      - date, season, league_source
      - home_team, away_team (normalisés)
      - cotes brutes Pinnacle + Average
      - probabilités implicites Pinnacle + Average
    """
    try:
        df = pd.read_csv(filepath, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding="latin-1", low_memory=False)

    if df.empty:
        logger.warning(f"  Fichier vide : {filepath.name}")
        return pd.DataFrame()

    # Parsing date — football-data utilise dd/mm/yy ou dd/mm/yyyy
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date", "HomeTeam", "AwayTeam"])

    # Normalisation équipes
    df["home_team"] = df["HomeTeam"].apply(normalize_team, source = 'odds')
    df["away_team"] = df["AwayTeam"].apply(normalize_team, source = 'odds')

    # Résultat réel (vérification cohérence)
    df["result_fdc"] = df["FTR"].map({"H": "H", "D": "D", "A": "A"})

    # ── Cotes brutes ──────────────────────────────────────────────────────────

    # Pinnacle (source de référence — le plus sharp)
    for col in ["PSH", "PSD", "PSA"]:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Average marché
    for col in ["AvgH", "AvgD", "AvgA"]:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Maximum marché
    for col in ["MaxH", "MaxD", "MaxA"]:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Probabilités implicites ───────────────────────────────────────────────

    # Pinnacle
    pinnacle_proba = df.apply(
        lambda r: implied_proba(r["PSH"], r["PSD"], r["PSA"]), axis=1
    )
    df["pinnacle_prob_h"] = [x[0] for x in pinnacle_proba]
    df["pinnacle_prob_d"] = [x[1] for x in pinnacle_proba]
    df["pinnacle_prob_a"] = [x[2] for x in pinnacle_proba]

    # Average marché
    avg_proba = df.apply(
        lambda r: implied_proba(r["AvgH"], r["AvgD"], r["AvgA"]), axis=1
    )
    df["market_prob_h"] = [x[0] for x in avg_proba]
    df["market_prob_d"] = [x[1] for x in avg_proba]
    df["market_prob_a"] = [x[2] for x in avg_proba]

    # ── Assemblage final ──────────────────────────────────────────────────────

    result = pd.DataFrame({
        "date":           df["Date"],
        "season":         season,
        "league_source":  league_source,
        "home_team":      df["home_team"],
        "away_team":      df["away_team"],
        "result_fdc":     df["result_fdc"],
        # Cotes brutes Pinnacle
        "odds_pinnacle_h": df["PSH"],
        "odds_pinnacle_d": df["PSD"],
        "odds_pinnacle_a": df["PSA"],
        # Cotes brutes Average
        "odds_avg_h":     df["AvgH"],
        "odds_avg_d":     df["AvgD"],
        "odds_avg_a":     df["AvgA"],
        # Cotes brutes Max
        "odds_max_h":     df["MaxH"],
        "odds_max_d":     df["MaxD"],
        "odds_max_a":     df["MaxA"],
        # Probabilités implicites Pinnacle
        "pinnacle_prob_h": df["pinnacle_prob_h"],
        "pinnacle_prob_d": df["pinnacle_prob_d"],
        "pinnacle_prob_a": df["pinnacle_prob_a"],
        # Probabilités implicites Average
        "market_prob_h":  df["market_prob_h"],
        "market_prob_d":  df["market_prob_d"],
        "market_prob_a":  df["market_prob_a"],
    })

    return result.dropna(subset=["date", "home_team", "away_team"])


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

    df_all = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"  Total : {len(df_all):,} matchs | {df_all['league_source'].nunique()} ligues")
    return df_all


# ── Écriture dans DuckDB ──────────────────────────────────────────────────────

def write_to_duckdb(df: pd.DataFrame, conn: _duckdb.DuckDBPyConnection, reset: bool = False):
    """Crée silver.odds et insère les données."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    if reset:
        conn.execute("DROP TABLE IF EXISTS silver.odds")
        logger.info("  Table silver.odds supprimée")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver.odds (
            date             DATE,
            season           VARCHAR,
            league_source    VARCHAR,
            home_team        VARCHAR,
            away_team        VARCHAR,
            result_fdc       VARCHAR,
            odds_pinnacle_h  DOUBLE,
            odds_pinnacle_d  DOUBLE,
            odds_pinnacle_a  DOUBLE,
            odds_avg_h       DOUBLE,
            odds_avg_d       DOUBLE,
            odds_avg_a       DOUBLE,
            odds_max_h       DOUBLE,
            odds_max_d       DOUBLE,
            odds_max_a       DOUBLE,
            pinnacle_prob_h  DOUBLE,
            pinnacle_prob_d  DOUBLE,
            pinnacle_prob_a  DOUBLE,
            market_prob_h    DOUBLE,
            market_prob_d    DOUBLE,
            market_prob_a    DOUBLE
        )
    """)

    conn.execute("DELETE FROM silver.odds")
    conn.register("df_odds", df)
    conn.execute("INSERT INTO silver.odds SELECT * FROM df_odds")

    n = conn.execute("SELECT COUNT(*) FROM silver.odds").fetchone()[0]
    logger.info(f"  silver.odds : {n:,} lignes insérées")


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true",
                        help="Supprime et recrée silver.odds")
    args = parser.parse_args()
    main(reset=args.reset)