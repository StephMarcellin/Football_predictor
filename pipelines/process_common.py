"""process_common.py — Socle partage Bronze -> Silver (mappings, helpers,
upsert_match_registry, ecriture DuckDB, quality check, report). Importe par
ingest_fbref.py, ingest_understat.py, ingest_whoscored_team_stats.py,
ingest_whoscored_events.py."""


import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import yaml
import hashlib
from loguru import logger
from thefuzz import fuzz, process as fuzzy_process

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"]["duckdb"]
RAW_DIR = ROOT_DIR / CFG["paths"]["raw_data"]

# Chargement dynamique depuis config.yaml
SEASON_FORMAT: str = CFG.get("season_format", "YYYY-YYYY")

# Le TEAM_MAPPING est chargé depuis DuckDB dans main(), une fois la connexion établie,
# puis injecté dans les fonctions de normalisation via une variable module-level.
# On initialise à vide — sera rempli par _init_team_mapping(con) dans main().
TEAM_MAPPING: dict[str, str] = {}
TEAM_MAPPING_IDS: dict[str, int] = {}

# Lignes team_mapping decouvertes pendant le run (fuzzy reussi, ADU, CE), en
# attente d'ecriture batch en DuckDB. Videe par _flush_team_mapping(con),
# appelee en fin de main() par chaque script ingest_*.py.
_PENDING_TEAM_MAPPING_ROWS: list[tuple[str, str, int]] = []

# Compte d'ADU releve par check_adu_pre() en debut de run — sert de reference
# a check_adu_post() pour detecter les NOUVEAUX ADU apparus pendant ce run.
_ADU_COUNT_AT_START: int | None = None


def _bootstrap_team_mapping(con: duckdb.DuckDBPyConnection) -> None:
    """
    Cree referentiel.team_mapping si elle n'existe pas encore. Ne charge
    PLUS RIEN depuis un seed CSV : dbt_project/seeds/team_mapping.csv est
    obsolete et ignore (ses team_id historiques n'etaient de toute facon
    pas des club_tm_id Transfermarkt fiables — cf. decisions_et_apprentissages).

    La table se reconstruit desormais de zero, exclusivement au fil des
    runs des 4 scripts Silver (ingest_whoscored_events.py, ingest_fbref.py,
    ingest_understat.py, ingest_whoscored_team_stats.py) via
    normalize_team_col() (fuzzy matching Transfermarkt + routage
    ADU/CE/Minor Club). ingest_bronze.py (ex-01_ingest.py) n'ecrit plus
    jamais dans cette table — son seul role est HTML/CSV -> Parquet Bronze.

    team_id est un sentinel numerique -1 (ADU) / -2 (CE) / 0 (Minor Club,
    non persiste) plutot qu'une chaine, pour rester compatible avec
    intermediate.match_registry.home_team_id qui est typee BIGINT.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS referentiel")
    con.execute("""
        CREATE TABLE IF NOT EXISTS referentiel.team_mapping (
            alias      VARCHAR NOT NULL,
            club_name  VARCHAR NOT NULL,
            team_id    BIGINT,
            PRIMARY KEY (alias)
        )
    """)
    n = con.execute("SELECT COUNT(*) FROM referentiel.team_mapping").fetchone()[0]
    logger.info(f"  referentiel.team_mapping : {n} entree(s) (table creee si absente, jamais reamorcee depuis un CSV)")


def _flush_team_mapping(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ecrit en une fois toutes les lignes team_mapping decouvertes depuis le
    dernier flush (fuzzy reussi, ADU, CE) — c'est le pendant "batch en fin
    de fichier/run" de la mise a jour immediate du cache memoire deja faite
    dans normalize_team_col(). A appeler en fin de main() de chaque script
    ingest_*.py, apres traitement de toutes ses sources.

    ON CONFLICT (alias) DO NOTHING : alias est deja garanti unique par le
    cache memoire (un alias decouvert une fois n'est plus jamais retente
    dans le meme run) — ce filet ne sert que si deux scripts du meme run
    process Python decouvrent independamment le meme alias avant qu'un
    flush intermediaire n'ait eu lieu.
    """
    global _PENDING_TEAM_MAPPING_ROWS
    if not _PENDING_TEAM_MAPPING_ROWS:
        return
    n = len(_PENDING_TEAM_MAPPING_ROWS)
    con.executemany(
        """
        INSERT INTO referentiel.team_mapping (alias, club_name, team_id)
        VALUES (?, ?, ?)
        ON CONFLICT (alias) DO NOTHING
        """,
        _PENDING_TEAM_MAPPING_ROWS,
    )
    logger.success(f"  referentiel.team_mapping : {n} nouvelle(s) ligne(s) ecrite(s)")
    _PENDING_TEAM_MAPPING_ROWS = []


def _init_team_mapping(con: duckdb.DuckDBPyConnection) -> None:
    """
    Charge referentiel.team_mapping dans la variable globale TEAM_MAPPING.
    Appelé une seule fois au démarrage de main() après ouverture de la connexion.
    Centraliser ici évite N connexions DuckDB dans les fonctions de normalisation.
    """
    global TEAM_MAPPING
    try:
        rows = con.execute(
            "SELECT club_name, alias FROM referentiel.team_mapping"
        ).fetchall()
        TEAM_MAPPING = {alias: club_name  for club_name , alias in rows}
        logger.info(f"  team_mapping chargé : {len(TEAM_MAPPING)} entrées")
    except Exception as e:
        logger.error(f"  Impossible de charger referentiel.team_mapping : {e}")
        TEAM_MAPPING = {}

def _init_team_mapping_ids(con: duckdb.DuckDBPyConnection) -> None:
    """
    Charge le dict {club_name: team_id} depuis referentiel.team_mapping.
    Appelé une seule fois dans main() après _init_team_mapping().
    """
    global TEAM_MAPPING_IDS
    try:
        rows = con.execute(
            "SELECT club_name, team_id FROM referentiel.team_mapping"
        ).fetchall()
        TEAM_MAPPING_IDS = {
            club_name: team_id
            for club_name, team_id in rows
            if team_id is not None
        }

        TEAM_MAPPING_IDS["Minor Club"] = 0  # Ajout de l'entrée "Minor Club" avec team_id = 0
        logger.info(f"  team_mapping_ids chargé : {len(TEAM_MAPPING_IDS)} entrées")
    except Exception as e:
        logger.error(f"  Impossible de charger team_mapping_ids : {e}")
        TEAM_MAPPING_IDS = {}
# competition_mapping : chargé depuis referentiel.competition_mapping dans DuckDB.
# Deux vues plates pré-calculées pour un accès O(1) dans la hot path de normalisation.
# Initialisées à vide — remplies par _init_competition_mapping(con) dans main().
COMP_TO_CANONICAL: dict[str, str] = {}
COMP_TO_CATEGORY:  dict[str, str] = {}

def _init_competition_mapping(con: duckdb.DuckDBPyConnection) -> None:
    """
    Charge referentiel.competition_mapping dans les variables globales
    COMP_TO_CANONICAL et COMP_TO_CATEGORY.
    Appelé une seule fois dans main() après ouverture de la connexion,
    au même endroit que _init_team_mapping().
    """
    global COMP_TO_CANONICAL, COMP_TO_CATEGORY
    try:
        rows = con.execute("""
            SELECT alias, competition_name, category
            FROM referentiel.competition_mapping
        """).fetchall()
        COMP_TO_CANONICAL = {alias: competition_name for alias, competition_name, _ in rows}
        COMP_TO_CATEGORY  = {alias: category  for alias, _, category  in rows}
        logger.info(f"  competition_mapping chargé : {len(COMP_TO_CANONICAL)} entrées")
    except Exception as e:
        logger.error(f"  Impossible de charger referentiel.competition_mapping : {e}")
        COMP_TO_CANONICAL = {}
        COMP_TO_CATEGORY  = {}

# TM_CLUBS_BY_SEASON_LEAGUE : {(saison, ligue): [(club_name, club_tm_id), ...]}
# depuis referentiel.transfermarkt_clubs. Couvre les équipes D1/D2 Big5 sur
# les saisons du projet. Pool de candidats pour le fuzzy matching de
# normalize_team_col() — jamais les clubs europeens (transfermarkt_clubs ne
# les couvre pas).
# Initialisé à vide — rempli par _init_transfermarkt(con) dans main().
TM_CLUBS_BY_SEASON_LEAGUE: dict[tuple[str, str], list[tuple[str, int]]] = {}

def _init_transfermarkt(con: duckdb.DuckDBPyConnection) -> None:
    """
    Charge referentiel.transfermarkt_clubs et regroupe par (saison, ligue) :
    c'est le pool de candidats pour le fuzzy matching de normalize_team_col().
    On a besoin de club_tm_id (pas juste du nom) pour pouvoir l'ecrire comme
    team_id dans team_mapping quand un match fuzzy est retenu.
    """
    global TM_CLUBS_BY_SEASON_LEAGUE
    try:
        rows = con.execute("""
            SELECT club_name, season, league, club_tm_id
            FROM referentiel.transfermarkt_clubs
        """).fetchall()
        grouped: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
        for club_name, season, league, club_tm_id in rows:
            grouped[(season, league)].append((club_name, club_tm_id))
        TM_CLUBS_BY_SEASON_LEAGUE = dict(grouped)
        logger.info(
            f"  transfermarkt_clubs charge : {len(rows)} clubs, "
            f"{len(TM_CLUBS_BY_SEASON_LEAGUE)} couples (saison, ligue)"
        )
    except Exception as e:
        logger.error(f"  Impossible de charger referentiel.transfermarkt_clubs : {e}")
        TM_CLUBS_BY_SEASON_LEAGUE = {}


def _init_match_registry(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS intermediate.match_registry (
            match_id      VARCHAR PRIMARY KEY,
            match_date    DATE,
            home_team_id  BIGINT,
            away_team_id  BIGINT,
            league_source VARCHAR,
            season        VARCHAR
        )
    """)
    logger.info("  match_registry initialisé")


# ══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY GATE — ADU (Big5/D2 non resolus)
# ══════════════════════════════════════════════════════════════════════════════

def check_adu_pre() -> None:
    """
    Gate de qualite — DEBUT de run. Ouvre sa propre connexion DuckDB (comme
    chaque script ingest_*.py) et bloque immediatement le pipeline si
    referentiel.team_mapping contient deja des lignes ADU (equipes Big5/D2
    dont le fuzzy matching a echoue lors d'un run precedent) : elles doivent
    etre corrigees a la main (mise a jour de la ligne dans team_mapping)
    avant de relancer quoi que ce soit.

    Stocke le compte dans _ADU_COUNT_AT_START pour que check_adu_post()
    puisse determiner combien de NOUVEAUX ADU sont apparus pendant ce run.
    """
    global _ADU_COUNT_AT_START
    con = duckdb.connect(str(DB_PATH))
    # con.execute("DELETE FROM referentiel.team_mapping WHERE alias = 'ADU'")
    try:
        _bootstrap_team_mapping(con)
        n = con.execute(
            "SELECT COUNT(*) FROM referentiel.team_mapping WHERE club_name = 'ADU'"
        ).fetchone()[0]
    finally:
        con.close()
    _ADU_COUNT_AT_START = n
    logger.info(f"  Quality Gate (pre-run) : {n} ADU existant(s) dans referentiel.team_mapping")
    if n > 0:
        raise RuntimeError(
            f"Quality Gate ADU : {n} equipe(s) Big5/D2 non resolue(s) dans "
            f"referentiel.team_mapping (club_name='ADU'). Correction manuelle "
            f"requise avant de relancer — voir logs/suivi_team_mapping.log."
        )


def check_adu_post() -> None:
    """
    Gate de qualite — FIN de run. Bloque le passage aux phases suivantes
    (dbt run / feature engineering) si de NOUVEAUX ADU sont apparus pendant
    l'ingestion qui vient de se derouler. Les CE (clubs europeens hors
    perimetre du referentiel Transfermarkt) ne bloquent jamais — comptes
    ici uniquement pour visibilite dans les logs.
    """
    con = duckdb.connect(str(DB_PATH))
    try:
        # con.execute("DELETE FROM referentiel.team_mapping WHERE alias = 'ADU'")
        n_adu = con.execute(
            "SELECT COUNT(*) FROM referentiel.team_mapping WHERE club_name = 'ADU'"
        ).fetchone()[0]
        n_ce = con.execute(
            "SELECT COUNT(*) FROM referentiel.team_mapping WHERE club_name = 'CE'"
        ).fetchone()[0]
    finally:
        con.close()
    before = _ADU_COUNT_AT_START or 0
    new_adu = n_adu - before
    logger.info(
        f"  Quality Gate (post-run) : {n_adu} ADU au total ({new_adu} nouveau(x)), "
        f"{n_ce} CE au total"
    )
    if new_adu > 0:
        raise RuntimeError(
            f"Quality Gate ADU : {new_adu} nouvelle(s) equipe(s) Big5/D2 non "
            f"resolue(s) pendant ce run. Correction manuelle requise dans "
            f"referentiel.team_mapping avant de poursuivre vers dbt/feature "
            f"engineering. Voir logs/suivi_team_mapping.log."
        )


# ── Logs ──────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/process.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)
# Log d'audit séparé pour les entités non normalisées
logger.add(
    "logs/audit_unmapped.log",
    level="WARNING",
    encoding="utf-8",
    rotation="2 MB",
    filter=lambda record: "AUDIT" in record["message"],
    format="{time:YYYY-MM-DD HH:mm:ss} | {message}",
)
# Suivi_team_mapping : une ligne JSON par decision de mapping (fuzzy reussi,
# ADU, CE). Fichier local plutot que DuckDB (demande explicite) — les memes
# evenements remontent deja dans l'UI Prefect via le bridge loguru->Prefect
# de orchestrator_common.run_step(), ce fichier est la trace structuree.
logger.add(
    "logs/suivi_team_mapping.log",
    level="INFO",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    filter=lambda record: record["extra"].get("suivi_mapping", False),
    format="{message}",
)

# Registre global des entités non mappées (accumulé sur toute la run)
_UNMAPPED_REGISTRY: dict[str, set[str]] = defaultdict(set)


# ══════════════════════════════════════════════════════════════════════════════
# RÈGLES DE VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

CAT_A_ZERO_FILL: frozenset[str] = frozenset({
    # FBref shooting
    "standard_sh", "standard_sot", "standard_pk", "standard_pkatt", "standard_gls",
    # FBref keeper
    "saves", "sota", "cs", "ga_keeper",
    "pk_att", "pk_allowed", "pk_saved", "pk_missed",
    # FBref misc
    "crdy", "crdr", "crdy2", "fls", "fld", "off", "crosses",
    "int", "tklw", "pkwon", "pkcon", "og",
    # FBref schedule
    "gf", "ga",
    # Understat comptages
    "home_goals", "away_goals", "home_deep", "away_deep",
    # WhoScored comptages entiers
    "ws_home_shots_for", "ws_away_shots_for",
    "ws_home_shots_against", "ws_away_shots_against",
    "ws_home_goals_for", "ws_away_goals_for",
    "ws_home_goals_against", "ws_away_goals_against",
})

CAT_B_NULL_KEEP: frozenset[str] = frozenset({
    # Understat xG + ppda
    "home_xg", "away_xg",
    "home_np_xg", "away_np_xg",
    "home_np_xg_diff", "away_np_xg_diff",
    "home_xpts", "away_xpts",
    "home_ppda", "away_ppda",
    # FBref ratios
    "save_pct", "standard_sot_pct", "standard_g_sh", "standard_g_sot",
    # WhoScored ratings + xG agrégés
    "ws_home_def_rating", "ws_away_def_rating",
    "ws_home_att_rating", "ws_away_att_rating",
    "ws_home_xg_for", "ws_away_xg_for",
    "ws_home_xg_against", "ws_away_xg_against",
    "ws_home_xg_diff_for", "ws_away_xg_diff_for",
    "ws_home_xg_diff_against", "ws_away_xg_diff_against",
    "ws_home_xg_per_shot_for", "ws_away_xg_per_shot_for",
    "ws_home_xg_per_shot_against", "ws_away_xg_per_shot_against",
})

CAT_C_REJECT: frozenset[str] = frozenset({
    "date",
    "team", "opponent", "league_source", "season",  # FBref
    "home_team", "away_team",                        # Understat
})

CAT_D_OUTLIERS: list[dict] = [
    {"cols": ["gf", "ga", "standard_gls", "home_goals", "away_goals"],
     "op": "gt", "threshold": 12,
     "msg": "goals > 12"},
    {"cols": ["home_xg", "away_xg", "home_np_xg", "away_np_xg"],
     "op": "gt", "threshold": 7.0,
     "msg": "xG > 7.0"},
    {"cols": ["home_ppda", "away_ppda"],
     "op": "lt", "threshold": 2.0,
     "msg": "ppda < 2.0 — pressing quasi-parfait"},
    {"cols": ["save_pct", "standard_sot_pct"],
     "op": "gt", "threshold": 100.0,
     "msg": "pourcentage > 100 — erreur de données"},
    {"cols": ["ws_home_def_rating", "ws_away_def_rating",
              "ws_home_att_rating", "ws_away_att_rating"],
     "op": "gt", "threshold": 10.0,
     "msg": "WhoScored rating > 10 — impossible (max=10)"},
]


# ══════════════════════════════════════════════════════════════════════════════
# MAPPING DE STANDARDISATION
# ══════════════════════════════════════════════════════════════════════════════

FBREF_RENAME: dict[str, str] = {
    "goals_for": "gf", "goals_against": "ga",
    "start_time": "time", "dayofweek": "day",
    "comp": "league_source",
    "goals": "standard_gls", "shots": "standard_sh",
    "shots_on_target": "standard_sot",
    "shots_on_target_pct": "standard_sot_pct",
    "goals_per_shot": "standard_g_sh",
    "goals_per_shot_on_target": "standard_g_sot",
    "pens_made": "standard_pk", "pens_att": "standard_pkatt",
    "gk_shots_on_target_against": "sota", "gk_goals_against": "ga_keeper",
    "gk_saves": "saves", "gk_save_pct": "save_pct",
    "gk_clean_sheets": "cs", "gk_pens_att": "pk_att",
    "gk_pens_allowed": "pk_allowed", "gk_pens_saved": "pk_saved",
    "gk_pens_missed": "pk_missed",
    "cards_yellow": "crdy", "cards_red": "crdr", "cards_yellow_red": "crdy2",
    "fouls": "fls", "fouled": "fld", "offsides": "off",
    "interceptions": "int", "tackles_won": "tklw",
    "pens_won": "pkwon", "pens_conceded": "pkcon", "own_goals": "og",
    "possession": "poss",
}

# Colonnes opérationnelles supprimées avant Silver
# 'round' retiré définitivement : 'Matchweek 1' est inutile pour le ML
COLS_TO_DROP: frozenset[str] = frozenset({
    "stat_category", "match_report", "notes",
    "captain", "referee", "attendance",
    "round",
    # Understat : colonnes inutiles ML
    "game", "league_id", "season_id", "home_team_id", "away_team_id",
    "home_team_code", "away_team_code", "has_data", "file_type",
    # Traçabilité Bronze — gardée optionnellement
    # "source", "scraped_at",  ← décommenter si on veut alléger Silver
})


# ══════════════════════════════════════════════════════════════════════════════
# NORMALISATION — COMPÉTITIONS
# ══════════════════════════════════════════════════════════════════════════════
def _slugify(text: str) -> str:
    """Simplifie une chaîne pour faciliter le matching (minuscules, sans ponctuation, sans espaces doubles)."""
    if not text:
        return ""
    # Passage en minuscule
    text = text.lower()
    # Remplacement des caractères spéciaux/ponctuation par un espace
    text = re.sub(r"[^a-z0-9]", " ", text)
    # Suppression des espaces multiples et trim
    return " ".join(text.split())

def normalize_competition_col(df: pl.DataFrame, col: str, source: str) -> pl.DataFrame:
    if col not in df.columns:
        return df

    # 1. On prépare un mapping de "slugs" vers les vraies valeurs du YAML
    # On fait ça à l'intérieur ou on le pré-calcule globalement pour la performance
    slug_to_canonical = {_slugify(k): v for k, v in COMP_TO_CANONICAL.items()}
    slug_to_category = {_slugify(k): v for k, v in COMP_TO_CATEGORY.items()}

    unique_comps = df.select(col).unique().drop_nulls()[col].to_list()
    canonical_map: dict[str, str] = {}
    category_map:  dict[str, str] = {}

    for comp in unique_comps:
        comp_slug = _slugify(comp)

        canonical = slug_to_canonical.get(comp_slug)
        category  = slug_to_category.get(comp_slug)

        if canonical is None:
            # Si même après simplification on ne trouve pas, ALORS on logue
            logger.warning(
                f"AUDIT [{source}] Compétition non mappée : '{comp}' (slug: '{comp_slug}') "
                f"— ajouter dans referentiel.competition_mapping dans DuckDB"
            )
            _UNMAPPED_REGISTRY[f"{source}__competitions"].add(comp)
            canonical_map[comp] = comp
            category_map[comp]  = "Other"
        else:
            canonical_map[comp] = canonical
            category_map[comp]  = category or "Other"

    # Application du mapping sur le DataFrame
    df = df.with_columns([
        pl.col(col).replace(canonical_map).alias(col),
        pl.col(col).replace(category_map).alias("comp_category")
    ])

    return df


# ══════════════════════════════════════════════════════════════════════════════
# NORMALISATION — ÉQUIPES
# ══════════════════════════════════════════════════════════════════════════════

def _log_suivi_mapping(
    source: str,
    league: str,
    season: str,
    raw_alias: str,
    mapped_club_name: str,
    mapped_team_id: int | None,
    match_score: int | None,
    status: str,
) -> None:
    """
    Ecrit une ligne JSON dans logs/suivi_team_mapping.log pour chaque decision
    de mapping memorisee (succes fuzzy, ADU, CE) — pas pour Minor Club, qui
    n'est jamais memorise dans team_mapping (cf. normalize_team_col).

    Fichier local plutot que DuckDB (decision explicite) : le sink loguru
    filtre sur l'extra "suivi_mapping" et n'ecrit QUE la ligne JSON dans
    logs/suivi_team_mapping.log — les autres sinks (console, process.log)
    recoivent aussi cet appel avec leur propre format habituel, ce qui est
    voulu pour la remontee Prefect (bridge loguru->Prefect dans
    orchestrator_common.run_step()).
    """
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "league": league,
        "season": season,
        "raw_alias": raw_alias,
        "mapped_club_name": mapped_club_name,
        "mapped_team_id": mapped_team_id,
        "match_score": match_score,
        "status": status,
    }
    logger.bind(suivi_mapping=True).info(json.dumps(record, ensure_ascii=False))


def normalize_team_col(
    df: pl.DataFrame,
    col: str,
    source: str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> pl.DataFrame:
    """
    Normalise une colonne de noms d'équipes en 3 niveaux.

    Avant tout, le prefixe pays FBref (eng, fr, es, it, de, il, ch, hr, rs,
    cy, no, gr, ua, nl, se, by, pt...), concatene sans espace devant le nom
    du club dans la colonne "team" de FBref (ex: "ilMaccabi Tel Aviv",
    "chYoung Boys"), est retire une bonne fois : clean_name = raw_name sans
    ce prefixe. C'est un artefact d'encodage FBref, pas une info d'identite
    du club. TOUT ce qui suit (lookup team_mapping, fuzzy Transfermarkt,
    memorisation dans team_mapping.alias, log Suivi) utilise clean_name —
    plus jamais raw_name — pour que les alias memorises restent lisibles
    ("Maccabi Tel Aviv", "Young Boys") au lieu de porter le prefixe.

    Niveau 1 — team_mapping (exact puis slug)
        Lookup exact dans TEAM_MAPPING[clean_name], puis slugifie si echec.
        Si trouvé → nom canonique (ou label memorise ADU/CE d'un run precedent).

    Niveau 2 — fuzzy Transfermarkt (Big5/D2 uniquement)
        Appelé si le Niveau 1 a échoué ET que le nom apparait au moins une
        fois dans une ligne dont comp_category vaut Big5 ou D2 (calculee en
        amont par normalize_competition_col). On ne tente PAS le fuzzy pour
        les clubs europeens : referentiel.transfermarkt_clubs ne couvre que
        le perimetre Big5 D1/D2, jamais les clubs de Ligue des
        Champions/Europa/Conference — un candidat n'existera jamais.
        thefuzz.token_sort_ratio >= 85 sur les clubs de la même (saison,
        ligue) dans TM_CLUBS_BY_SEASON_LEAGUE. Match trouvé → mémorisé dans
        team_mapping (cache immédiat + écriture batch différée).

    Niveau 3 — routage du fallback
        Big5/D2 sans match fuzzy → "ADU" (bloquant, Quality Gate).
        Europe (Ligue des Champions/Europa/Conference) → "CE" (non bloquant).
        Ni l'un ni l'autre (Coupe nationale, D3+...) → "Minor Club"
        (comportement historique, non mémorisé — trop nombreux, trop peu
        réutilisés pour justifier une ligne dans team_mapping).

    Colonne ajoutée : raw_{col} (nom brut original, traçabilité)
    """
    if col not in df.columns:
        return df

    # Sauvegarde du nom brut pour la traçabilité
    df = df.with_columns(pl.col(col).alias(f"raw_{col}"))

    # Pré-calcul du slug_mapping pour le Niveau 1
    slug_team_mapping: dict[str, str] = {
        _slugify(k): v for k, v in TEAM_MAPPING.items()
    }

    has_season = "season" in df.columns
    has_league = "league_source" in df.columns
    has_category = "comp_category" in df.columns

    # Contexte (comp_category, saison, ligue) par nom brut : necessaire pour
    # savoir dans quelle branche router un nom qui echoue au Niveau 1
    # (Big5/D2 -> ADU si echec fuzzy, Europe -> CE, le reste -> Minor Club).
    name_contexts: dict[str, list[tuple[str, str, str]]] = {}
    if has_category and has_season and has_league:
        ctx_df = (
            df.select([col, "comp_category", "season", "league_source"])
            .unique()
            .drop_nulls(subset=[col])
        )
        for row in ctx_df.iter_rows(named=True):
            name_contexts.setdefault(row[col], []).append(
                (row["comp_category"], row["season"], row["league_source"])
            )

    unique_names = df.select(col).unique().drop_nulls()[col].to_list()
    local_map: dict[str, str] = {}
    tally = {"niveau1": 0, "fuzzy": 0, "adu": 0, "ce": 0, "minor_club": 0}

    for name in unique_names:
        raw_name = name.strip()

        # GARDE-FOU : On ignore les labels réservés si le DataFrame a déjà été partiellement transformé
        if raw_name in ("ADU", "CE", "Minor Club"):
            local_map[name] = raw_name
            continue

        # Prefixe pays FBref retire une bonne fois — reutilise partout ensuite
        # (lookup, fuzzy, memorisation). Sur les noms sans prefixe (autres
        # sources, ou FBref sans ce pattern), clean_name == raw_name.
        clean_name = re.sub(r'^[a-z]{2,3}(?=[A-Z])', '', raw_name)

        # ── Niveau 1 : team_mapping (exact puis slug) ─────────────────────────
        canonical = TEAM_MAPPING.get(clean_name)
        if canonical is None:
            canonical = slug_team_mapping.get(_slugify(clean_name))
        if canonical is not None:
            local_map[name] = canonical
            tally["niveau1"] += 1
            continue

        # ── Niveau 2/3 : fuzzy Transfermarkt (Big5/D2) + routage ADU/CE ───────
        contexts = name_contexts.get(name, [])
        target = next((c for c in contexts if c[0] in ("Big5", "D2")), None)
        is_big5_d2 = target is not None
        if target is None:
            target = next((c for c in contexts if c[0] == "Europe"), None)

        if target is not None:
            _, season_ctx, league_ctx = target
            season_norm = _parse_season_str(str(season_ctx))

            match_club, match_id, match_score = None, None, None
            # best_attempt_* : trace du meilleur candidat teste, MEME s'il est
            # rejete (score < 85) ou si is_big5_d2 est False. Sans ca, le log
            # ADU/CE_DETECTED confond deux cas tres differents : "aucun club
            # dans referentiel.transfermarkt_clubs pour cette (saison, ligue)"
            # (candidates vide) et "un candidat existait mais scorait trop
            # bas" (best_attempt_score renseigne) — la distinction est ce qui
            # permet de juger si un ADU est une vraie lacune referentielle ou
            # un faux negatif du scorer.
            best_attempt_club, best_attempt_score = None, None
            if is_big5_d2:
                # Le fuzzy n'a de sens que pour Big5/D2 : transfermarkt_clubs
                # ne couvre que ce perimetre, jamais les clubs europeens —
                # inutile de tenter un match qui ne peut jamais reussir.
                candidates = TM_CLUBS_BY_SEASON_LEAGUE.get((season_norm, league_ctx), [])
                if candidates:
                    best = fuzzy_process.extractOne(
                        clean_name, [c[0] for c in candidates], scorer=fuzz.token_sort_ratio,
                    )
                    if best is not None:
                        best_attempt_club, best_attempt_score = best
                        if best[1] >= 85:
                            match_club, match_score = best
                            match_id = next(tid for cname, tid in candidates if cname == match_club)

            if match_club is not None:
                TEAM_MAPPING[clean_name] = match_club
                TEAM_MAPPING_IDS[match_club] = match_id
                _PENDING_TEAM_MAPPING_ROWS.append((clean_name, match_club, match_id))
                local_map[name] = match_club
                tally["fuzzy"] += 1
                logger.info(
                    f"  [{source}][{col}] Fuzzy : '{clean_name}' -> '{match_club}' "
                    f"(score={match_score}, id={match_id})"
                )
                _log_suivi_mapping(source, league_ctx, season_norm, clean_name,
                                    match_club, match_id, match_score, "MAPPED_FUZZY")
            else:
                fallback_label = "ADU" if is_big5_d2 else "CE"
                fallback_id = -1 if is_big5_d2 else -2
                TEAM_MAPPING[clean_name] = fallback_label
                TEAM_MAPPING_IDS.setdefault(fallback_label, fallback_id)
                _PENDING_TEAM_MAPPING_ROWS.append((clean_name, fallback_label, fallback_id))
                local_map[name] = fallback_label
                tally["adu" if is_big5_d2 else "ce"] += 1
                log_fn = logger.error if is_big5_d2 else logger.warning
                if best_attempt_club is not None:
                    # Un candidat existait, rejete car sous le seuil (85) —
                    # ADU probablement un vrai faux negatif du scorer OU un
                    # club genuinement different (a verifier au cas par cas).
                    log_fn(
                        f"  [{source}][{col}] {fallback_label} : meilleur candidat pour "
                        f"'{clean_name}' ({league_ctx}, {season_norm}) = "
                        f"'{best_attempt_club}' (score={best_attempt_score}, seuil=85 non atteint)"
                    )
                else:
                    # Aucun candidat du tout : soit is_big5_d2=False (CE, cas
                    # normal), soit une vraie lacune de referentiel.transfermarkt_clubs
                    # pour cette (saison, ligue) — a verifier cote seed.
                    log_fn(
                        f"  [{source}][{col}] {fallback_label} : aucun candidat pour "
                        f"'{clean_name}' ({league_ctx}, {season_norm})"
                    )
                _log_suivi_mapping(source, league_ctx, season_norm, clean_name,
                                    fallback_label, fallback_id, best_attempt_score,
                                    f"{fallback_label}_DETECTED")
            continue

        # Ni Big5/D2 ni Europe (Coupe, D3+...) -> Minor Club, non memorise
        local_map[name] = "Minor Club"
        tally["minor_club"] += 1

    logger.info(
        f"  [{source}][{col}] Niveau1={tally['niveau1']} Fuzzy={tally['fuzzy']} "
        f"ADU={tally['adu']} CE={tally['ce']} MinorClub={tally['minor_club']}"
    )

    # Application du mapping sur le DataFrame
    df = df.with_columns(
        pl.col(col).replace_strict(local_map, default=pl.col(col)).alias(col)
    )

    return df

def normalize_team_id_col(
    df: pl.DataFrame,
    id_col: str,
    name_col: str,
    raw_col: str,
) -> pl.DataFrame:
    """
    Résout l'id référentiel (team_id Transfermarkt, ou ADU=-1 / CE=-2 /
    Minor Club=0) d'une colonne d'id natif à une source, via le nom déjà
    normalisé par normalize_team_col().

    Précondition : name_col doit déjà avoir été traité par
    normalize_team_col() — le lookup se fait sur le nom CANONIQUE, jamais
    sur l'id natif (qui ne sert qu'à la traçabilité, via raw_col).

    Args:
        df:       DataFrame contenant id_col et name_col.
        id_col:   colonne d'id natif à la source (écrasée en sortie par
                   l'id référentiel).
        name_col: colonne de nom déjà normalisée.
        raw_col:  nom de la colonne où sauvegarder l'id natif brut avant
                   écrasement (même principe que raw_{col} dans
                   normalize_team_col()).
    """
    if id_col not in df.columns or name_col not in df.columns:
        return df

    df = df.with_columns(pl.col(id_col).alias(raw_col))

    df = df.with_columns(
        pl.col(name_col).replace_strict(TEAM_MAPPING_IDS, default=None).alias(id_col)
    )

    return df
# ══════════════════════════════════════════════════════════════════════════════
# NORMALISATION — SAISONS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_season_str(s: str) -> str:
    """
    Convertit tous les formats de saison en format canonique "YYYY-YYYY".
    Formats supportés :
      "1718"     → "2017-2018"   (Understat brut int ou str)
      "2017"     → "2017-2018"   (Understat season_id)
      "2017-18"  → "2017-2018"   (variante courte)
      "2017-2018"→ "2017-2018"   (déjà canonique)
    """
    s = str(s).strip()
    if len(s) == 4 and s.isdigit():
        # "1718" → années 20xx
        if int(s[:2]) >= 90:  # "9899" → 1998-1999
            return f"19{s[:2]}-19{s[2:]}"
        return f"20{s[:2]}-20{s[2:]}"
    if len(s) == 4 and not s.isdigit():
        return s  # format inconnu
    # "2017" → "2017-2018"
    if len(s) == 4 and s.isdigit() and int(s) >= 1990:
        y = int(s)
        return f"{y}-{y+1}"
    # "2017-18" → "2017-2018"
    m = re.match(r'^(\d{4})-(\d{2})$', s)
    if m:
        y1, y2short = int(m.group(1)), int(m.group(2))
        y2 = int(str(y1)[:2] + m.group(2))
        return f"{y1}-{y2}"
    # "2017-2018" → déjà bon
    if re.match(r'^\d{4}-\d{4}$', s):
        return s
    return s


def standardize_season(df: pl.DataFrame) -> pl.DataFrame:
    """Convertit la colonne season en format canonique 'YYYY-YYYY'."""
    if "season" not in df.columns:
        return df
    return df.with_columns(
        pl.col("season")
        .cast(pl.Utf8)
        .map_elements(_parse_season_str, return_dtype=pl.Utf8)
        .alias("season")
    )


# ══════════════════════════════════════════════════════════════════════════════
# UTILITAIRES PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def standardize_date(df: pl.DataFrame) -> pl.DataFrame:
    """Force la colonne date en pl.Date ISO."""
    if "date" not in df.columns:
        return df
    if df["date"].dtype != pl.Date:
        df = df.with_columns(
            pl.col("date")
            .cast(pl.Utf8)
            .str.slice(0, 10)
            .str.to_date(format="%Y-%m-%d", strict=False)
        )
    return df

def encode_result_1n2(df: pl.DataFrame) -> pl.DataFrame:
    """W/D/L + venue → H/D/A (perspective équipe)."""
    if "result" not in df.columns or "venue" not in df.columns:
        return df
    return df.with_columns(
        pl.when(
            (pl.col("result") == "W") & (pl.col("venue").str.to_lowercase() == "home")
        ).then(pl.lit("H"))
        .when(
            (pl.col("result") == "W") & (pl.col("venue").str.to_lowercase() == "away")
        ).then(pl.lit("A"))
        .when(
            (pl.col("result") == "L") & (pl.col("venue").str.to_lowercase() == "home")
        ).then(pl.lit("A"))
        .when(
            (pl.col("result") == "L") & (pl.col("venue").str.to_lowercase() == "away")
        ).then(pl.lit("H"))
        .when(pl.col("result") == "D").then(pl.lit("D"))
        # Terrain neutre : pas de notion Home/Away — W/L/D encode directement
        # le résultat de l'équipe (perspective équipe conservée)
        .when(
            (pl.col("result") == "W") & (pl.col("venue").str.to_lowercase() == "neutral")
        ).then(pl.lit("H"))
        .when(
            (pl.col("result") == "L") & (pl.col("venue").str.to_lowercase() == "neutral")
        ).then(pl.lit("A"))
        .otherwise(None)
        .alias("result_1n2")
    )

def generate_match_id(df: pl.DataFrame) -> pl.DataFrame:
    """
    Génère un match_id déterministe et partagé pour les deux lignes d'un même match.

    - Les matchs Understat ont déjà un match_id (entier) → cast en Utf8, préfixe 'us_'
    - Les autres (Coupes, matchs sans Understat) → hash SHA1 sur (date, sorted(team, opponent), league_source)

    Clé de hash :
        "{date}|{team_min}|{team_max}|{league_source}"
        tri alphabétique des équipes → même ID pour les deux lignes du match

    Format final :
        Understat : "us_12345"
        FBref only : "fbref_a3f2c1d4e5"

    Préconditions :
        - Colonnes requises : date, team, opponent, league_source, match_id
        - normalize_team_col() déjà appliqué (noms canoniques)
        - standardize_season() déjà appliqué
    """
    required = {"date", "team", "opponent", "league_source", "match_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"generate_match_id : colonnes manquantes → {missing}")

    def _hash_match(date, team, opponent, league_source) -> str:
        team_a, team_b = sorted([str(team), str(opponent)])
        raw = f"{date}|{team_a}|{team_b}|{league_source}"
        return "fbref_" + hashlib.sha1(raw.encode()).hexdigest()[:10]

    return df.with_columns(
        pl.when(pl.col("match_id").is_not_null())
        # Understat match_id existant → homogénéisation du type + préfixe
        .then(pl.lit("us_") + pl.col("match_id").cast(pl.Utf8))
        # Pas de match_id → génération par hash
        .otherwise(
            pl.struct(["date", "team", "opponent", "league_source"])
            .map_elements(
                lambda s: _hash_match(
                    s["date"], s["team"], s["opponent"], s["league_source"]
                ),
                return_dtype=pl.Utf8,
            )
        )
        .alias("match_id")
    )

def apply_cat_c_rejection(df: pl.DataFrame, source: str) -> pl.DataFrame:
    active = [c for c in CAT_C_REJECT if c in df.columns]
    if not active:
        return df
    before = len(df)
    df = df.drop_nulls(subset=active)
    removed = before - len(df)
    if removed > 0:
        logger.warning(f"  [{source}] Cat C : {removed} ligne(s) rejetées (nulls identifiants)")
    return df

def apply_cat_a_zerofill(df: pl.DataFrame, source: str) -> pl.DataFrame:
    cols = [c for c in CAT_A_ZERO_FILL if c in df.columns]
    if not cols:
        return df
    filled = df.with_columns([pl.col(c).fill_null(0) for c in cols])
    n = sum(df[c].null_count() - filled[c].null_count() for c in cols)
    if n > 0:
        logger.debug(f"  [{source}] Cat A : {n} null(s) → 0")
    return filled

def apply_cat_d_outliers(df: pl.DataFrame, source: str) -> pl.DataFrame:
    for rule in CAT_D_OUTLIERS:
        for col in rule["cols"]:
            if col not in df.columns:
                continue
            mask = (pl.col(col) > rule["threshold"]) if rule["op"] == "gt" \
                   else (pl.col(col) < rule["threshold"])
            n = df.filter(mask.fill_null(False)).height
            if n > 0:
                examples = (
                    df.filter(mask.fill_null(False))
                    .select([c for c in ["date", "team", "home_team",
                                         "league_source", col] if c in df.columns])
                    .head(3).to_dicts()
                )
                logger.warning(f"  [{source}] Cat D — {col} : {n} ligne(s) ({rule['msg']})")
                for ex in examples:
                    logger.warning(f"    {ex}")
    return df

def cast_numeric_cols(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            if col in CAT_A_ZERO_FILL:
                exprs.append(pl.col(col).cast(pl.Int32, strict=False).alias(col))
            elif col in CAT_B_NULL_KEEP or col.startswith("ws_"):
                exprs.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
    if exprs:
        df = df.with_columns(exprs)
    return df


def remove_duplicates(df: pl.DataFrame, key_cols: list[str], source: str) -> pl.DataFrame:
    present = [c for c in key_cols if c in df.columns]
    if len(present) < 2:
        return df
    before = len(df)
    df = df.unique(subset=present, keep="first")
    removed = before - len(df)
    if removed > 0:
        logger.warning(f"  [{source}] {removed} doublon(s) supprimés (clé: {present})")
    return df


def drop_unused_cols(df: pl.DataFrame) -> pl.DataFrame:
    drop = [c for c in COLS_TO_DROP if c in df.columns]
    return df.drop(drop) if drop else df

def upsert_match_registry(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    date_col: str,
    home_col: str,
    away_col: str,
) -> None:
    """
    Insère les nouveaux matchs dans intermediate.match_registry.
    SHA1 calculé sur (date, home_team_id, away_team_id, league_source, season).
    Garantit l'unicité du match_id quelle que soit la source d'ingestion.
    """
    import hashlib
    import pandas as pd

    rows = (
        df
        .select([date_col, home_col, away_col, "league_source", "season"])
        .unique()
        .drop_nulls()
    )

    records = []
    # for row in rows.iter_rows(named=True):
    #     home_name = row[home_col]
    #     away_name = row[away_col]

    #     # Récupération des ID canoniques (BIGINT) depuis le référentiel
    #     home_id = TEAM_MAPPING_IDS.get(home_name)
    #     away_id = TEAM_MAPPING_IDS.get(away_name)

    #     # Fallback sur le nom textuel uniquement si l'ID n'est pas encore résolu (ex: ADU / None)
    #     h_key = str(home_id) if home_id is not None and home_id > 0 else str(home_name)
    #     a_key = str(away_id) if away_id is not None and away_id > 0 else str(away_name)

    for row in rows.iter_rows(named=True):
        # On récupère le nom brut si la colonne raw_ existe dans la ligne
        raw_h_col = f"raw_{home_col}"
        raw_a_col = f"raw_{away_col}"
        
        home_raw = row.get(raw_h_col, row[home_col])
        away_raw = row.get(raw_a_col, row[away_col])

        home_name = row[home_col]
        away_name = row[away_col]

        home_id = TEAM_MAPPING_IDS.get(home_name)
        away_id = TEAM_MAPPING_IDS.get(away_name)

        # Si l'ID n'est pas un vrai ID positif (> 0), on utilise le nom brut d'origine, PAS "ADU"
        h_key = str(home_id) if home_id is not None and home_id > 0 else str(home_raw)
        a_key = str(away_id) if away_id is not None and away_id > 0 else str(away_raw)

        # Clé déterministe basée sur les ID canoniques
        key = f"{row[date_col]}|{h_key}|{a_key}|{row['league_source']}|{row['season']}"
        match_id = hashlib.sha1(key.encode()).hexdigest()

        records.append({
            "match_id":      match_id,
            "match_date":    row[date_col],
            "home_team_id":  home_id,
            "away_team_id":  away_id,
            "league_source": row["league_source"],
            "season":        row["season"],
        })

    if not records:
        return

    df_reg = pd.DataFrame(records).drop_duplicates(subset=["match_id"])
    con.register("df_registry", df_reg)
    con.execute("""
        INSERT INTO intermediate.match_registry
        SELECT DISTINCT * FROM df_registry
        ON CONFLICT (match_id) DO NOTHING
    """)
    n = con.execute("SELECT COUNT(*) FROM intermediate.match_registry").fetchone()[0]
    logger.info(f"  match_registry : {n:,} matchs au total")

# ══════════════════════════════════════════════════════════════════════════════
# QUALITY CHECK (AUDIT)
# ══════════════════════════════════════════════════════════════════════════════

def run_quality_check(con: duckdb.DuckDBPyConnection) -> None:
    """
    Vérifie la joinabilité entre toutes les tables Silver.
    Logue les taux de match et les entités non normalisées.

    Checks effectués :
      1. Équipes uniques non normalisées (accumulées pendant le process)
      2. Taux de jointure fbref_schedule × understat_schedule (sur team + season)
      3. Taux de jointure fbref_schedule × whoscored_team_season (sur team + season)
      4. Cohérence des saisons entre sources
      5. Valeurs league_source hors Big 5 résiduelles
    """
    logger.info("══════════════════════════════════════")
    logger.info("  QUALITY CHECK — JOINABILITÉ SILVER  ")
    logger.info("══════════════════════════════════════")

    tables = {
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='silver'"
        ).fetchall()
    }

    # ── 1. Entités non normalisées (collectées pendant le process) ─────────────
    total_unmapped = sum(len(v) for v in _UNMAPPED_REGISTRY.values())
    if total_unmapped == 0:
        logger.success("  ✅ Toutes les entités sont normalisées")
    else:
        logger.warning(f"  ⚠️  {total_unmapped} entité(s) non normalisée(s) :")
        for ctx, names in sorted(_UNMAPPED_REGISTRY.items()):
            logger.warning(f"    [{ctx}] ({len(names)} noms) : {sorted(names)[:10]}"
                           + (" ..." if len(names) > 10 else ""))
            logger.warning(f"AUDIT [{ctx}] entités non normalisées : {sorted(names)}")

    # ── 2. Jointure fbref_schedule × understat_schedule ────────────────────────
    if "fbref_schedule" in tables and "understat_schedule" in tables:
        result = con.execute("""
            WITH fbref_teams AS (
                SELECT DISTINCT team, season, league_source FROM silver.fbref_schedule
            ),
            understat_teams AS (
                SELECT DISTINCT home_team AS team, season, league_source
                FROM silver.understat_schedule
                UNION
                SELECT DISTINCT away_team, season, league_source
                FROM silver.understat_schedule
            ),
            joined AS (
                SELECT f.team, f.season
                FROM fbref_teams f
                LEFT JOIN understat_teams u
                    ON f.team = u.team AND f.season = u.season
                WHERE u.team IS NOT NULL
            )
            SELECT
                (SELECT COUNT(DISTINCT team || season) FROM fbref_teams) AS fbref_count,
                COUNT(DISTINCT team || season) AS matched_count
            FROM joined
        """).fetchone()
        fbref_n, matched_n = result
        pct = (matched_n / fbref_n * 100) if fbref_n else 0
        icon = "✅" if pct >= 95 else "⚠️ "
        logger.info(f"  {icon} fbref_schedule × understat_schedule : "
                    f"{matched_n}/{fbref_n} ({pct:.1f}%)")
    else:
        logger.info("  ℹ️  fbref_schedule ou understat_schedule absent — check sauté")

    # ── 3. Jointure fbref_schedule × whoscored_team_season ─────────────────────
    if "fbref_schedule" in tables and "whoscored_team_season" in tables:
        result = con.execute("""
            WITH fbref_teams AS (
                SELECT DISTINCT team, season FROM silver.fbref_schedule
            ),
            ws_teams AS (
                SELECT DISTINCT team, season FROM silver.whoscored_team_season
            )
            SELECT
                (SELECT COUNT(*) FROM fbref_teams) AS fbref_count,
                COUNT(*) AS matched_count
            FROM fbref_teams f
            JOIN ws_teams w ON f.team = w.team AND f.season = w.season
        """).fetchone()
        fbref_n, matched_n = result
        pct = (matched_n / fbref_n * 100) if fbref_n else 0
        icon = "✅" if pct >= 90 else "⚠️ "
        logger.info(f"  {icon} fbref_schedule × whoscored_team_season : "
                    f"{matched_n}/{fbref_n} ({pct:.1f}%)")
    else:
        logger.info("  ℹ️  fbref_schedule ou whoscored_team_season absent — check sauté")

    # ── 4. Cohérence des formats de saison ──────────────────────────────────────
    bad_season_tables = []
    for t in tables:
        has_season = con.execute(
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema='silver' AND table_name='{t}' AND column_name='season'"
        ).fetchone()[0]
        if not has_season:
            continue
        n_bad = con.execute(
            f"SELECT COUNT(*) FROM silver.{t} "
            f"WHERE season IS NOT NULL AND season NOT LIKE '____-____'"
        ).fetchone()[0]
        if n_bad > 0:
            bad_season_tables.append((t, n_bad))

    if not bad_season_tables:
        logger.success("  ✅ Formats de saison cohérents dans toutes les tables")
    else:
        for t, n in bad_season_tables:
            logger.warning(f"  ⚠️  silver.{t} : {n} saison(s) hors format 'YYYY-YYYY'")

    # ── 5. Distribution des comp_category par table ──────────────────────────────
    for t in tables:
        has_cat = con.execute(
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema='silver' AND table_name='{t}' "
            f"AND column_name='comp_category'"
        ).fetchone()[0]
        if not has_cat:
            continue
        dist = con.execute(
            f"SELECT comp_category, COUNT(*) as n "
            f"FROM silver.{t} GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        dist_str = " | ".join(f"{cat}:{n}" for cat, n in dist)
        logger.info(f"  silver.{t:<30} comp_category → {dist_str}")
        # Vérifier présence de Minor Club dans team + opponent
        for team_col in ["team", "opponent", "home_team", "away_team"]:
            has_col = con.execute(
                f"SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema='silver' AND table_name='{t}' "
                f"AND column_name='{team_col}'"
            ).fetchone()[0]
            if not has_col:
                continue
            n_minor = con.execute(
                f"SELECT COUNT(*) FROM silver.{t} WHERE {team_col} = 'Minor Club'"
            ).fetchone()[0]
            if n_minor > 0:
                logger.info(
                    f"    {team_col} : {n_minor} 'Minor Club' "
                    f"(équipes de Coupe/Europe hors mapping)"
                )


    # ── 6. Garde-fous sur les valeurs (config/data_rules.yml) ──────────────────
    check_data_rules(con)
    logger.info("══════════════════════════════════════")

def check_data_rules(con: duckdb.DuckDBPyConnection,
                     rules_path: Path = ROOT_DIR / "config" / "data_rules.yml") -> None:
    """
    Garde-fous sur les VALEURS, définis en config (config/data_rules.yml).
    Format : {table: {colonne: {min?, max?, not_null?, severity?}}}.
    Chaque règle → une requête de comptage des violations. Violation :
    'warn' par défaut (log), ou 'fail' (lève une exception) si severity=fail.
    Le schéma de chaque table est résolu dynamiquement ; une table absente
    est ignorée (le check marche donc avant OU après dbt).
    """
    if not rules_path.exists():
        logger.warning(f"  Pas de fichier de règles : {rules_path}")
        return
    rules = yaml.safe_load(open(rules_path, encoding="utf-8")) or {}

    logger.info("── GARDE-FOUS VALEURS (data_rules.yml) ──")

    # table_name → schema (pour ne pas coder le schéma en dur)
    loc = {r[1]: r[0] for r in con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables"
    ).fetchall()}

    n_viol = 0
    for table, cols in rules.items():
        schema = loc.get(table)
        if not schema:
            logger.debug(f"  ⏩ Table absente, ignorée : {table}")
            continue
        for col, rule in cols.items():
            checks = []
            if "min" in rule:        checks.append((f'"{col}" < {rule["min"]}',  f"< {rule['min']}"))
            if "max" in rule:        checks.append((f'"{col}" > {rule["max"]}',  f"> {rule['max']}"))
            if rule.get("not_null"): checks.append((f'"{col}" IS NULL',          "NULL"))
            for cond, label in checks:
                bad = con.execute(
                    f'SELECT COUNT(*) FROM {schema}.{table} WHERE {cond}'
                ).fetchone()[0]
                if bad:
                    n_viol += 1
                    msg = f"  ⚠️  {table}.{col} : {bad} valeur(s) {label}"
                    if rule.get("severity") == "fail":
                        logger.error(msg)
                        raise ValueError(f"Règle violée (fail) : {table}.{col} {label}")
                    logger.warning(msg)

    if n_viol == 0:
        logger.success("  ✅ Toutes les règles de valeurs respectées")
# ══════════════════════════════════════════════════════════════════════════════
# ÉCRITURE DUCKDB + RAPPORT
# ══════════════════════════════════════════════════════════════════════════════

def _write_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    table_name: str,
    source_label: str,
) -> None:
    """Polars → Arrow → DuckDB (zéro copie)."""
    null_count = sum(df[c].null_count() for c in df.columns)
    logger.info(
        f"    Silver : {len(df):,} lignes × {len(df.columns)} cols "
        f"| nulls : {null_count:,}"
    )
    arrow_table = df.to_arrow()
    con.execute(f"DROP TABLE IF EXISTS silver.{table_name}")
    con.execute(f"CREATE TABLE silver.{table_name} AS SELECT * FROM arrow_table")
    n = con.execute(f"SELECT COUNT(*) FROM silver.{table_name}").fetchone()[0]
    logger.success(f"    silver.{table_name} : {n:,} lignes ✅")


def print_report(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("── Résumé Silver ────────────────────────────────────────")
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'silver' ORDER BY 1"
    ).fetchall()
    for (t,) in tables:
        n = con.execute(f"SELECT COUNT(*) FROM silver.{t}").fetchone()[0]
        ncols = con.execute(
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema='silver' AND table_name='{t}'"
        ).fetchone()[0]
        logger.info(f"  silver.{t:<35} {n:>7,} lignes  {ncols:>3} cols")

__all__ = [n for n in dir() if not n.startswith("__")]
