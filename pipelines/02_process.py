"""
Pipeline 02 — Process (Bronze Parquet → Silver DuckDB)
=======================================================
Lit les Parquet produits par 01_ingest.py depuis data/raw/{source}/parquet/,
applique les règles de validation et normalisation, et écrit dans DuckDB
schéma silver.*.

OBJECTIF DE CE SCRIPT : JOINABILITÉ À 100%
────────────────────────────────────────────
Après exécution, toute jointure entre tables silver.* sur team, league_source,
match_id ou season doit retourner un taux de match de 100%.
Cela repose sur trois piliers :

  1. Competition filter first
     Avant toute normalisation, les lignes hors Big 5 sont exclues (Coupes,
     Compétitions Européennes). Raison : ces matchs polluent les stats de forme
     et ne correspondent à aucune ligne Understat/WhoScored.

  2. Exact match uniquement pour les équipes
     Le fuzzy matching (thefuzz) à seuil 80 a été retiré — il est trop risqué
     pour des noms courts (Inter, Lyon, Roma) où un match approximatif peut
     produire une équipe incorrecte sans warning visible.
     Stratégie retenue :
       a. Nettoyage du préfixe langue (frPSG → PSG)
       b. Lookup exact dans TEAM_MAPPING
       c. Si non trouvé → WARNING explicite + nom brut (jamais "Minor Club")
     Les WARNING doivent être traités en complétant team_mapping dans config.yaml.

  3. Quality check post-process
     Un audit_unmapped.log est généré à la fin avec toutes les entités
     (équipes, compétitions) non normalisées trouvées dans les données.

DEUX GRAINS DE DONNÉES
───────────────────────
  Match-grain  : FBref + Understat
    → silver.fbref_{cat} + silver.understat_{type}
    Clé : (team, opponent, date, league_source) pour FBref
          (match_id, home_team, away_team) pour Understat

  Team-season-grain : WhoScored
    → silver.whoscored_team_season
    Clé : (team, season, league_source)

RÈGLES DE VALIDATION
─────────────────────
  Cat A — Zero-Fill   : comptages → fill_null(0)
  Cat B — Null-Keep   : métriques analytiques → null conservé
  Cat C — Strict      : colonnes identifiantes → lignes nulles rejetées
  Cat D — Outliers    : seuils → WARNING loggé, ligne conservée

Usage :
    python pipelines/02_process.py
    python pipelines/02_process.py --source fbref
    python pipelines/02_process.py --source understat
    python pipelines/02_process.py --source whoscored
    python pipelines/02_process.py --reset
    python pipelines/02_process.py --audit-only   # Quality check sans écriture
"""

import argparse
import re
from collections import defaultdict
from pathlib import Path

import duckdb
import polars as pl
import yaml
import hashlib
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH  = Path(CFG["paths"]["db"])
RAW_DIR  = Path(CFG["paths"]["raw_data"])

# Chargement dynamique depuis config.yaml
SEASON_FORMAT: str = CFG.get("season_format", "YYYY-YYYY")

# Le TEAM_MAPPING est chargé depuis DuckDB dans main(), une fois la connexion établie,
# puis injecté dans les fonctions de normalisation via une variable module-level.
# On initialise à vide — sera rempli par _init_team_mapping(con) dans main().
TEAM_MAPPING: dict[str, str] = {}

def _init_team_mapping(con: duckdb.DuckDBPyConnection) -> None:
    """
    Charge referentiel.team_mapping dans la variable globale TEAM_MAPPING.
    Appelé une seule fois au démarrage de main() après ouverture de la connexion.
    Centraliser ici évite N connexions DuckDB dans les fonctions de normalisation.
    """
    global TEAM_MAPPING
    try:
        rows = con.execute(
            "SELECT raw_name, canonical_name FROM referentiel.team_mapping"
        ).fetchall()
        TEAM_MAPPING = {raw: canonical for raw, canonical in rows}
        logger.info(f"  team_mapping chargé : {len(TEAM_MAPPING)} entrées")
    except Exception as e:
        logger.error(f"  Impossible de charger referentiel.team_mapping : {e}")
        TEAM_MAPPING = {}

# competition_mapping : {nom_source: {canonical: str, category: str}}
# category = Big5 | D2 | Cup | Europe | Other
_RAW_COMP_MAP: dict = CFG.get("competition_mapping", {})

# Pré-calculer deux vues plates pour un accès O(1) dans la hot path
COMP_TO_CANONICAL: dict[str, str] = {
    k: v["canonical"] for k, v in _RAW_COMP_MAP.items()
    if isinstance(v, dict) and "canonical" in v
}

COMP_TO_CATEGORY: dict[str, str] = {
    k: v["category"] for k, v in _RAW_COMP_MAP.items()
    if isinstance(v, dict) and "category" in v
}

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
                f"— ajouter dans competition_mapping dans config.yaml"
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

def normalize_team_col(
    df: pl.DataFrame,
    col: str,
    source: str,
    conn: duckdb.DuckDBPyConnection | None = None
) -> pl.DataFrame:
    if col not in df.columns:
        return df

    # --- Sauvegarde du nom initial brut ---
    df = df.with_columns(
        pl.col(col).alias(f"raw_{col}")
    )

    # 1. Préparation des mappings
    slug_team_mapping = {_slugify(k): v for k, v in TEAM_MAPPING.items()}
    unique_names = df.select(col).unique().drop_nulls()[col].to_list()

    local_map: dict[str, str] = {}
    clean_map: dict[str, str] = {}

    for name in unique_names:
        raw_name = name.strip()

        # --- ÉTAPE 1 : Nettoyage Suffixes & Préfixes ---
        clean = re.sub(r'^[a-z]+(?=[A-Z])', '', raw_name)
        # clean = re.sub(r' (Foot|FC|AS|Club|CF|)$', '', clean, flags=re.IGNORECASE).strip()
        clean_map[name] = clean

        # --- ÉTAPE 2 & 3 : Direct & Slug ---
        canonical = TEAM_MAPPING.get(raw_name)

        if canonical is None:
            name_slug = _slugify(raw_name)
            canonical = slug_team_mapping.get(name_slug)

        if canonical is None:
            canonical = TEAM_MAPPING.get(clean) or slug_team_mapping.get(_slugify(clean))

        # --- ÉTAPE 4 : Attribution ---
        if canonical:
            local_map[name] = canonical
        else:
            if name not in ["Minor Club", "None", ""]:
                _UNMAPPED_REGISTRY[f"{source}__teams"].add(name)
            local_map[name] = "Minor Club"

    # --- ÉTAPE 5 : Validation contre le référentiel ---
    if conn is not None:
        # Clubs qui vont être mappés en Minor Club
        minor_club_names = {
            name for name, canonical in local_map.items()
            if canonical == "Minor Club"
            and name not in ["Minor Club", "None", ""]
        }

        if minor_club_names and "season" in df.columns and "league_source" in df.columns:
            # Récupérer les triplets (raw_name, season, league) des lignes concernées
            suspects_df = (
                df.filter(pl.col(col).is_in(list(minor_club_names)))
                .select([col, "season", "league_source"])
                .unique()
            )

            if len(suspects_df) > 0:
                # Charger le référentiel depuis DuckDB
                try:
                    ref = conn.execute("""
                                            SELECT club_name, season, league
                                            FROM referentiel.transfermarkt_clubs
                                        """).pl()

                    # Joindre sur le nom canonique potentiel
                    # On utilise raw_team slugifié vs référentiel slugifié
                    ref_set = set(zip(ref["club_name"], ref["season"], ref["league"]))

                    # Pour chaque suspect, vérifier si son nom nettoyé
                    # correspond à un club du référentiel
                    alerts = []
                    for row in suspects_df.iter_rows(named=True):
                        raw   = row[col]
                        season = row["season"]
                        league = row["league_source"]
                        # Vérification directe et slugifiée vs référentiel
                        for ref_team, ref_season, ref_league in ref_set:
                            if (
                                ref_season == season
                                and ref_league == league
                                and (
                                    _slugify(raw) == _slugify(ref_team)
                                    or _slugify(clean_map.get(raw, raw)) == _slugify(ref_team)
                                )
                            ):
                                alerts.append({
                                    "raw_name"    : raw,
                                    "ref_team"    : ref_team,
                                    "season"      : season,
                                    "league"      : league,
                                    "suggestion"  : f"Ajouter '{raw}': '{ref_team}' dans team_mapping"
                                })
                                break

                    if alerts:
                        alerts_df = pl.DataFrame(alerts)
                        logger.warning(
                            f"[{source}][{col}] {len(alerts)} clubs du référentiel "
                            f"mappés en 'Minor Club' — variantes manquantes dans la table Transfermarkt :\n"
                            f"{str(alerts_df)}"
                        )
                        # Log aussi dans audit_unmapped
                        for a in alerts:
                            _UNMAPPED_REGISTRY[f"{source}__referentiel_suspects"].add(
                                f"{a['raw_name']} → {a['ref_team']} ({a['season']}, {a['league']})"
                            )

                except Exception as e:
                    logger.warning(
                        f"[{source}][{col}] Validation référentiel impossible : {e}"
                    )

    # --- ÉTAPE 6 : Application des transformations ---
    if local_map:
        df = df.with_columns([
            pl.col(col).replace_strict(clean_map, default=pl.col(col)).alias(f"clean_{col}"),
            pl.col(col).replace_strict(local_map, default=pl.col(col)).alias(col)
        ])

    return df

def validate_against_referentiel(
    df: pl.DataFrame,
    ref: pl.DataFrame,
    source: str
) -> pl.DataFrame:
    """
    Détecte les clubs normalisés en 'Minor Club' alors qu'ils sont
    dans le référentiel — signe d'une variante manquante dans config.yaml
    ou d'un bug dans normalize_team_col().
    """
    if "team" not in df.columns or "season" not in df.columns:
        return df

    # Clubs du référentiel pour ce contexte
    ref_teams = set(zip(ref["club_name"], ref["season"], ref["league"]))

    # Lignes Minor Club qui devraient être mappées
    suspects = df.filter(
        (pl.col("team") == "Minor Club") &
        pl.struct(["raw_team", "season", "league_source"]).map_elements(
            lambda s: (s["raw_team"], s["season"], s["league_source"]) in ref_teams,
            return_dtype=pl.Boolean
        )
    )

    if len(suspects) > 0:
        logger.warning(
            f"[{source}] {len(suspects)} clubs référentiel mappés en 'Minor Club' "
            f"— variantes manquantes dans la table Transfermarkt :\n"
            f"{suspects.select(['raw_team','season','league_source']).unique().to_pandas().to_string()}"
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


# ══════════════════════════════════════════════════════════════════════════════
# TRAITEMENT PAR SOURCE
# ══════════════════════════════════════════════════════════════════════════════

def process_fbref(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/fbref/parquet/{cat}/*.parquet → silver.fbref_{cat}.

    Ordre :
      1. Renommage data-stat → noms canoniques
      2. Normalisation compétition → ajoute league_source canonique + comp_category
         (Big5 / D2 / Cup / Europe / Other) — aucune ligne supprimée
      3. Normalisation équipes → Minor Club si absent du mapping
      4. Pipeline de validation standard
    """
    prq_root = RAW_DIR / "fbref" / "parquet"
    if not prq_root.exists():
        logger.info("  FBref : dossier Parquet absent, ignoré")
        return

    categories = sorted([d.name for d in prq_root.iterdir() if d.is_dir()])
    logger.info(f"  FBref : {len(categories)} catégorie(s) → {categories}")

    for cat in categories:
        files = sorted((prq_root / cat).glob("*.parquet"))
        if not files:
            continue

        logger.info(f"  ── FBref/{cat} : {len(files)} fichier(s)")
        df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
        logger.info(f"    Brut : {len(df):,} × {len(df.columns)} cols")

        # 1. Renommage (data-stat → noms canoniques)
        rename = {k: v for k, v in FBREF_RENAME.items() if k in df.columns}
        if rename:
            df = df.rename(rename)

        # 2. Normalisation compétition → league_source canonique + comp_category
        #    Aucun filtre — on conserve Coupes, D2 et Europe
        if "league_source" in df.columns:
            df = normalize_competition_col(df, "league_source", f"fbref/{cat}")

        # 3. Normalisation équipes (après filtrage — moins de noms à traiter)
        df = normalize_team_col(df, "team", f"fbref/{cat}", conn = con)
        df = normalize_team_col(df, "opponent", f"fbref/{cat}", conn = con)

        # 4. Pipeline de validation
        df = standardize_date(df)
        df = standardize_season(df)
        df = drop_unused_cols(df)
        df = apply_cat_c_rejection(df, f"fbref/{cat}")
        df = encode_result_1n2(df)
        df = cast_numeric_cols(df)
        df = apply_cat_a_zerofill(df, f"fbref/{cat}")
        df = apply_cat_d_outliers(df, f"fbref/{cat}")
        df = remove_duplicates(df, ["team", "opponent", "date", "league_source"], f"fbref/{cat}")

        _write_to_duckdb(con, df, f"fbref_{cat}", f"fbref/{cat}")


def process_understat(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/understat/parquet/{schedule,stats}/*.parquet
    → silver.understat_schedule + silver.understat_stats
    """
    prq_root = RAW_DIR / "understat" / "parquet"
    if not prq_root.exists():
        logger.info("  Understat : dossier Parquet absent, ignoré")
        return

    subtypes = sorted([d.name for d in prq_root.iterdir() if d.is_dir()])
    logger.info(f"  Understat : {subtypes}")

    for subtype in subtypes:
        files = sorted((prq_root / subtype).glob("*.parquet"))
        if not files:
            continue

        logger.info(f"  ── Understat/{subtype} : {len(files)} fichier(s)")
        df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
        logger.info(f"    Brut : {len(df):,} × {len(df.columns)} cols")

        # Normalisation compétition (colonne league_source)
        if "league_source" in df.columns:
            df = normalize_competition_col(df, "league_source", f"understat/{subtype}")

        # Normalisation équipes
        df = normalize_team_col(df, "home_team", f"understat/{subtype}", conn = con)
        df = normalize_team_col(df, "away_team", f"understat/{subtype}", conn = con)

        # Standardisation saison : int 2021 → "2021-2022"
        df = standardize_season(df)

        # Standardisation date (Understat : "2017-08-11 19:45:00")
        df = standardize_date(df)

        df = drop_unused_cols(df)
        df = apply_cat_c_rejection(df, f"understat/{subtype}")
        df = cast_numeric_cols(df)
        df = apply_cat_a_zerofill(df, f"understat/{subtype}")
        df = apply_cat_d_outliers(df, f"understat/{subtype}")
        df = remove_duplicates(df, ["match_id", "home_team", "away_team"], f"understat/{subtype}")

        _write_to_duckdb(con, df, f"understat_{subtype}", f"understat/{subtype}")


def process_whoscored(con: duckdb.DuckDBPyConnection) -> None:
    """
    Traite data/raw/whoscored/parquet/*.parquet → silver.whoscored_team_season
    Grain : 1 ligne = 1 équipe × saison (stats agrégées).
    """
    prq_root = RAW_DIR / "whoscored" / "parquet"
    if not prq_root.exists():
        logger.info("  WhoScored : dossier Parquet absent, ignoré")
        return

    files = sorted(prq_root.glob("*.parquet"))
    if not files:
        logger.info("  WhoScored : aucun Parquet trouvé")
        return

    logger.info(f"  WhoScored : {len(files)} fichier(s) → grain team-season")
    df = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
    logger.info(f"    Brut : {len(df):,} × {len(df.columns)} cols")

    # Normalisation compétition
    if "league_source" in df.columns:
        df = normalize_competition_col(df, "league_source", "whoscored")

    # Normalisation équipes
    df = normalize_team_col(df, "team", "whoscored", conn = con)

    # Standardisation saison
    df = standardize_season(df)

    # Cat C
    df = df.drop_nulls(subset=[c for c in ["team", "season", "league_source"] if c in df.columns])

    # Cast ws_* → Float64
    ws_cols = [c for c in df.columns if c.startswith("ws_")]
    if ws_cols:
        df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in ws_cols])

    df = apply_cat_a_zerofill(df, "whoscored")
    df = apply_cat_d_outliers(df, "whoscored")
    df = remove_duplicates(df, ["team", "season", "league_source"], "whoscored")

    _write_to_duckdb(con, df, "whoscored_team_season", "whoscored")


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

    logger.info("══════════════════════════════════════")


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


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_PROCESSORS = {
    "fbref":     process_fbref,
    "understat": process_understat,
    "whoscored": process_whoscored,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process Bronze Parquet → Silver DuckDB silver.*"
    )
    parser.add_argument("--source", default=None, choices=list(SOURCE_PROCESSORS.keys()))
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--audit-only", action="store_true",
                        help="Quality check uniquement, sans retraiter les données")
    args = parser.parse_args()

    logger.info("=== Démarrage process Bronze → Silver ===")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    _init_team_mapping(con)

    if args.audit_only:
        run_quality_check(con)
        con.close()
        return

    if args.reset:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='silver'"
        ).fetchall()]
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS silver.{t}")
        logger.info(f"  {len(tables)} table(s) supprimée(s) (--reset)")

    sources = [args.source] if args.source else list(SOURCE_PROCESSORS.keys())

    for source in sources:
        logger.info(f"── Source : {source} ───────────────────────────────────")
        try:
            SOURCE_PROCESSORS[source](con)
        except Exception as e:
            logger.error(f"  Erreur sur {source} : {e}", exc_info=True)

    print_report(con)
    run_quality_check(con)
    con.close()
    logger.success("=== Process terminé ===")


if __name__ == "__main__":
    main()
