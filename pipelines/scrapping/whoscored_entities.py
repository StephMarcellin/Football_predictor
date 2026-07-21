"""
whoscored_entities.py — Extraction des entités WhoScored depuis le JSON brut
=============================================================================
Parse l'objet matchCentreData archivé (data/raw/whoscored/*.json.gz) pour en
tirer des tables silver au-delà des events :

Tables de RÉFÉRENCE (dimensions, dédupliquées sur tous les matchs) :
    silver.stg_whoscored_players_ref     player_id → player_name
    silver.stg_whoscored_formations_ref  formation_id → formation_name

Appelé par load_whoscored_archive.py, qui lit déjà les archives une par une.
On réutilise DB_PATH et le logger du scraper : une seule source de vérité.

Convention : on garde le team_id / player_id WhoScored bruts. La normalisation
vers les id canoniques du projet se fera dans la couche int_ dbt, comme pour
les events.
"""

import json

import duckdb
import pandas as pd

# Source unique du chemin DuckDB et du logger (défini dans le scraper).
from scrape_whoscored_details import DB_PATH
from loguru import logger


# ══════════════════════════════════════════════════════════════════════════════
# SCHÉMAS — tables de référence
# ══════════════════════════════════════════════════════════════════════════════

CREATE_PLAYERS_REF_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_players_ref (
    player_id   INTEGER PRIMARY KEY,
    player_name VARCHAR
);
"""

CREATE_FORMATIONS_REF_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_formations_ref (
    formation_id   INTEGER PRIMARY KEY,
    formation_name VARCHAR
);
"""


def init_ref_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Crée les tables de référence si elles n'existent pas."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute(CREATE_PLAYERS_REF_TABLE)
    conn.execute(CREATE_FORMATIONS_REF_TABLE)


# ══════════════════════════════════════════════════════════════════════════════
# COLLECTE — accumule les dimensions au fil des archives (en mémoire)
# ══════════════════════════════════════════════════════════════════════════════

def collect_player_names(data: dict, acc: dict) -> None:
    """
    Ajoute les paires (player_id → nom) de ce match dans l'accumulateur `acc`.
    Source : playerIdNameDictionary (clés = id sous forme de chaîne).
    Dédup naturelle : un même id écrase la valeur précédente (identique).
    """
    for pid, name in (data.get("playerIdNameDictionary") or {}).items():
        try:
            acc[int(pid)] = name
        except (ValueError, TypeError):
            continue


def collect_formations_ref(data: dict, acc: dict) -> None:
    """
    Ajoute les paires (formation_id → nom) vues dans ce match dans `acc`.
    Source : home/away.formations[].formationId / formationName.
    """
    for side in ("home", "away"):
        side_obj = data.get(side) or {}
        for fo in side_obj.get("formations", []) or []:
            fid = fo.get("formationId")
            fname = fo.get("formationName")
            if fid is None:
                continue
            try:
                acc[int(fid)] = str(fname) if fname is not None else None
            except (ValueError, TypeError):
                continue


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT — écrit les dimensions accumulées en une passe (fin de load)
# ══════════════════════════════════════════════════════════════════════════════

def upsert_players_ref(names: dict) -> int:
    """
    Upsert du dictionnaire {player_id: name} dans stg_whoscored_players_ref.
    ON CONFLICT DO UPDATE : ré-exécuter met simplement à jour les noms.
    Retourne le nombre de joueurs écrits.
    """
    if not names:
        return 0
    conn = duckdb.connect(str(DB_PATH))
    try:
        init_ref_tables(conn)
        df = pd.DataFrame(
            [{"player_id": k, "player_name": v} for k, v in names.items()]
        )
        conn.register("df_players_ref", df)
        conn.execute("""
            INSERT INTO silver.stg_whoscored_players_ref
                (player_id, player_name)
            SELECT player_id, player_name FROM df_players_ref
            ON CONFLICT (player_id) DO UPDATE SET player_name = excluded.player_name
        """)
        logger.info(f"  players_ref : {len(df)} joueurs upsertés")
        return len(df)
    finally:
        conn.close()


def upsert_formations_ref(formations: dict) -> int:
    """
    Upsert du catalogue {formation_id: name} dans stg_whoscored_formations_ref.
    Retourne le nombre de formations écrites.
    """
    if not formations:
        return 0
    conn = duckdb.connect(str(DB_PATH))
    try:
        init_ref_tables(conn)
        df = pd.DataFrame(
            [{"formation_id": k, "formation_name": v} for k, v in formations.items()]
        )
        conn.register("df_formations_ref", df)
        conn.execute("""
            INSERT INTO silver.stg_whoscored_formations_ref
                (formation_id, formation_name)
            SELECT formation_id, formation_name FROM df_formations_ref
            ON CONFLICT (formation_id) DO UPDATE
                SET formation_name = excluded.formation_name
        """)
        logger.info(f"  formations_ref : {len(df)} formations upsertées")
        return len(df)
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# SCHÉMAS — tables de FAIT (par match)
# ══════════════════════════════════════════════════════════════════════════════

CREATE_PLAYER_MATCH_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_player_match (
    ws_match_id         VARCHAR NOT NULL,
    team_id             INTEGER,   -- team_id WhoScored (normalisé plus tard en int_)
    player_id           INTEGER,
    shirt_no            INTEGER,
    position            VARCHAR,
    is_first_eleven     BOOLEAN,
    is_man_of_the_match BOOLEAN,
    height              INTEGER,
    weight              INTEGER,
    age                 INTEGER,
    rating              DOUBLE,    -- note WhoScored finale (valeur à la dernière minute)
    stats_json          VARCHAR,   -- stats brutes par minute — rien ne se perd
    PRIMARY KEY (ws_match_id, player_id)
);
"""

CREATE_FORMATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_formations (
    ws_match_id         VARCHAR NOT NULL,
    team_id             INTEGER,
    formation_seq       INTEGER,   -- index de la formation dans la liste de l'équipe
    formation_id        INTEGER,
    period              INTEGER,
    start_minute        INTEGER,   -- startMinuteExpanded (peut se répéter → hors clé)
    end_minute          INTEGER,   -- endMinuteExpanded
    captain_player_id   INTEGER,
    player_ids          VARCHAR,   -- JSON : ordre des joueurs sur la grille
    formation_positions VARCHAR,   -- JSON : coordonnées vertical/horizontal
    PRIMARY KEY (ws_match_id, team_id, formation_seq)
);
"""

CREATE_TEAM_MATCH_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_team_match (
    ws_match_id   VARCHAR NOT NULL,
    team_id       INTEGER,
    field         VARCHAR,   -- 'home' / 'away'
    manager_name  VARCHAR,
    country_name  VARCHAR,
    average_age   DOUBLE,
    stats_json    VARCHAR,   -- 35+ métriques équipe par minute — rien ne se perd
    PRIMARY KEY (ws_match_id, team_id)
);
"""

CREATE_MATCH_META_TABLE = """
CREATE TABLE IF NOT EXISTS silver.stg_whoscored_match_meta (
    ws_match_id   VARCHAR NOT NULL PRIMARY KEY,
    referee_id    INTEGER,
    referee_name  VARCHAR,
    venue_name    VARCHAR,
    attendance    INTEGER,
    weather_code  VARCHAR,
    kickoff_time  VARCHAR,   -- startTime (date + heure)
    ht_score      VARCHAR,
    ft_score      VARCHAR,
    et_score      VARCHAR,
    pk_score      VARCHAR
);
"""


def init_fact_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Crée les 4 tables de fait si elles n'existent pas."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # Migration : une ancienne table stg_whoscored_formations (PK sur start_minute,
    # sans formation_seq) est recréée avec le nouveau schéma. Ne s'exécute qu'une
    # fois — dès que formation_seq existe, plus aucun DROP.
    cols = [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='silver' AND table_name='stg_whoscored_formations'"
    ).fetchall()]
    if cols and "formation_seq" not in cols:
        conn.execute("DROP TABLE silver.stg_whoscored_formations")

    conn.execute(CREATE_PLAYER_MATCH_TABLE)
    conn.execute(CREATE_FORMATIONS_TABLE)
    conn.execute(CREATE_TEAM_MATCH_TABLE)
    conn.execute(CREATE_MATCH_META_TABLE)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _final_value(series):
    """
    Valeur d'une série {minute: valeur} à la minute la plus élevée.
    WhoScored indexe stats et ratings par minute ; la dernière minute donne
    la valeur de fin de match (note finale, cumul...). None si vide/invalide.
    """
    if not isinstance(series, dict) or not series:
        return None
    try:
        last_key = max(series.keys(), key=lambda k: int(k))
        return series[last_key]
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# PARSE — construit les lignes de chaque table de fait pour un match
# ══════════════════════════════════════════════════════════════════════════════

def parse_player_match(data: dict, ws_match_id: str) -> list:
    """1 ligne par joueur par match : identité + note + stats brutes (JSON)."""
    rows = []
    for side in ("home", "away"):
        side_obj = data.get(side) or {}
        team_id = side_obj.get("teamId")
        for p in side_obj.get("players", []) or []:
            stats = p.get("stats") or {}
            rows.append({
                "ws_match_id":         ws_match_id,
                "team_id":             team_id,
                "player_id":           p.get("playerId"),
                "shirt_no":            p.get("shirtNo"),
                "position":            p.get("position"),
                "is_first_eleven":     p.get("isFirstEleven"),
                "is_man_of_the_match": p.get("isManOfTheMatch"),
                "height":              p.get("height"),
                "weight":              p.get("weight"),
                "age":                 p.get("age"),
                "rating":              _safe_float(_final_value(stats.get("ratings"))),
                "stats_json":          json.dumps(stats, ensure_ascii=False),
            })
    return rows


def parse_formations(data: dict, ws_match_id: str) -> list:
    """1 ligne par période de formation par équipe (timeline tactique)."""
    rows = []
    for side in ("home", "away"):
        side_obj = data.get(side) or {}
        team_id = side_obj.get("teamId")
        for seq, fo in enumerate(side_obj.get("formations", []) or []):
            rows.append({
                "ws_match_id":         ws_match_id,
                "team_id":             team_id,
                "formation_seq":       seq,
                "formation_id":        fo.get("formationId"),
                "period":              fo.get("period"),
                "start_minute":        fo.get("startMinuteExpanded"),
                "end_minute":          fo.get("endMinuteExpanded"),
                "captain_player_id":   fo.get("captainPlayerId"),
                "player_ids":          json.dumps(fo.get("playerIds", []), ensure_ascii=False),
                "formation_positions": json.dumps(fo.get("formationPositions", []), ensure_ascii=False),
            })
    return rows


def parse_team_match(data: dict, ws_match_id: str) -> list:
    """1 ligne par équipe par match : contexte + stats équipe brutes (JSON)."""
    rows = []
    for side in ("home", "away"):
        side_obj = data.get(side) or {}
        rows.append({
            "ws_match_id":  ws_match_id,
            "team_id":      side_obj.get("teamId"),
            "field":        side_obj.get("field"),
            "manager_name": side_obj.get("managerName"),
            "country_name": side_obj.get("countryName"),
            "average_age":  _safe_float(side_obj.get("averageAge")),
            "stats_json":   json.dumps(side_obj.get("stats") or {}, ensure_ascii=False),
        })
    return rows


def parse_match_meta(data: dict, ws_match_id: str) -> list:
    """1 ligne par match : arbitre, stade, affluence, scores. (liste d'1 élément)"""
    ref = data.get("referee") or {}
    return [{
        "ws_match_id":  ws_match_id,
        "referee_id":   ref.get("officialId"),
        "referee_name": ref.get("name"),
        "venue_name":   data.get("venueName"),
        "attendance":   data.get("attendance"),
        "weather_code": data.get("weatherCode"),
        "kickoff_time": data.get("startTime"),
        "ht_score":     data.get("htScore"),
        "ft_score":     data.get("ftScore"),
        "et_score":     data.get("etScore"),
        "pk_score":     data.get("pkScore"),
    }]


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT — écrit les 4 tables de fait d'un match (une seule connexion)
# ══════════════════════════════════════════════════════════════════════════════

def _write_fact(conn, table: str, rows: list,
                int_cols=(), float_cols=(), bool_cols=()) -> None:
    """
    DELETE des lignes du match + INSERT des nouvelles (idempotent).
    Insertion par colonnes explicites → robuste aux colonnes ajoutées plus tard.
    """
    if not rows:
        return
    ws_id = rows[0]["ws_match_id"]
    conn.execute(f"DELETE FROM silver.{table} WHERE ws_match_id = ?", [ws_id])

    df = pd.DataFrame(rows)
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].astype("boolean")

    cols = ", ".join(df.columns.tolist())
    conn.register("df_fact", df)
    conn.execute(f"INSERT INTO silver.{table} ({cols}) SELECT {cols} FROM df_fact")
    conn.unregister("df_fact")


def upsert_match_facts(data: dict, ws_match_id: str) -> None:
    """
    Écrit les 4 tables de fait d'un match en une seule connexion DuckDB.
    Idempotent : recharger un match remplace proprement ses lignes.
    """
    conn = duckdb.connect(str(DB_PATH))
    try:
        init_fact_tables(conn)
        _write_fact(
            conn, "stg_whoscored_player_match", parse_player_match(data, ws_match_id),
            int_cols=("team_id", "player_id", "shirt_no", "height", "weight", "age"),
            float_cols=("rating",),
            bool_cols=("is_first_eleven", "is_man_of_the_match"),
        )
        _write_fact(
            conn, "stg_whoscored_formations", parse_formations(data, ws_match_id),
            int_cols=("team_id", "formation_seq", "formation_id", "period",
                      "start_minute", "end_minute", "captain_player_id"),
        )
        _write_fact(
            conn, "stg_whoscored_team_match", parse_team_match(data, ws_match_id),
            int_cols=("team_id",),
            float_cols=("average_age",),
        )
        _write_fact(
            conn, "stg_whoscored_match_meta", parse_match_meta(data, ws_match_id),
            int_cols=("referee_id", "attendance"),
        )
    finally:
        conn.close()
