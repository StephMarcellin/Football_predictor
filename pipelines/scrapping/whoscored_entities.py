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
