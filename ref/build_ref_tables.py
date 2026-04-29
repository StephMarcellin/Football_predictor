"""
ref/build_ref_tables.py — Tables de référence WhoScored (event types & qualifiers)
====================================================================================
Crée et maintient deux tables de référence dans le schéma `ref` de DuckDB :

  ref.ws_event_types     — type_id / type_name des events WhoScored
                           Source : silver.stg_whoscored_events.type_id + type_name
                           Grain  : 1 ligne = 1 type d'event unique

  ref.ws_qualifier_types — qual_type_id / qual_display_name des qualifiers JSON
                           Source : silver.stg_whoscored_events.qualifiers_json
                           Grain  : 1 ligne = 1 type de qualifier unique
                           Méthode: json_extract par index fixe (0→N) — sans UNNEST

STRATÉGIE DE MISE À JOUR : INSERT OR IGNORE
  On n'écrase jamais les entrées existantes. Seuls les nouveaux IDs sont insérés.
  Cela permet d'enrichir les tables au fil des nouvelles données sans perdre
  les annotations manuelles éventuellement ajoutées.

DEUX ESPACES DISTINCTS — NE PAS CONFONDRE :
  type_id     → identifie le TYPE d'event   (ex: 1=Pass, 16=Goal, 7=Tackle)
  qual_type_id → identifie un ATTRIBUT      (ex: 23=Fast break, 26=Free kick)
  Un même event peut avoir plusieurs qualifiers simultanément dans qualifiers_json.

Appelable :
  python ref/build_ref_tables.py             # mise à jour incrémentale (défaut)
  python ref/build_ref_tables.py --reset     # recrée les tables depuis zéro
  python ref/build_ref_tables.py --show      # affiche les tables sans modifier
  python ref/build_ref_tables.py --lookup 23 # cherche un qualifier par ID
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent

os.chdir(ROOT_DIR)

with open(ROOT_DIR / "config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

DB_PATH = ROOT_DIR / CFG["paths"].get("duckdb", CFG["paths"].get("db", "db/football.duckdb"))

Path("logs").mkdir(exist_ok=True)
logger.add(
    "logs/ref_tables.log",
    level="DEBUG",
    encoding="utf-8",
    rotation="5 MB",
    retention=10,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | [ref] {message}",
)

# Nombre max de positions de qualifier à scanner par event
# La plupart des events ont < 15 qualifiers ; 20 couvre tous les cas observés.
MAX_QUALIFIER_POSITIONS = 20


# ══════════════════════════════════════════════════════════════════════════════
# CRÉATION DES TABLES
# ══════════════════════════════════════════════════════════════════════════════

SQL_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS ref"

SQL_CREATE_EVENT_TYPES = """
CREATE TABLE IF NOT EXISTS ref.ws_event_types (
    type_id        INTEGER PRIMARY KEY,
    type_name      VARCHAR NOT NULL,
    description    VARCHAR,          -- annotation manuelle optionnelle
    is_offensive   BOOLEAN,          -- flag : action offensive ?
    is_defensive   BOOLEAN,          -- flag : action défensive ?
    is_shot        BOOLEAN,          -- flag : tentative de but ?
    first_seen_at  TIMESTAMP DEFAULT current_timestamp,
    updated_at     TIMESTAMP DEFAULT current_timestamp
)
"""

SQL_CREATE_QUALIFIER_TYPES = """
CREATE TABLE IF NOT EXISTS ref.ws_qualifier_types (
    qual_type_id     INTEGER PRIMARY KEY,
    qual_display_name VARCHAR NOT NULL,
    description       VARCHAR,         -- annotation manuelle optionnelle
    category          VARCHAR,         -- ex: 'situation', 'body_part', 'zone', 'pass_type'
    first_seen_at     TIMESTAMP DEFAULT current_timestamp,
    updated_at        TIMESTAMP DEFAULT current_timestamp
)
"""


# ══════════════════════════════════════════════════════════════════════════════
# ALIMENTATION — Event types
# ══════════════════════════════════════════════════════════════════════════════

def build_event_types(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Insère les type_id/type_name manquants depuis stg_whoscored_events.
    Stratégie : INSERT OR IGNORE — ne touche pas aux entrées existantes.
    Retourne le nombre de nouveaux types insérés.
    """
    logger.info("  [event_types] Scan de silver.stg_whoscored_events...")

    df_new = conn.execute("""
        SELECT DISTINCT
            type_id,
            type_name
        FROM silver.stg_whoscored_events
        WHERE type_id IS NOT NULL
          AND type_name IS NOT NULL
        ORDER BY type_id
    """).df()

    if df_new.empty:
        logger.warning("  [event_types] Aucun event trouvé dans silver.stg_whoscored_events")
        return 0

    # Récupérer les IDs déjà connus
    existing = conn.execute("SELECT type_id FROM ref.ws_event_types").df()
    existing_ids = set(existing["type_id"].tolist()) if not existing.empty else set()

    df_insert = df_new[~df_new["type_id"].isin(existing_ids)]

    if df_insert.empty:
        logger.info(f"  [event_types] Aucun nouveau type — {len(existing_ids)} types déjà connus")
        return 0

    conn.register("_df_event_insert", df_insert)
    conn.execute("""
        INSERT INTO ref.ws_event_types (type_id, type_name)
        SELECT type_id, type_name FROM _df_event_insert
    """)
    conn.unregister("_df_event_insert")

    logger.success(f"  [event_types] {len(df_insert)} nouveaux types insérés")
    return len(df_insert)


# ══════════════════════════════════════════════════════════════════════════════
# ALIMENTATION — Qualifier types
# ══════════════════════════════════════════════════════════════════════════════

def _build_qualifier_scan_sql(max_pos: int) -> str:
    """
    Génère un SQL qui extrait les (qual_type_id, qual_display_name) distincts
    en scannant les positions 0 → max_pos-1 du tableau qualifiers_json.

    Contrainte sandbox : pas de UNNEST, pas de LATERAL.
    On utilise json_extract avec index fixe et UNION ALL.
    """
    unions = []
    for i in range(max_pos):
        unions.append(f"""
    SELECT
        TRY_CAST(json_extract_string(qualifiers_json, '$[{i}].type.value') AS INTEGER)
            AS qual_type_id,
        json_extract_string(qualifiers_json, '$[{i}].type.displayName')
            AS qual_display_name
    FROM silver.stg_whoscored_events
    WHERE qualifiers_json IS NOT NULL
      AND qualifiers_json != '[]'
      AND json_extract_string(qualifiers_json, '$[{i}].type.value') IS NOT NULL""")

    union_sql = "\n    UNION ALL".join(unions)

    return f"""
    WITH all_quals AS (
        {union_sql}
    )
    SELECT DISTINCT qual_type_id, qual_display_name
    FROM all_quals
    WHERE qual_type_id IS NOT NULL
      AND qual_display_name IS NOT NULL
    ORDER BY qual_type_id
    """


def build_qualifier_types(conn: duckdb.DuckDBPyConnection,
                          max_pos: int = MAX_QUALIFIER_POSITIONS) -> int:
    """
    Insère les qual_type_id/qual_display_name manquants en scannant
    qualifiers_json par position fixe (sans UNNEST).
    Retourne le nombre de nouveaux qualifiers insérés.
    """
    logger.info(f"  [qualifier_types] Scan qualifiers_json (positions 0→{max_pos - 1})...")

    scan_sql = _build_qualifier_scan_sql(max_pos)
    df_new = conn.execute(scan_sql).df()

    if df_new.empty:
        logger.warning("  [qualifier_types] Aucun qualifier trouvé")
        return 0

    logger.info(f"  [qualifier_types] {len(df_new)} types distincts trouvés dans les données")

    # Récupérer les IDs déjà connus
    existing = conn.execute("SELECT qual_type_id FROM ref.ws_qualifier_types").df()
    existing_ids = set(existing["qual_type_id"].tolist()) if not existing.empty else set()

    df_insert = df_new[~df_new["qual_type_id"].isin(existing_ids)]

    if df_insert.empty:
        logger.info(
            f"  [qualifier_types] Aucun nouveau qualifier — "
            f"{len(existing_ids)} qualifiers déjà connus"
        )
        return 0

    conn.register("_df_qual_insert", df_insert)
    conn.execute("""
        INSERT INTO ref.ws_qualifier_types (qual_type_id, qual_display_name)
        SELECT qual_type_id, qual_display_name FROM _df_qual_insert
    """)
    conn.unregister("_df_qual_insert")

    logger.success(f"  [qualifier_types] {len(df_insert)} nouveaux qualifiers insérés")
    return len(df_insert)


# ══════════════════════════════════════════════════════════════════════════════
# AFFICHAGE
# ══════════════════════════════════════════════════════════════════════════════

def show_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Affiche le contenu des deux tables de référence."""

    logger.info("\n══════ ref.ws_event_types ══════")
    df_evt = conn.execute("""
        SELECT type_id, type_name, is_offensive, is_defensive, is_shot, description
        FROM ref.ws_event_types
        ORDER BY type_id
    """).df()
    if df_evt.empty:
        logger.warning("  Table vide — lancer sans --show d'abord")
    else:
        logger.info(f"  {len(df_evt)} types d'events :\n{df_evt.to_string(index=False)}")

    logger.info("\n══════ ref.ws_qualifier_types ══════")
    df_qual = conn.execute("""
        SELECT qual_type_id, qual_display_name, category, description
        FROM ref.ws_qualifier_types
        ORDER BY qual_type_id
    """).df()
    if df_qual.empty:
        logger.warning("  Table vide — lancer sans --show d'abord")
    else:
        logger.info(f"  {len(df_qual)} types de qualifiers :\n{df_qual.to_string(index=False)}")


def lookup_qualifier(conn: duckdb.DuckDBPyConnection, qual_id: int) -> None:
    """Cherche un qualifier par son ID et affiche toutes ses infos."""
    df = conn.execute("""
        SELECT * FROM ref.ws_qualifier_types WHERE qual_type_id = ?
    """, [qual_id]).df()

    if df.empty:
        logger.warning(f"  Qualifier {qual_id} inconnu — pas encore vu dans les données")
        # Tentative de recherche directe dans les données brutes
        logger.info(f"  Recherche directe dans qualifiers_json...")
        found = False
        for i in range(MAX_QUALIFIER_POSITIONS):
            result = conn.execute(f"""
                SELECT DISTINCT
                    json_extract_string(qualifiers_json, '$[{i}].type.displayName') AS name
                FROM silver.stg_whoscored_events
                WHERE TRY_CAST(
                    json_extract_string(qualifiers_json, '$[{i}].type.value') AS INTEGER
                ) = {qual_id}
                LIMIT 1
            """).df()
            if not result.empty and result["name"].iloc[0] is not None:
                logger.info(f"  ✅ Trouvé à position [{i}] : {qual_id} = {result['name'].iloc[0]}")
                found = True
                break
        if not found:
            logger.warning(f"  ❌ Qualifier {qual_id} absent de toutes les données")
    else:
        logger.info(f"\n  Qualifier {qual_id} :\n{df.to_string(index=False)}")


# ══════════════════════════════════════════════════════════════════════════════
# RESET
# ══════════════════════════════════════════════════════════════════════════════

def reset_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Supprime et recrée les tables de référence depuis zéro."""
    logger.warning("  Reset des tables ref.ws_* — toutes les annotations manuelles seront perdues")
    conn.execute("DROP TABLE IF EXISTS ref.ws_event_types")
    conn.execute("DROP TABLE IF EXISTS ref.ws_qualifier_types")
    conn.execute(SQL_CREATE_EVENT_TYPES)
    conn.execute(SQL_CREATE_QUALIFIER_TYPES)
    logger.info("  Tables recréées vides")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run(reset: bool = False, show: bool = False, lookup: int | None = None) -> None:
    logger.info("═══ build_ref_tables — Référentiels WhoScored ═══")

    if not DB_PATH.exists():
        logger.error(f"DuckDB introuvable : {DB_PATH}")
        raise FileNotFoundError(DB_PATH)

    conn = duckdb.connect(str(DB_PATH))

    try:
        # Vérifier prérequis
        n_events = conn.execute(
            "SELECT COUNT(*) FROM silver.stg_whoscored_events"
        ).fetchone()[0]
        logger.info(f"  silver.stg_whoscored_events : {n_events:,} events")

        # Créer le schéma et les tables si nécessaire
        conn.execute(SQL_CREATE_SCHEMA)
        conn.execute(SQL_CREATE_EVENT_TYPES)
        conn.execute(SQL_CREATE_QUALIFIER_TYPES)

        if reset:
            reset_tables(conn)

        if show:
            show_tables(conn)
            return

        if lookup is not None:
            lookup_qualifier(conn, lookup)
            return

        # ── Mise à jour incrémentale ──────────────────────────────────────────
        logger.info("  Mise à jour incrémentale (INSERT OR IGNORE)...")

        n_evt  = build_event_types(conn)
        n_qual = build_qualifier_types(conn)

        # Rapport final
        total_evt  = conn.execute("SELECT COUNT(*) FROM ref.ws_event_types").fetchone()[0]
        total_qual = conn.execute("SELECT COUNT(*) FROM ref.ws_qualifier_types").fetchone()[0]

        logger.success(
            f"  ref.ws_event_types     : {total_evt} types "
            f"({n_evt} nouveaux)"
        )
        logger.success(
            f"  ref.ws_qualifier_types : {total_qual} qualifiers "
            f"({n_qual} nouveaux)"
        )

        if n_evt == 0 and n_qual == 0:
            logger.info("  ✅ Tables déjà à jour — aucune modification")
        else:
            logger.info(
                "  ℹ️  Pour annoter manuellement (description, flags) :\n"
                "     UPDATE ref.ws_event_types SET is_offensive=TRUE WHERE type_id IN (1,3,...);\n"
                "     UPDATE ref.ws_qualifier_types SET category='situation' WHERE qual_type_id=23;"
            )

    finally:
        conn.close()

    logger.success("═══ build_ref_tables terminé ═══")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tables de référence WhoScored (event types & qualifiers)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python ref/build_ref_tables.py                  # mise à jour incrémentale
  python ref/build_ref_tables.py --reset          # recrée depuis zéro
  python ref/build_ref_tables.py --show           # affiche les tables
  python ref/build_ref_tables.py --lookup 23      # cherche le qualifier 23
  python ref/build_ref_tables.py --lookup 1       # cherche le qualifier 1
        """
    )
    parser.add_argument("--reset",  action="store_true",
                        help="Recrée les tables depuis zéro (perd les annotations manuelles)")
    parser.add_argument("--show",   action="store_true",
                        help="Affiche les tables sans modifier")
    parser.add_argument("--lookup", type=int, default=None, metavar="ID",
                        help="Cherche un qualifier par son ID")
    args = parser.parse_args()

    run(reset=args.reset, show=args.show, lookup=args.lookup)