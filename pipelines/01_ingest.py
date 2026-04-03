"""
Pipeline 01 — Ingest (HTML FBref → Parquet)
=============================================
Lit les fichiers HTML scrapés depuis FBref et les convertit en fichiers
Parquet partitionnés par catégorie : {club}_{saison}.parquet

Pattern attendu des fichiers HTML :
    matchlogs_{club}_{annee}_{categorie}.html
    ex: matchlogs_Brest_2122_shooting.html
        → club="Brest", saison="2021-2022", catégorie="shooting"

Architecture de sortie (Parquet) :
    data/raw/fbref/parquet/{categorie}/{club}_{saison}.parquet

Pourquoi Parquet ? Voir NOTE_PARQUET.md dans le répertoire racine.

Extensibilité via SOURCE_HANDLERS :
    Chaque source de données est un handler indépendant enregistré dans
    SOURCE_HANDLERS. Pour ajouter une source (API de cotes, CSV météo...),
    il suffit d'écrire une fonction parse_xxx() et de l'enregistrer.
    Le handler doit retourner un DataFrame ou None.

Usage :
    python pipelines/01_ingest.py                      # tous les HTML (incrémental)
    python pipelines/01_ingest.py --reset              # supprime et recharge tout
    python pipelines/01_ingest.py --file path/to/file  # fichier unique
"""

import re
import sys
import argparse
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from bs4 import BeautifulSoup
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

RAW_DIR  = Path(CFG["paths"]["raw_data"])   # data/raw/
HTML_DIR = RAW_DIR / "fbref" / "html"       # où Selenium dépose les HTML
PRQ_DIR  = RAW_DIR / "fbref" / "parquet"    # sortie Parquet

# ── Normalisation des noms de clubs ───────────────────────────────────────────
# Les noms dans les fichiers Selenium peuvent varier (tirets, accents...).
# Ce dictionnaire centralise la normalisation pour garantir des jointures
# cohérentes dans les pipelines suivants.
# À compléter au fur et à mesure que de nouveaux clubs apparaissent.
# Format : {"nom_dans_fichier": "nom_canonique"}
CLUB_NORMALIZE: dict = CFG.get("club_normalize", {})


# ── Registre des sources (SOURCE_HANDLERS) ────────────────────────────────────
# Chaque handler est une fonction avec la signature :
#   (path: Path, meta: dict) -> Optional[pd.DataFrame]
#
# Pour ajouter une nouvelle source :
#   1. Écrire une fonction parse_ma_source(path, meta) -> Optional[pd.DataFrame]
#   2. L'enregistrer dans SOURCE_HANDLERS avec une clé string unique
#   3. Mettre à jour parse_filename() pour détecter les fichiers de cette source
#
# Actuellement : seul "fbref" est implémenté.
# Prochaines sources prévues : "odds_api", "weather", ...
SOURCE_HANDLERS: dict = {}  # rempli après définition des fonctions

# ── Logs ──────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
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
    """
    Convertit le code annee du filename en saison lisible.
    "2122" -> "2021-2022", "2425" -> "2024-2025"
    """
    if len(code) == 4 and code.isdigit():
        y1 = int("20" + code[:2])
        y2 = int("20" + code[2:])
        return f"{y1}-{y2}"
    return code


def parse_filename(path: Path) -> Optional[dict]:
    """
    Extrait club, saison, catégorie depuis le nom de fichier.
    Pattern : matchlogs_{club}_{annee}_{categorie}.html
    Retourne None si le pattern ne correspond pas.
    """
    stem = path.stem  # sans extension
    m = re.match(r"matchlogs_(.+?)_(\d{4})_(.+)$", stem)
    if not m:
        logger.warning(f"  Pattern non reconnu : {path.name}")
        return None
    club, annee, categorie = m.group(1), m.group(2), m.group(3)
    # Normalisation du nom de club (mapping config.yaml → nom canonique)
    club_normalized = CLUB_NORMALIZE.get(club, club)
    if club_normalized != club:
        logger.debug(f"  Club normalisé : {club} → {club_normalized}")
    return {
        "club":      club_normalized,
        "season":    parse_season(annee),
        "categorie": categorie,
        "source":    "fbref",
    }


# ── Parseur HTML FBref ────────────────────────────────────────────────────────

def parse_fbref_html(path: Path, meta: dict) -> Optional[pd.DataFrame]:
    """
    Parse la table matchlogs_for d'un fichier HTML FBref.

    Technique : utilise l'attribut data-stat de chaque cellule comme nom
    de colonne. C'est plus robuste que le double header L0/L1 qui variait
    selon les exports CSV. Les noms sont directement sémantiques :
    "shots", "goals_for", "shots_on_target_pct", etc.

    Ignore les lignes thead répétées insérées par FBref tous les 20 matchs.
    Gère les cellules avec pénaltys au format "0 (12)" -> "0".
    """
    try:
        html = path.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table", {"id": "matchlogs_for"})
        if not table:
            logger.warning(f"  Table matchlogs_for absente : {path.name}")
            return None

        # Colonnes : dernière ligne du thead (contient les data-stat)
        header_row = table.find("thead").find_all("tr")[-1]
        cols = [th.get("data-stat") for th in header_row.find_all("th")]

        # Corps : ignorer les lignes d'en-tête répétées mid-table
        rows = []
        for tr in table.find("tbody").find_all("tr"):
            if "thead" in tr.get("class", []):
                continue
            row = {}
            for td in tr.find_all(["th", "td"]):
                stat = td.get("data-stat")
                if stat:
                    text = td.get_text(strip=True)
                    # Gérer les scores penalties "0 (13)" → "0"
                    m_pen = re.match(r"^(\d+)\s*\(\d+\)", text)
                    row[stat] = m_pen.group(1) if m_pen else text
            if row:
                rows.append(row)

        if not rows:
            logger.warning(f"  Aucune ligne extraite : {path.name}")
            return None

        valid_cols = [c for c in cols if c]
        df = pd.DataFrame(rows)[valid_cols] if valid_cols else pd.DataFrame(rows)

        # Métadonnées de provenance
        df["team"]          = meta["club"]
        df["season"]        = meta["season"]
        df["source"]        = meta["source"]
        df["stat_category"] = meta["categorie"]

        # Nettoyages de base : supprimer lignes sans date valide
        df = df[df["date"].notna() & (df["date"] != "")]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].reset_index(drop=True)

        logger.info(f"  {path.name} → {len(df)} lignes, {len(df.columns)} colonnes")
        return df

    except Exception as e:
        logger.error(f"  Erreur sur {path.name} : {e}")
        return None


# ── Enregistrement des handlers ───────────────────────────────────────────────
# On enregistre après définition des fonctions pour éviter les références
# circulaires. Pour ajouter une source : SOURCE_HANDLERS["ma_source"] = ma_fn
SOURCE_HANDLERS["fbref"] = parse_fbref_html


# ── Écriture Parquet ──────────────────────────────────────────────────────────

def write_parquet(df: pd.DataFrame, meta: dict) -> Path:
    """
    Écrit le DataFrame en Parquet (compression Snappy).
    Chemin : data/raw/fbref/parquet/{categorie}/{club}_{saison}.parquet

    Snappy est choisi pour son équilibre lecture rapide / compression correcte,
    ce qui correspond bien à l'usage avec DuckDB.
    """
    out_dir = PRQ_DIR / meta["categorie"]
    out_dir.mkdir(parents=True, exist_ok=True)

    season_safe = meta["season"].replace("-", "_")
    out_path    = out_dir / f"{meta['club']}_{season_safe}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")

    size_kb = out_path.stat().st_size / 1024
    logger.debug(f"  Écrit : {out_path.name} ({size_kb:.1f} Ko)")
    return out_path


# ── Traitement d'un fichier ───────────────────────────────────────────────────

def process_file(path: Path, force: bool = False) -> bool:
    """
    Traite un fichier HTML FBref et l'écrit en Parquet.

    Mode incrémental (défaut) : si le Parquet de destination existe déjà,
    le fichier est ignoré. Utilise --reset pour tout retraiter.
    """
    meta = parse_filename(path)
    if not meta:
        return False

    # Vérification incrémentale : skip si Parquet déjà présent
    if not force:
        season_safe = meta["season"].replace("-", "_")
        out_path    = PRQ_DIR / meta["categorie"] / f"{meta['club']}_{season_safe}.parquet"
        if out_path.exists():
            logger.debug(f"  Skip (déjà ingéré) : {path.name}")
            return True  # considéré comme succès

    # Sélectionner le bon handler selon la source
    handler = SOURCE_HANDLERS.get(meta["source"])
    if not handler:
        logger.error(f"  Handler inconnu pour source '{meta['source']}'")
        return False

    df = handler(path, meta)
    if df is None or df.empty:
        return False

    write_parquet(df, meta)
    return True


# ── Rapport final ─────────────────────────────────────────────────────────────

def print_report():
    """Affiche un résumé des fichiers Parquet produits."""
    if not PRQ_DIR.exists():
        return

    logger.info("── Rapport Parquet ──────────────────────────────────────")
    total_files = 0
    total_rows  = 0

    for cat_dir in sorted(PRQ_DIR.iterdir()):
        if not cat_dir.is_dir():
            continue
        files = list(cat_dir.glob("*.parquet"))
        if not files:
            continue
        n_rows = sum(pq.read_metadata(f).num_rows for f in files)
        logger.info(f"  {cat_dir.name:<25} {len(files):3d} fichiers  {n_rows:,} lignes")
        total_files += len(files)
        total_rows  += n_rows

    logger.info(f"  {'TOTAL':<25} {total_files:3d} fichiers  {total_rows:,} lignes")


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest HTML FBref → Parquet")
    parser.add_argument("--reset", action="store_true",
                        help="Supprime tous les Parquet existants avant de recharger")
    parser.add_argument("--file",  default=None,
                        help="Traiter un seul fichier HTML (chemin absolu ou relatif)")
    args = parser.parse_args()

    logger.info("=== Démarrage ingest ===")
    logger.info(f"  Source HTML : {HTML_DIR}")
    logger.info(f"  Sortie PRQ  : {PRQ_DIR}")

    # Reset si demandé
    if args.reset and PRQ_DIR.exists():
        shutil.rmtree(PRQ_DIR)
        logger.info("  Parquet existants supprimés (--reset)")

    PRQ_DIR.mkdir(parents=True, exist_ok=True)

    # Déterminer les fichiers à traiter
    if args.file:
        files = [Path(args.file)]
        logger.info(f"  Mode fichier unique : {args.file}")
    else:
        if not HTML_DIR.exists():
            logger.error(f"Dossier HTML introuvable : {HTML_DIR}")
            logger.info("Crée ce dossier et dépose tes fichiers HTML dedans.")
            sys.exit(1)
        files = sorted(HTML_DIR.rglob("matchlogs_*.html"))
        logger.info(f"  {len(files)} fichiers HTML trouvés")

    # Traitement
    force = args.reset  # si reset, on a déjà vidé le dossier mais on force quand même
    ok = err = skip = 0
    for path in files:
        result = process_file(path, force=force)
        if result:
            ok += 1
        else:
            err += 1

    logger.info(f"  Résultat : {ok} OK | {err} erreurs")
    print_report()
    logger.success("=== Ingest terminé ===")


if __name__ == "__main__":
    main()