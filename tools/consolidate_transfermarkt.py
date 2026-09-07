"""
consolidate_transfermarkt.py — Consolide les CSV Transfermarkt bruts en un seed.

Rassemble tous les `data/raw/transfermarkt/csv/clubs_*.csv` dans
`dbt_project/seeds/transfermarkt_clubs.csv`
(format : club_name, club_tm_id, league, season, market_value_m, club_url).

Cette étape NE TOUCHE PAS team_mapping (alias / 'NEW' / team_id) — c'est le job
séparé de `tools/enrich_team_mapping.py`.

Garde-fous :
  • APPEND-ONLY : n'écrase jamais une ligne existante ; ajoute uniquement les
    (club_tm_id, league, season) absents du seed. Rien n'est perdu.
  • Backup .bak horodaté avant écriture.
  • --dry-run : liste ce qui serait ajouté, aucune écriture.
  • Valide les colonnes de chaque CSV brut ; log les valeurs marchandes non parsées.
  • Sortie triée (diffs git propres), idempotent.

Usage :
    python tools/consolidate_transfermarkt.py
    python tools/consolidate_transfermarkt.py --dry-run
"""
import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
import yaml
from loguru import logger

ROOT_DIR = next(p for p in Path(__file__).resolve().parents if (p / "config.yaml").exists())
CFG      = yaml.safe_load((ROOT_DIR / "config.yaml").read_text(encoding="utf-8"))
RAW_DIR  = ROOT_DIR / CFG["paths"]["raw_data"] / "transfermarkt" / "csv"
SEED_CSV = ROOT_DIR / "dbt_project" / "seeds" / "transfermarkt_clubs.csv"

RAW_COLS  = ["club_name", "club_url", "club_tm_id", "market_value", "league", "season", "scraped_at"]
SEED_COLS = ["club_name", "club_tm_id", "league", "season", "market_value_m", "club_url"]
KEY       = ["club_tm_id", "league", "season"]


def parse_value(s):
    """'86,18 mio. €' → 86.18 ; '1,20 Mrd. €' → 1200.0 ; '500 Tsd. €' → 0.5 ;
    vide / '-' → None. (FR : point = milliers, virgule = décimale.)"""
    if s is None:
        return None
    t = str(s).strip().lower()
    if t in ("", "-", "nan", "none"):
        return None
    m = re.search(r"[\d.,]+", t)
    if not m:
        return None
    num = m.group(0)
    num = num.replace(".", "").replace(",", ".") if "," in num else num
    try:
        val = float(num)
    except ValueError:
        return None
    if "mrd" in t:
        val *= 1000
    elif "tsd" in t:
        val *= 0.001
    return round(val, 2)


def read_raw() -> pl.DataFrame:
    """Concatène tous les clubs_*.csv, parse la valeur, réordonne au schéma seed."""
    files = sorted(RAW_DIR.glob("clubs_*.csv"))
    if not files:
        raise SystemExit(f"Aucun CSV dans {RAW_DIR}")
    frames = []
    for f in files:
        df = pl.read_csv(f, infer_schema_length=2000)
        missing = [c for c in RAW_COLS if c not in df.columns]
        if missing:
            raise SystemExit(f"{f.name} : colonnes manquantes {missing}")
        frames.append(df.select(RAW_COLS))
    raw = pl.concat(frames, how="vertical")
    logger.info(f"{len(files)} fichiers, {len(raw):,} lignes brutes")

    raw = raw.with_columns(
        pl.col("market_value").map_elements(parse_value, return_dtype=pl.Float64).alias("market_value_m")
    )
    bad = raw.filter(
        pl.col("market_value_m").is_null()
        & pl.col("market_value").is_not_null()
        & (pl.col("market_value").cast(pl.Utf8).str.strip_chars() != "")
    ).height
    if bad:
        logger.warning(f"{bad} valeurs marchandes non parsées → NULL")

    raw = raw.select(SEED_COLS).with_columns(pl.col("club_tm_id").cast(pl.Int64, strict=False))
    # filet : un club une seule fois par (tm_id, league, season)
    return raw.unique(subset=KEY, keep="first", maintain_order=True)


def main(dry_run: bool = False):
    raw = read_raw()

    if SEED_CSV.exists():
        existing = (pl.read_csv(SEED_CSV, infer_schema_length=2000)
                    .select(SEED_COLS)
                    .with_columns(pl.col("club_tm_id").cast(pl.Int64, strict=False)))
    else:
        existing = raw.head(0)

    # APPEND-ONLY : anti-join → seules les clés absentes de l'existant
    new_rows = raw.join(existing.select(KEY).unique(), on=KEY, how="anti")
    logger.info(f"existant : {len(existing):,} lignes | nouvelles : {len(new_rows):,}")

    if dry_run:
        for r in new_rows.head(40).iter_rows(named=True):
            logger.info(f"  + {r['club_name']:28} | {r['league']:22} | {r['season']}")
        logger.info(f"[DRY-RUN] {len(new_rows)} lignes seraient ajoutées (aucune écriture).")
        return

    if len(new_rows) == 0:
        logger.success("Rien de nouveau — seed déjà à jour ✅")
        return

    bak = SEED_CSV.with_name(f"{SEED_CSV.stem}.bak_{datetime.now():%Y%m%d_%H%M%S}.csv")
    shutil.copy2(SEED_CSV, bak)
    logger.info(f"Backup → {bak.name}")

    out = pl.concat([existing, new_rows], how="vertical").sort(["league", "season", "club_name"])
    out.write_csv(SEED_CSV)
    logger.success(f"{len(new_rows)} ajoutées → {SEED_CSV.name} ({len(out):,} lignes) ✅")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Consolide les CSV Transfermarkt en seed.")
    p.add_argument("--dry-run", action="store_true", help="Aperçu sans écrire")
    a = p.parse_args()
    main(dry_run=a.dry_run)
