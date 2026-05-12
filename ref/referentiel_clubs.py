import pandas as pd
import re
import yaml
from pathlib import Path

"""Script pour consolider les données des clubs depuis les CSV de transfermarkt et les préparer pour dbt."""
# Lire tous les CSV
ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR /"config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

RAW_DIR = Path(CFG["paths"]["raw_data"])
csv_dir = ROOT_DIR / RAW_DIR / "transfermarkt" / "csv"

print(csv_dir)

def parse_market_value(val: str) -> float | None:
    """Parse '1,01 Mrd. €' → 1010.0, '884,25 mio. €' → 884.25"""
    if not isinstance(val, str):
        return None
    val = val.strip()
    # Extraire le nombre et l'unité
    match = re.search(r"([\d.,]+)\s*(Mrd|mio|Tsd)", val)
    if not match:
        return None
    number = float(match.group(1).replace(",", "."))
    unit = match.group(2)
    if unit == "Mrd":
        return round(number * 1000, 2)
    elif unit == "mio":
        return round(number, 2)
    elif unit == "Tsd":
        return round(number / 1000, 2)
    return None



dfs = []
for f in csv_dir.glob("*.csv"):
    df = pd.read_csv(f)
    dfs.append(df)

# Consolider
all_clubs = pd.concat(dfs, ignore_index=True)

# Parser market_value
all_clubs["market_value_m"] = all_clubs["market_value"].apply(parse_market_value)

# Garder les colonnes utiles
all_clubs = all_clubs[[
    "club_name", "club_tm_id", "league", "season",
    "market_value_m", "club_url"
]].drop_duplicates()

# Trier
all_clubs = all_clubs.sort_values(["league", "season", "club_name"])

print(f"Total lignes : {len(all_clubs)}")
print(f"Saisons : {sorted(all_clubs['season'].unique())}")
print(f"Ligues : {sorted(all_clubs['league'].unique())}")
print(all_clubs.head(5).to_string())

# Sauvegarder
out_path = ROOT_DIR / "dbt_project" / "seeds" / "transfermarkt_clubs.csv"
out_path.parent.mkdir(exist_ok=True)
all_clubs.to_csv(out_path, index=False)
print(f"\n✅ Sauvegardé : {out_path}")