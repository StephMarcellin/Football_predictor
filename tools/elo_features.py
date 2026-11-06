"""elo_features.py — export CSV des features Elo/classement pour les expériences (rolling_chunk --extra).
La logique vit dans pipelines/features/team_elo.py (même code qu'en production). Sortie : reports/chunks/elo_features.csv"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines" / "features"))
import team_elo  # noqa: E402
from team_elo import build, LEAGUE_CATS  # noqa: E402,F401  (réexportés pour tools/features_v2.py)

if __name__ == "__main__":
    F = team_elo.main(write=False)
    dest = ROOT / "reports" / "chunks" / "elo_features.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    F.to_csv(dest, index=False)
    print(f"{len(F):,} lignes -> {dest}")
