"""
Package features — Feature Engineering Silver → Gold
=====================================================
Regroupe les 5 modules de feature engineering en un package cohérent.

Structure :
    features/
    ├── columns.py      — listes de colonnes partagées (NEW_COLS, DIFF_COLS)
    ├── rolling.py      — ex-03_features.py  (FBref/Understat/WhoScored rolling)
    ├── whoscored.py    — ex-03b             (WhoScored events → spatial features)
    ├── draw.py         — ex-03c × 2         (Draw Behavior + Signaux Nul/Victoire)
    └── sandbox.py      — ex-03c_suggested   (features candidates, read-only)

Expositions publiques :
    from features import run_rolling, run_whoscored, run_draw, run_sandbox
"""

from .rolling   import run_rolling_features   as run_rolling
from .whoscored import run_pipeline            as run_whoscored
from .draw      import run_pipeline            as run_draw
from .sandbox   import run                     as run_sandbox

__all__ = ["run_rolling", "run_whoscored", "run_draw", "run_sandbox"]
