#!/usr/bin/env python3
"""
run_pipeline_main.py — Master orchestrator

Appelle les 4 orchestrateurs de phase en séquence :

1. run_scraping.py (PHASE 1) — ~4-5h
   └─ FBref, Understat, WhoScored, Transfermart, football-data

2. run_ingest.py (PHASE 2) — ~2-3h
   └─ Bronze → Silver (normalisation + team_mapping)

3. run_features_engineering.py (PHASE 3) — ~7h
   └─ Silver → intermediate.* + gold.* + machine_learning.*

4. run_ml.py (PHASE 4) — ~5-6h
   └─ Entraînement + prédictions + backtest

---

Total : ~18-24h pour run complet

Usage :
    make pipeline                    # run complet
    make from-scraping              # depuis scraping
    make from-ingest                # depuis ingest
    make from-features-engineering  # depuis features
    make from-ml                    # depuis ML
"""

if __name__ == "__main__":
    print("run_pipeline_main.py — Master orchestrator (à implémenter)")
    print("\nÉtapes :")
    print("1. run_scraping.py (PHASE 1)")
    print("2. run_ingest.py (PHASE 2)")
    print("3. run_features_engineering.py (PHASE 3)")
    print("4. run_ml.py (PHASE 4)")
