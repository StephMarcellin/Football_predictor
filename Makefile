# ============================================================
#  Makefile — Projet 3-Étoiles
#  Prérequis Windows : winget install GnuWin32.Make
#  ou via chocolatey : choco install make
#
#  Usage :
#    make help          → liste toutes les commandes
#    make train         → lance l'étape d'entraînement
#    make pipeline      → lance le pipeline complet
# ============================================================

# Interpréteur Python — modifie si tu utilises un venv
PYTHON = python

# Chemin du script orchestrateur (relatif à la racine du projet)
PIPELINE = pipelines\run_pipeline.py

# ── Cible par défaut ──────────────────────────────────────
.DEFAULT_GOAL := help

# ============================================================
#  AIDE
# ============================================================
help:
	@echo.
	@echo  Projet 3-Étoiles — Commandes disponibles
	@echo  ==========================================
	@echo.
	@echo  Pipeline complet
	@echo    make pipeline          Lance toutes les etapes
	@echo    make pipeline-dry      Simule sans executer
	@echo    make pipeline-refresh  Relance avec dbt seed --full-refresh
	@echo.
	@echo  Etapes individuelles
	@echo    make ingest            Scraping Bronze (01_ingest.py)
	@echo    make odds              Cotes (01b_odds.py)
	@echo    make process           Silver layer (02_process.py)
	@echo    make dbt-seed          Initialise les seeds dbt
	@echo    make dbt-run           Execute les modeles dbt (Gold)
	@echo    make dbt-test          Valide les tests dbt
	@echo    make train             Entraine le modele
	@echo    make predict           Genere les predictions
	@echo    make backtest          Lance le backtest
	@echo.
	@echo  Depuis une etape
	@echo    make from-train        Reprend depuis train
	@echo    make from-predict      Reprend depuis predict
	@echo    make from-backtest     Reprend depuis backtest
	@echo.
	@echo  Outils
	@echo    make agent             Lance l'agent Gemini (interactif)
	@echo    make prefect-ui        Demarre le serveur Prefect
	@echo    make prefect-serve     Demarre le scheduler cron Prefect
	@echo    make mlflow-ui         Demarre l'interface MLflow
	@echo    make list-steps        Liste les etapes du pipeline
	@echo    make install           Installe les dependances pip
	@echo    make check-env         Verifie que .env existe
	@echo.

# ============================================================
#  PIPELINE COMPLET
# ============================================================
pipeline: check-env
	start /B prefect server start
	powershell -File tools\wait_for_prefect.ps1
	$(PYTHON) $(PIPELINE)

pipeline-dry: check-env
	$(PYTHON) $(PIPELINE) --dry-run

pipeline-refresh: check-env
	$(PYTHON) $(PIPELINE) --full-refresh

# ============================================================
#  ETAPES BRONZE / SILVER (lancées directement, pas via orchestrateur)
# ============================================================
ingest: check-env
	$(PYTHON) pipelines\01_ingest.py

odds: check-env
	$(PYTHON) pipelines\01b_odds.py

process: check-env
	$(PYTHON) pipelines\02_process.py

# ============================================================
#  ETAPES VIA L'ORCHESTRATEUR
# ============================================================
dbt-seed: check-env
	$(PYTHON) $(PIPELINE) --step dbt_seed

dbt-run: check-env
	$(PYTHON) $(PIPELINE) --step dbt_run

dbt-test: check-env
	$(PYTHON) $(PIPELINE) --step dbt_test

train: check-env
	$(PYTHON) $(PIPELINE) --step train

predict: check-env
	$(PYTHON) $(PIPELINE) --step predict

backtest: check-env
	$(PYTHON) $(PIPELINE) --step backtest

# ============================================================
#  REPRENDRE DEPUIS UNE ETAPE
# ============================================================
from-dbt_run: check-env
	$(PYTHON) $(PIPELINE) --from dbt_run

from-dbt_test: check-env
	$(PYTHON) $(PIPELINE) --from dbt_test	

from-dbt_test_check: check-env
	$(PYTHON) $(PIPELINE) --from dbt_test_check

from-train: check-env
	$(PYTHON) $(PIPELINE) --from train

from-predict: check-env
	$(PYTHON) $(PIPELINE) --from predict

from-backtest: check-env
	$(PYTHON) $(PIPELINE) --from backtest

# ============================================================
#  OUTILS
# ============================================================
agent: check-env
	$(PYTHON) pipelines\agent_gemini.py

prefect-ui:
	prefect server start

prefect-serve: check-env
	$(PYTHON) $(PIPELINE) --serve

mlflow-ui:
	mlflow ui --backend-store-uri mlruns

list-steps:
	$(PYTHON) $(PIPELINE) --list

install:
	pip install -r requirements.txt

# Docker
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d prefect mlflow

docker-down:
	docker-compose down

docker-train:
	docker-compose run pipeline python pipelines\run_pipeline.py --step train

docker-pipeline:
	docker-compose run pipeline python pipelines\run_pipeline.py

# DBT
dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve --port 8080
# ============================================================
#  VERIFICATION
# ============================================================
check-env:
	@if not exist .env (echo ERREUR : fichier .env manquant. Copie .env.example en .env et remplis les valeurs. && exit 1)

.PHONY: help pipeline pipeline-dry pipeline-refresh \
        ingest odds process \
        dbt-seed dbt-run dbt-test \
        train predict backtest \
        from-train from-predict from-backtest \
        agent prefect-ui prefect-serve mlflow-ui \
        list-steps install check-env