# ============================================================
#  Makefile — Projet 3-Étoiles
#  Prérequis Windows : winget install GnuWin32.Make
#  ou via chocolatey : choco install make
#
#  Usage :
#    make help          → liste toutes les commandes
#    make daily         → lance le pipeline complet en mode quotidien
#    make weekly        → lance le pipeline complet en mode hebdomadaire
#    make yearly        → lance le pipeline complet en mode annuel
# ============================================================

# Interpréteur Python — modifie si tu utilises un venv
PYTHON = python

# Chemins des orchestrateurs
MASTER    = pipelines\run_pipeline_main.py
SCRAPPING = pipelines\scrapping\run_scrapping.py
INGEST    = pipelines\ingest\run_ingest.py
FEATURES  = pipelines\run_features_engineering.py
ML        = pipelines\run_ml.py

# ── Cible par défaut ──────────────────────────────────────
.DEFAULT_GOAL := help

# ============================================================
#  AIDE
# ============================================================
help:
	@echo.
	@echo  Projet 3-Etoiles — Commandes disponibles
	@echo  ==========================================
	@echo.
	@echo  Master (3 flux de cadence)
	@echo    make daily             Flux quotidien
	@echo    make weekly            Flux hebdomadaire (daily + ml)
	@echo    make yearly            Flux annuel (refit features + ml)
	@echo    make daily-dry         Simule daily
	@echo    make weekly-dry        Simule weekly
	@echo    make yearly-dry        Simule yearly
	@echo    make master-list       Liste les flux et leur composition
	@echo.
	@echo  Master — scheduler Prefect Cloud (bloquant)
	@echo    make serve-daily       Deployment daily
	@echo    make serve-weekly      Deployment weekly
	@echo    make serve-yearly      Deployment yearly
	@echo.
	@echo  Orchestrateurs de phase (lancement manuel)
	@echo    make scrapping         run_scrapping.py complet
	@echo    make ingest            run_ingest.py complet
	@echo    make ingest-events     run_ingest.py --step whoscored_events
	@echo    make features          run_features_engineering.py complet
	@echo    make features-daily    run_features_engineering.py --flow daily
	@echo    make features-yearly   run_features_engineering.py --flow yearly
	@echo    make ml                run_ml.py
	@echo.
	@echo  Scrappers manuels (sources non orchestrées)
	@echo    make fbref             Scrape FBref
	@echo    make understat         Scrape Understat
	@echo.
	@echo  Outils
	@echo    make agent             Agent Gemini interactif
	@echo    make mlflow-ui         Interface MLflow (localhost:5000)
	@echo    make dbt-docs          Docs dbt (localhost:8080)
	@echo    make install           Installe les dependances pip
	@echo    make check-env         Verifie que .env existe
	@echo.
	@echo  Docker
	@echo    make docker-build      Build images
	@echo    make docker-up         Up prefect + mlflow
	@echo    make docker-down       Down services
	@echo.

# ============================================================
#  MASTER — Flux de cadence
# ============================================================
daily: check-env
	$(PYTHON) $(MASTER) --daily

daily-dry: check-env
	$(PYTHON) $(MASTER) --daily --dry-run

weekly: check-env
	$(PYTHON) $(MASTER) --weekly

weekly-dry: check-env
	$(PYTHON) $(MASTER) --weekly --dry-run

yearly: check-env
	$(PYTHON) $(MASTER) --yearly

yearly-dry: check-env
	$(PYTHON) $(MASTER) --yearly --dry-run

master-list: check-env
	$(PYTHON) $(MASTER) --list

# ============================================================
#  MASTER — Scheduler Prefect Cloud (bloquant)
# ============================================================
serve-daily: check-env
	$(PYTHON) $(MASTER) --daily --serve

serve-weekly: check-env
	$(PYTHON) $(MASTER) --weekly --serve

serve-yearly: check-env
	$(PYTHON) $(MASTER) --yearly --serve

# ============================================================
#  ORCHESTRATEURS DE PHASE (lancement manuel)
# ============================================================
scrapping: check-env
	$(PYTHON) $(SCRAPPING)

ingest: check-env
	$(PYTHON) $(INGEST)

ingest-events: check-env
	$(PYTHON) $(INGEST) --step whoscored_events

features: check-env
	$(PYTHON) $(FEATURES)

features-daily: check-env
	$(PYTHON) $(FEATURES) --flow daily

features-yearly: check-env
	$(PYTHON) $(FEATURES) --flow yearly

ml: check-env
	$(PYTHON) $(ML)

# ============================================================
#  SCRAPPERS MANUELS (sources non orchestrées)
# ============================================================
fbref: check-env
	$(PYTHON) pipelines\scrapping\team_stats\scrape_fbref.py

understat: check-env
	$(PYTHON) pipelines\scrapping\team_stats\scrape_understat.py

# ============================================================
#  OUTILS
# ============================================================
agent: check-env
	$(PYTHON) pipelines\agent_gemini.py

mlflow-ui:
	mlflow ui --backend-store-uri mlruns

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve --port 8080

install:
	pip install -r requirements.txt

# ============================================================
#  DOCKER
# ============================================================
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d prefect mlflow

docker-down:
	docker-compose down

# ============================================================
#  VERIFICATION
# ============================================================
check-env:
	@if not exist .env (echo ERREUR : fichier .env manquant. Copie .env.example en .env et remplis les valeurs. && exit 1)

.PHONY: help \
        daily daily-dry weekly weekly-dry yearly yearly-dry master-list \
        serve-daily serve-weekly serve-yearly \
        scrapping ingest ingest-events features features-daily features-yearly ml \
        fbref understat \
        agent mlflow-ui dbt-docs install \
        docker-build docker-up docker-down \
        check-env