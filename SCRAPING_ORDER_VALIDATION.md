# Validation : Ordre du Scraping

## CE QUE TU PROPOSES

### Étape 1 : Scraping Transfermarket + Team Mapping + Seeds
1. **scrape_transfermarket.py** → récupère noms clubs par année/ligue → CSV individuels
2. **referentiel_clubs.py** → agrège CSVs → `dbt_project/seeds/transfermarkt_clubs.csv`
3. **enrich_team_mapping.py** → ajoute alias='NEW' → `dbt_project/seeds/team_mapping.csv`
4. **(MANUEL)** corriger les alias 'NEW' dans team_mapping.csv
5. **dbt seed** → charge seeds dans DuckDB

### Étape 2 : Scraping Global
1. **WhoScored Events** (grain match)
   - scrape_whoscored_match.py → URLs
   - scrape_whoscored_details.py → scrape données
2. **Team Stats** (grain année)
   - scrape_fbref.py
   - scrape_understat.py
   - scrape_whoscored.py

---

## CE QUI EXISTE RÉELLEMENT

### Scripts trouvés
- ✓ `pipelines/scrapping/team_stats/scrape_transfermarkt.py` (existe)
- ✓ `ref/referentiel_clubs.py` (existe, consolide les CSV)
- ✓ `tools/consolidate_transfermarkt.py` (existe aussi, alternative plus sûre)
- ✓ `tools/enrich_team_mapping.py` (existe)
- ✓ `pipelines/scrapping/events/scrape_whoscored_match.py` (existe)
- ✓ `pipelines/scrapping/events/scrape_whoscored_details.py` (existe)
- ✓ `pipelines/scrapping/team_stats/scrape_fbref.py` (existe)
- ✓ `pipelines/scrapping/team_stats/scrape_understat.py` (existe)
- ✓ `pipelines/scrapping/team_stats/scrape_whoscored.py` (existe)
- ✓ `pipelines/scrapping/run_scrapping.py` (orchestrateur)

### Flux ACTUEL dans run_pipeline.py
```
1. dbt_seed                   ← charge CSVs seeds dans DuckDB
2. ingest (01_ingest.py)      ← lit team_mapping DEPUIS DuckDB (de la seed)
3. odds (01b_odds.py)         ← scrape cotes
4. process_events.py          ← normalise events
5. process_team_stats.py      ← normalise team stats
```

### Divergence critique
**À AUCUN MOMENT**, run_pipeline.py n'appelle:
- scrape_transfermarkt.py
- referentiel_clubs.py
- consolidate_transfermarkt.py
- enrich_team_mapping.py

Ces scripts sont **totalement offline** du pipeline principal. Ils ne sont lancés qu'**une fois** ou **manuellement** pour **préparer** les seeds.

---

## CLARIFICATION : Deux processus distincts

### Processus 1 : SETUP INITIAL (une fois, offline)
**Objectif** : générer les seeds (`transfermarkt_clubs.csv`, `team_mapping.csv`)

```bash
# 1. Scraper Transfermarket pour TOUTES les saisons/ligues
python pipelines/scrapping/team_stats/scrape_transfermarkt.py

# 2. Consolider les CSVs bruts en seed
python ref/referentiel_clubs.py
# OU (plus sûr, avec backup)
python tools/consolidate_transfermarkt.py

# 3. Enrichir le team_mapping avec les clubs trouvés
python tools/enrich_team_mapping.py

# 4. MANUEL : Corriger team_mapping.csv pour tous les alias='NEW'
# Éditer dbt_project/seeds/team_mapping.csv à la main

# 5. (Optionnel) Vérifier les fichiers seeds
# dbt_project/seeds/transfermarkt_clubs.csv ✓
# dbt_project/seeds/team_mapping.csv ✓ (avec alias corrigés)
```

**Cela ne se lance qu'UNE FOIS** (ou très rarement si nouveaux clubs/saisons).

### Processus 2 : PIPELINE RÉGULIER (hebdo/daily, automatisé)
**Objectif** : scraper données fraîches, normaliser, entraîner, prédire

```bash
# Suppose que les seeds (transfermarkt_clubs.csv, team_mapping.csv) existent déjà
python run_pipeline.py
# ou
make pipeline
```

Ordre exact :
```
dbt_seed 
  → ingest (01_ingest.py : FBref, Understat, WhoScored HTML)
  → odds (01b_odds.py : cotes)
  → process_events.py
  → process_team_stats.py
  → validate_silver
  → dbt_run (Gold)
  → xT, PSxG (features Python)
  → validate_gold
  → train
  → predict
  → backtest
  → agent_analysis
```

---

## POINTS DE CLARIFICATION

### 1. Quand enrich_team_mapping.py doit s'exécuter ?
- **Uniquement si** nouveau club trouvé en scrape Transfermarket
- **Pas à chaque pipeline** (c'est hors ligne)
- **Avant** dbt_seed (pour que les alias soient corrects quand on charge en base)

### 2. Quand referentiel_clubs.py vs consolidate_transfermarkt.py ?
Deux alternatives pour le même job (consolidation). À choisir :
- **referentiel_clubs.py** : simple, pas de garde-fous
- **consolidate_transfermarkt.py** : APPEND-ONLY, backup .bak, --dry-run → **à préférer**

### 3. Dépendance cachée entre scrape_transfermarkt.py et enrich_team_mapping.py
```
scrape_transfermarkt.py (génère raw_data/transfermarkt/csv/*.csv)
  ↓
consolidate_transfermarkt.py (lit *.csv, écrit seed transfermarkt_clubs.csv)
  ↓
enrich_team_mapping.py (lit transfermarkt_clubs.csv, enrichit team_mapping.csv)
  ↓
(MANUEL) Corriger team_mapping.csv
  ↓
dbt seed (charge seeds en DuckDB)
  ↓
run_pipeline.py → ingest (lit team_mapping depuis DuckDB)
```

### 4. Flux WhoScored : scrape_whoscored_match.py + scrape_whoscored_details.py
Ces deux scripts sont **DIFFÉRENTS** de l'orchestrateur run_scrapping.py qui tourne la nuit.
- **run_scrapping.py** : appelle scrape_whoscored_details.py (détails de matchs existants)
- **scrape_whoscored_match.py** : récupère les URLs des matchs (pas appelé dans run_scrapping.py)

**Questions** : 
- scrape_whoscored_match.py est-il supposé s'exécuter une fois au démarrage ?
- Ou une fois par saison ?
- Ou jamais (les URLs étant déjà en base) ?

---

## RÉSUMÉ

✓ **TON ORDRE EST CORRECT** pour le setup initial (Transfermarket → team_mapping → seeds → pipeline)

✗ **MAIS** il n'est PAS intégré dans run_pipeline.py — c'est un processus hors ligne qui ne s'exécute qu'une fois (ou manuellement) pour **préparer** les seeds.

**L'ordre que run_pipeline.py ASSUME** :
1. Seeds (transfermarkt_clubs.csv, team_mapping.csv) existent déjà
2. dbt seed charge ces seeds en DuckDB
3. ingest normalise avec team_mapping depuis DuckDB
4. Puis suite du pipeline (odds, process, features, ML)

---

## Prochaines étapes
- [ ] Valider si scrape_whoscored_match.py doit être intégré quelque part
- [ ] Valider si consolidate_transfermarkt.py (vs referentiel_clubs.py) est le bon choix
- [ ] Créer un script d'orchestration pour le **setup initial** (separate de run_pipeline.py)
- [ ] Documenter la cadence : une fois ? par saison ? manuel ?
