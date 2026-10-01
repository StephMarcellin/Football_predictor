# Dossier `ingest/` — Qui fait quoi

## 4 scripts + 1 orchestrateur

### 1️⃣ `01_ingest.py` — HTML/CSV → Parquet (Bronze)
**Rôle** : Convertit les données brutes scrappées en Parquet.

**Input** :
- FBref HTML matchlogs → `data/raw/fbref/parquet/`
- Understat CSV schedule/stats → `data/raw/understat/parquet/`
- WhoScored HTML → `data/raw/whoscored/parquet/`

**Output** :
- Parquet files dans `data/raw/{source}/parquet/{categorie}/`

**Ce qu'il fait** :
- Lit HTML/CSV bruts depuis le système de fichiers
- Utilise Polars lazy execution (multi-threading)
- Normalise les noms de colonnes
- Ajoute traçabilité (source, scraped_at)
- Charge team_mapping depuis DuckDB pour normaliser les équipes
- Mode incrémental (skip si exist) ou --reset

---

### 2️⃣ `01b_odds.py` — Cotes → Silver.odds
**Rôle** : Charge les cotes et calcule probabilités implicites.

**Input** :
- CSV football-data.co.uk → `data/raw/bets/`

**Output** :
- `silver.odds` table dans DuckDB

**Ce qu'il fait** :
- Lit CSVs (format : `{LEAGUE_SLUG}_{SEASON_CODE}.csv`)
- Normalise les noms d'équipes (team_mapping de DuckDB)
- Mappe les ligues (ENG-Premier League → Premier League)
- Calcule probabilités implicites Pinnacle + Average
- Upsert dans silver.odds

---

### 3️⃣ `process_events.py` — WhoScored Events → Silver + match_registry
**Rôle** : Normalise les index de matchs WhoScored et enregistre les matchs.

**Input** :
- `silver.stg_whoscored_match_index` (écrit par scraper events)

**Output** :
- `silver.stg_whoscored_match_index` (réécrite + normalisée)
- `intermediate.match_registry` (grain événements, appended)

**Ce qu'il fait** :
- Normalise les noms de compétitions (league_source)
- Normalise les noms d'équipes (home_team_name, away_team_name)
- Standardise les saisons
- Enregistre les matchs dans match_registry (pour tracking)

---

### 4️⃣ `process_team_stats.py` — FBref/Understat/WhoScored → Silver
**Rôle** : Normalise les stats par équipe (3 sources).

**Input** :
- FBref Parquets → `data/raw/fbref/parquet/{cat}/*.parquet`
- Understat Parquets → `data/raw/understat/parquet/{type}/*.parquet`
- WhoScored Parquets → `data/raw/whoscored/parquet/*.parquet`

**Output** :
- `silver.fbref_{cat}` tables (shooting, keeper, misc, schedule…)
- `silver.understat_*` tables
- `silver.whoscored_*` tables
- `intermediate.match_registry` (appended)

**Ce qu'il fait** :
- Lit chaque source en Parquet
- Renomme colonnes (data-stat → noms canoniques)
- Normalise compétitions + équipes
- Standardise dates, saisons
- Validation : zerofill, outliers detection, dedup
- Enregistre les matchs dans match_registry (pour tracking)

---

### 5️⃣ `run_ingest.py` — Orchestrateur
**Rôle** : Lance les 4 étapes dans l'ordre.

**Ordre d'exécution** :
```
1. ingest           (01_ingest.py)
2. odds             (01b_odds.py)
3. process_events   (process_events.py)
4. process_team_stats (process_team_stats.py)
```

**Usage** :
```bash
# Toutes les étapes
python pipelines/ingest/run_ingest.py

# Une seule étape
python pipelines/ingest/run_ingest.py --step odds

# Simulation
python pipelines/ingest/run_ingest.py --dry-run
```

---

## DÉPENDANCES ET ORDRE

### Dépendances entre scripts

```
Scraping (run_scrapping.py)  ← scrappe WhoScored events (JSON archivés)
              ↓
load_archive  ← charge JSON en silver.stg_whoscored_match_index

                ↓
    ┌───────────┴──────────────────────────────┐
    ↓                                            ↓
01_ingest.py                            01b_odds.py
(FBref, Understat,                    (CSV cotes)
 WhoScored HTML → Parquet)                 ↓
    ↓                              silver.odds (DuckDB)
Parquets (Bronze)
    ↓
process_events.py ─────┐
(normalise index)       │
                        ├─→ match_registry (intermédiaire, tracking)
process_team_stats.py ──┤
(normalise stats)       │
                        └─→ silver.fbref_*, silver.understat_*, silver.whoscored_*

                        ↓
                    dbt_run (Gold)
```

### Dépendances critiques

1. **Scraping doit avoir lieu avant ingest**
   - `run_scrapping.py` (events WhoScored) doit avoir tourné
   - `01_ingest.py` suppose que les HTML/CSV bruts existent

2. **odds peut tourner en parallèle avec ingest**
   - `01b_odds.py` lit des CSV à côté
   - Aucune dépendance avec ingest

3. **process_events APRÈS ingest**
   - Lit stg_whoscored_match_index (écrit par run_scrapping)
   - Normalise et enrichit

4. **process_team_stats APRÈS ingest**
   - Lit les Parquets générés par 01_ingest.py
   - Normalise et valide

---

## APPELÉ PAR `run_pipeline.py`

Dans run_pipeline.py, chaque étape du ingest/ est appelée séparément :

```python
"ingest": {
    "fn": mod_01.main,      # 01_ingest.main()
    "kwargs": {},
    "critical": True,
},
"odds": {
    "fn": mod_01b.main,     # 01b_odds.main()
    "kwargs": {},
    "critical": True,
},
"process_events": {
    "fn": mod_pe.main,      # process_events.main()
    "kwargs": {},
    "critical": True,
},
"process_team_stats": {
    "fn": mod_pts.main,     # process_team_stats.main()
    "kwargs": {},
    "critical": True,
},
```

**Note** : `run_ingest.py` n'est PAS appelé par run_pipeline. C'est un orchestrateur **séparé** (utile si on veut relancer le ingest seul).

---

## RÉSUMÉ RAPIDE

| Script | Input | Output | Rôle |
|--------|-------|--------|------|
| **01_ingest.py** | HTML/CSV bruts | Parquets (Bronze) | Conversion format |
| **01b_odds.py** | CSV cotes | silver.odds | Charger cotes |
| **process_events.py** | stg_whoscored_match_index | silver.* + match_registry | Normaliser events |
| **process_team_stats.py** | Parquets Bronze | silver.fbref_*, silver.understat_*, silver.whoscored_* | Normaliser stats |
| **run_ingest.py** | — | — | Orchestrer les 4 au-dessus |

