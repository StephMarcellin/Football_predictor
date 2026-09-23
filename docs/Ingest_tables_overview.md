# `run_ingest.py` — Tables produites (Bronze → Silver)

Analyse de `pipelines/ingest/run_ingest.py` et des scripts qu'il appelle. Basée sur lecture du code (`process_common.py` + les 4 scripts `ingest_*.py`), pas sur une requête live de `football_dev.duckdb` (fichier verrouillé au moment de l'analyse — probablement une connexion active côté Windows).

## 1. Ce que fait le pipeline

`run_ingest.py` enchaîne 8 étapes dans cet ordre :

| # | Étape | Script | Critique |
|---|---|---|---|
| 1 | `ingest` | `ingest_bronze.py` | oui |
| 2 | `odds` | `01b_odds.py` | **désactivée** (commentée dans `build_steps()`) |
| 3 | `check_adu_pre` | `process_common.check_adu_pre` | oui — bloque si des ADU résiduels traînent déjà |
| 4 | `whoscored_events` | `ingest_whoscored_events.py` | oui |
| 5 | `fbref` | `ingest_fbref.py` | oui |
| 6 | `understat` | `ingest_understat.py` | oui (dépend de `fbref` terminé avant) |
| 7 | `whoscored_team_stats` | `ingest_whoscored_team_stats.py` | oui |
| 8 | `check_adu_post` | `process_common.check_adu_post` | oui — bloque si de nouveaux ADU sont apparus pendant le run |

Deux points à noter, pas alignés avec `pipelines/ingest/README.md` actuel :
- **`odds` est désactivée** dans le code (`# "odds": {...}` commenté) alors que le README la présente comme une étape active qui écrit `silver.odds`. Si elle tourne un jour, elle écrit les cotes football-data.co.uk + probabilités implicites.
- Le README mentionne encore `01_ingest.py`, `process_events.py`, `process_team_stats.py` et un préalable `run_scrapping.py --step load_archive` — ces noms ont été remplacés (`ingest_bronze.py`, `ingest_whoscored_events.py`, `ingest_fbref.py` / `ingest_understat.py` / `ingest_whoscored_team_stats.py`). À mettre à jour si tu veux garder le README fiable.

`ingest_bronze.py` (étape 1) n'écrit **aucune table DuckDB** : il transforme le HTML/CSV brut en fichiers **Parquet Bronze** sur disque (`data/raw/{fbref,understat,whoscored}/parquet/...`). C'est uniquement à partir de l'étape 4 (whoscored_events) que des tables DuckDB apparaissent.

`check_adu_pre`/`check_adu_post` ne créent rien : ce sont des gates qualité qui lisent `referentiel.team_mapping` et lèvent une exception si des équipes Big5/D2 n'ont pas été résolues par le fuzzy matching (`club_name = 'ADU'`).

## 2. Tables effectivement créées/écrites par ce pipeline

Chacune des tables `silver.*` ci-dessous est **entièrement remplacée** à chaque run (`DROP TABLE` puis `CREATE TABLE ... AS SELECT * FROM arrow_table` dans `_write_to_duckdb()`) — pas d'append, donc idempotent mais pas incrémental au sens dbt.

### `referentiel.team_mapping`
Alimentée en continu par les 4 scripts silver via `normalize_team_col()` (fuzzy matching sur Transfermarkt) + flush en fin de script (`_flush_team_mapping`). C'est le référentiel pivot alias brut → nom canonique / `team_id`.
- Colonnes : `alias` (PK), `club_name`, `team_id`
- `team_id` porte des sentinelles : `-1` = ADU (Big5/D2 non résolu, bloque le pipeline), `-2` = CE (club européen hors périmètre Transfermarkt), `0` = Minor Club (non persisté)

### `intermediate.match_registry`
Registre pivot de tous les matchs vus, toutes sources confondues. Alimenté par `upsert_match_registry()` appelé depuis `whoscored_events`, `fbref` et `understat` (pas `whoscored_team_stats`, qui est au grain équipe-saison, pas match).
- Colonnes : `match_id` (PK, SHA1 sur date+équipes+ligue+saison), `match_date`, `home_team_id`, `away_team_id`, `league_source`, `season`
- Sert de clé de jointure commune entre les sources en aval (dbt gold)

### `silver.stg_whoscored_match_index`
Cas particulier : cette table n'est **pas créée** par `run_ingest.py` — elle est écrite en amont par le scraper WhoScored (hors scope de ce pipeline). `ingest_whoscored_events.py` la **lit, normalise (équipes + compétition + saison) et la réécrit** (même table, mêmes colonnes, valeurs nettoyées), en parallèle d'un upsert dans `match_registry`.

### `silver.fbref_schedule`, `silver.fbref_shooting`, `silver.fbref_keeper`, `silver.fbref_misc`
Une table par catégorie de statistiques FBref (dossiers sous `data/raw/fbref/parquet/{cat}/`), grain **1 ligne = 1 match du point de vue d'un club**.
- `fbref_schedule` : résultat du match, date, venue, compétition, adversaire
- `fbref_shooting` : tirs (`standard_sh`, `standard_sot`, `standard_g_sh`, ...)
- `fbref_keeper` : stats gardien (`sota`, `ga_keeper`, `saves`, `save_pct`, `cs`, `pk_att/allowed/saved/missed`)
- `fbref_misc` : cartons, fautes, hors-jeu, interceptions, tacles gagnés, possession, pens gagnés/concédés, csc

Toutes les 4 passent par la même chaîne de validation : normalisation compétition/équipes, standardisation date/saison, cast numérique, dédoublonnage sur `(team, opponent, date, league_source)`.

### `silver.understat_schedule`, `silver.understat_stats`
Grain 1 ligne = 1 match (format wide home/away). `understat_schedule` = résultat + xG par match ; `understat_stats` = stats avancées (npxG, ppda, etc.). `understat_stats` récupère sa date par jointure sur `silver.fbref_schedule` — d'où la dépendance d'ordre `fbref` avant `understat`.

### `silver.whoscored_team_season`
Grain 1 ligne = 1 équipe × saison, stats agrégées (colonnes `ws_*`, castées en `Float64`). Construit à partir de 8 fichiers Parquet par ligue/saison (Defensive/Offensive/xG × Home/Away × For/Against) déjà joints au stade Bronze.

## 3. Tables lues mais pas créées par ce pipeline

Ces deux tables sont des **prérequis** (probablement chargées par `dbt seed`, avant `run_ingest.py`) — si elles sont vides ou absentes, le fuzzy matching ne peut pas résoudre les équipes et tout finit en ADU :
- `referentiel.competition_mapping` (alias → nom de compétition canonique + catégorie Big5/D2/Cup/Europe/Other)
- `referentiel.transfermarkt_clubs` (pool de clubs Transfermarkt par saison/ligue, utilisé comme candidats du fuzzy matching)

## 4. Récapitulatif

| Table | Schéma | Créée par ce pipeline ? | Grain |
|---|---|---|---|
| `team_mapping` | referentiel | oui (alimentée en continu) | 1 alias équipe |
| `match_registry` | intermediate | oui (upsert) | 1 match |
| `stg_whoscored_match_index` | silver | non (normalisée/réécrite) | 1 match WhoScored |
| `fbref_schedule` | silver | oui | 1 match × club |
| `fbref_shooting` | silver | oui | 1 match × club |
| `fbref_keeper` | silver | oui | 1 match × club |
| `fbref_misc` | silver | oui | 1 match × club |
| `understat_schedule` | silver | oui | 1 match |
| `understat_stats` | silver | oui | 1 match |
| `whoscored_team_season` | silver | oui | 1 équipe × saison |
| `odds` | silver | non — étape désactivée | 1 match (si activée) |
| `competition_mapping` | referentiel | non — lue seulement | 1 alias compétition |
| `transfermarkt_clubs` | referentiel | non — lue seulement | 1 club × saison × ligue |
