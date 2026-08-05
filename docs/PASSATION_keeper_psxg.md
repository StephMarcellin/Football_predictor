# PASSATION — Projet 3-Étoiles · session PSxG gardien

## Fait cette session
**Chaîne PSxG gardien complète, fiable, testée, documentée** — dernière brique de la chaîne xGOT.

- **Méthode d'attribution du gardien** (décidée + validée corpus) :
  - Tirs **arrêtés** → gardien = **Save miroir** relié au tir par le **qualifier 233** (`related_event_id` est NULL — le vrai lien passe par qual 233, comme `int_penalties`). `Save.player_id` = GK lineup à **99,995 %**.
  - **Buts** → gardien = **GK lineup** (`slot=1` ⟺ `grid_vertical=0`) de l'équipe qui défend, période de formation active (`start_minute` max ≤ minute du tir).
  - `keeper_id = COALESCE(save, lineup)`. **CSC et penalties exclus** (périmètre amont).

- **Refactor anti train/serve skew** : socle **`xgot_features`** (features + périmètre partagés) ; `xgot_training` = socle + label (**refactor neutre prouvé**, logloss 0,4565 identique) ; fonction **`build_X`** extraite dans `xgot_train.py` (partagée train/scoring).

- **Étape de scoring créée** : `pipelines/xgot_score.py` applique `xgot.joblib` à tous les tirs cadrés → écrit **`machine_learning.xgot_predictions`** (137 523 tirs, Σ xGOT = 40 728, `xgot ∈ [0,1]`), idempotent (CREATE OR REPLACE), déclarée **source dbt** (patron `xt_grid`).

- **`int_keeper_shots`** (attribution par tir) : 137 523 tirs, **grain unique**. Voies : save 96 302 / lineup 40 154 / none 1 067 (0,78 %). Réconciliation buts/xGOT parfaite.

- **Garde-fou symétrique `def_gk_available`** : ne garde un tir que si l'équipe qui défend a un GK lineup dans le match (buts ET arrêts attribuables). Sinon les arrêts seraient crédités sans le risque de but → gardiens « invaincus ». Exclut **2,29 %** des tirs.

- **`int_keeper_psxg`** (gardien × saison × championnat) : `psxg_plus_minus = Σ xGOT subi − buts encaissés`. Après garde-fou : **1 886 gardien-saisons, Σ PSxG+/- = -313 (-0,78 %), 0 invaincu**, top-classement plausible (save_pct 0,72-0,84).

- **Doc + tests dbt** ajoutés pour les 4 artefacts (grain unique, `accepted_values`, `not_null`).

- **Anomalie « 1 but via save » résolue** : c'est un **but sur rebond** (tir arrêté puis rebond marqué, qual 233 relie le but au Save du gardien) → attribution correcte, pas un bug.

## Fichiers (état actuel)
**Créés :**
- `dbt_project/models/machine_learning/xgot_features.sql` — socle features xGOT
- `pipelines/xgot_score.py` — scoring xGOT par tir → `xgot_predictions`
- `dbt_project/models/intermediate/int_keeper_shots.sql` — attribution gardien par tir
- `dbt_project/models/intermediate/int_keeper_psxg.sql` — agrégat PSxG+/-

**Modifiés :**
- `dbt_project/models/machine_learning/xgot_training.sql` — branché sur `xgot_features`
- `pipelines/xgot_train.py` — `build_X` extrait de `prepare`
- `dbt_project/models/sources.yml` — source `machine_learning.xgot_predictions`
- `dbt_project/models/machine_learning/schema.yml` — doc `xgot_features` + màj `xgot_training`
- `dbt_project/models/intermediate/schema.yml` — doc + tests `int_keeper_shots`, `int_keeper_psxg`

## ⚠️ À builder / tester au prochain lancement
```powershell
dbt run  --select int_keeper_shots int_keeper_psxg
dbt test --select int_keeper_shots int_keeper_psxg xgot_features
```
(`int_keeper_shots` a gagné la colonne `def_gk_available` — la table en base doit être rebuildée.)

## PLAN DE ROUTE (suite)
1. **Câbler le scoring dans l'orchestration** — ⚠️ la chaîne PSxG casse le tout-dbt : ordre à respecter **`xgot_features` (dbt) → `xgot_score.py` (Python) → `int_keeper_shots`/`int_keeper_psxg` (dbt)**. Intercaler le script Python entre deux phases dbt dans `run_pipeline`/Prefect.
2. **Backlog re-load formations** : **2024-2025 Serie A (0 % lineup)** et **2.Bundesliga 2024-2025 (95 %)** → réintègrent automatiquement le PSxG via `def_gk_available` une fois les `formation_slots` re-chargés en silver.
3. Exposer le **nom du gardien** (`int_whoscored_players`) en couche gold ; éventuel `int_keeper_match` si on veut le PSxG en feature du modèle 1N2.
4. Backlog antérieur : câbler split process dans `run_pipeline`, retirer `02_process.py`, Great Expectations, GitHub Actions.

## NOTES À GARDER EN TÊTE
- **`xgot_predictions` écrite HORS dbt** (Python) → lue en *source* dbt. Rejouable à chaque run (idempotent).
- **Garde-fou `def_gk_available`** : auto-correcteur — re-charger les formations ré-inclut les poches sans toucher au code.
- **Biais de calibration isotone** : Σ xGOT sous-estime les buts de ~0,8-1,9 % → PSxG+/- global légèrement négatif. Mineur, à resserrer plus tard si besoin.
- **1 but via save = rebond**, attribution correcte.
- Base DuckDB **verrouillée par moments** (sessions concurrentes) pendant la session — normal.
- **Ne jamais `--full-refresh` en prod hebdo** sauf changement de logique d'un modèle.

## Commits à pusher (branche `feature/keeper-psxg`)
1. `refactor: socle xgot_features partagé + build_X (anti train/serve skew)`
   → `xgot_features.sql`, `xgot_training.sql`, `xgot_train.py`
2. `feat: scoring xGOT par tir → machine_learning.xgot_predictions`
   → `xgot_score.py`, `sources.yml`
3. `feat: PSxG+/- gardien (attribution qual233/lineup + garde-fou symétrique)`
   → `int_keeper_shots.sql`, `int_keeper_psxg.sql`
4. `docs: schema.yml pour xgot_features, xgot_predictions, int_keeper_shots, int_keeper_psxg`
   → `machine_learning/schema.yml`, `intermediate/schema.yml`
