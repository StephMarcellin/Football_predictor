# Passation — Session marts & modèles (17 août 2026)

> Thème : famille 11 (KNN) finalisée, construction des **marts par modèle**,
> premier **trainer 1N2** et premier **backtest value**. Verdict backtest : pas
> d'edge exploitable vs Pinnacle en l'état — analyse et suites ci-dessous.
> Branche de travail : `feature/ml-prediction-pipeline`.

---

## 1. Ce qui a été réalisé (jusqu'au premier backtest)

### Famille 11 — KNN (features 76, 77, 78)
- **Diagnostic données** (`pipelines/diagnostics/knn_diagnostic.py`) : seuil de
  fiabilité du profil figé à **10 observations** (split-half r≈0,70), **3 clusters
  par côté** (silhouette), 7 espaces de similarité validés.
- **Correctif clé** : `def_threat_conceded_per90_lag` et
  `scorer_xgot_overperformance_lag` étaient déjà codés dans `joueur_saison.sql`
  mais absents de la table `incremental` → réglé par un `--full-refresh`.
- **Clusters 76/77** (`pipelines/03_knn_impute.py`) : KMeans standardisé sur
  profil + position, **labels sémantiques stables** (off_finisher/creator/low,
  def_aerial/recuperator/low, GK), déterminisme réglé (tri des lignes).
  → `machine_learning.player_style_clusters`.
- **Imputation 78** : KNN action-spécifique (coordonnées joueur fiables) avec
  repli cluster, **méthode par cible choisie par validation par masquage**
  (RMSE : KNN < cluster < global sur 6/7 cibles ; `def_duel_win_rate_by_zone`
  laissé en repli cluster). → `machine_learning.zonal_profiles_imputed`
  (639 525 lignes, 0 NULL, flag `*_imputed` par colonne).
- **Wiring dbt** : sources `machine_learning` déclarées ; `team_corridor_profile`
  et `joueur_match` repointés vers la table imputée.

### Couche gold (nettoyage principe « mart = sélection »)
- `equipe_confrontation_zone` : pivot des confrontations de couloir au grain
  (match, team) — perspective offensive + défensive.
- `equipe_gardien_match` : profil de saison du gardien titulaire.

### Marts (1 par modèle — `config/models.yml`)
| Mart | Grain | Cible |
|---|---|---|
| `mart_1n2` | (match, team) | result_1n2 (H/D/A) |
| `mart_goals` | (match, team) | team_goals → dérive O/U, BTTS, clean sheet |
| `mart_scorers` | (match, player) | scored ≥1 |
| `mart_assists` | (match, player) | assisted ≥1 |
| `mart_events` | (match) | total_cards, total_corners (bruts) |

Tous 100 % sélection (le seul calcul restant, le pivot couloir, est en gold).
Cotes propagées dans `mart_1n2` **pour le backtest**, bannies des features.

### Modélisation
- `pipelines/ml_common.py` : plomberie partagée (chargement mart, split par
  saison, préparation X/y avec **exclusion des cotes** `MARKET_PREFIXES`,
  LightGBM + early stopping, **calibration isotone**, évaluation).
- `pipelines/train_1n2.py` : trainer 1N2 (multiclasse, calibré, MLflow best-effort).
- `pipelines/backtest_1n2.py` : backtest value vs Pinnacle/moyen, edge, Kelly
  fractionnaire, **CLV**, balayage marché × seuil.

### Résultats (honnêtes)
- 1N2 : logloss ~**0,99** (VAL/TEST), accuracy ~53 %. Bat la baseline fréquences
  (1,07), mais **légèrement au-dessus** d'un book sharp (~0,96).
- Backtest saison 2024-25 : **ROI −7 à −12 %** sur toutes les configs, **CLV
  ~+0,2 %** (négligeable). → **Pas d'edge exploitable vs Pinnacle.** Le marché
  est parfaitement calibré (vérifié) ; c'est pour ça qu'on ne le bat pas.

### Limites connues (consignées)
- Couche tactique (confrontations + gardien) présente sur **~55 % des matchs**
  (couverture des compos), **0 % sur 2025-26**.
- Cotes présentes sur **~55 %** des matchs (côté domicile).
- Reportées faute de données source : famille 4 (`lineup_avg_rating/age/
  experience`, `n_key_starters_missing`), `keeper_distribution_lag`.
- Contexte match léger dans les marts joueurs (V2) ; profil zonal joueur non
  agrégé (V2).

---

## 2. Pour rendre le projet VISIBLE / présentable en entretien (prod)

L'edge de prédiction n'est **pas** un prérequis pour présenter un projet prod
crédible. La chaîne scrape → tables → KNN → marts → train → backtest tourne déjà
de bout en bout. Il reste surtout à « câbler » :

1. **Orchestration** — découper `run_pipeline.py` en **3 flux** (quotidien
   transform+predict / hebdo retrain / rare refit xT-xGOT). Insérer l'étape
   `03_knn_impute` entre deux phases dbt, exactement comme les chaînes xT/xGOT.
2. **Scraping compos 2025-26** — débloque la couche tactique et les prédictions
   courantes (aujourd'hui à 0 %).
3. **Script de prédiction** sur les nouveaux marts (adapter `05_predict`) pour
   sortir les prédictions des matchs à venir.
4. **Tests dbt** sur les nouveaux modèles gold/marts (grain unique, not_null) +
   doc `schema.yml` des marts.
5. **MLflow** — corriger les creds dagshub (403 actuel) ou basculer sur `mlruns/`
   local pour la démo.
6. **Vitrine engineering** — CI GitHub Actions, Great Expectations, Docker (déjà
   là). Ce sont les pièces qui « font pro » en entretien.

---

## 3. Pour « être gagnant » (chercher l'edge — projet de fond)

1. **Entraîner les 4 autres modèles** (goals, scorers, assists, events) en
   répliquant le patron `ml_common` + trainer.
2. **Réinjection des probas** — dériver O/U, BTTS, clean sheet du modèle Buts
   (distribution par équipe) ; **injecter les sorties du modèle Buts comme
   features du 1N2** (stacking two-stage). Idem probas buteurs.
3. **Enrichir les marts joueurs** avec le contexte match (attaque équipe /
   défense adverse / gardien adverse) + profil zonal joueur (agrégation gold).
4. **Améliorer les modèles** : Optuna, meilleure calibration, couverture compos
   complète, sélection de features (écarter les dims faibles : `def_errors`,
   `def_duel_win_rate_by_zone`).
5. **Viser des marchés plus mous** : cotes d'ouverture, marché moyen, props
   joueurs, petits championnats — où l'edge est plus atteignable que vs Pinnacle
   à la clôture.
6. **CLV comme métrique nord** : suivre systématiquement la Closing Line Value,
   meilleur prédicteur de rentabilité long terme.
7. Compléter la famille 4 (ratings/âge via scraping) et `keeper_distribution_lag`.

---

## 4. Git (état de reprise)

Tout est commité sur `feature/ml-prediction-pipeline` (7 commits thématiques :
config, intermediate, gold, KNN, marts+modèles, docs). Prochaine session : MR
vers `main` après revue, puis attaquer l'orchestration (§2.1) ou l'entraînement
des autres modèles (§3.1).
