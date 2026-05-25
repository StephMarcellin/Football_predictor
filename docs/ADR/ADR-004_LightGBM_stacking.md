# ADR-004 — LightGBM Two-Stage Stacking

## Statut

Adopté

## Contexte

La tâche de prédiction est une **classification multiclasse à 3 classes** (H / D / A) sur des matchs de football Big5. Les caractéristiques du problème orientent fortement le choix de modèle :

- **Signal faible** : le football est un sport à forte variance. La meilleure équipe perd régulièrement. Même un modèle parfait ne dépasserait pas ~50–55% d'accuracy sur données historiques.
- **Classes déséquilibrées** : H ≈ 44%, D ≈ 26%, A ≈ 30%. Les nuls sont structurellement sous-prédits par les modèles classiques.
- **Features hétérogènes** : rolling stats, features WhoScored (xG, pressing), cotes de marché, H2H — des échelles et natures très différentes.
- **Importance des probabilités calibrées** : l'objectif final est de détecter des value bets (comparer P(modèle) vs P(marché)). Un modèle bien discriminant mais mal calibré est inutilisable pour les paris.
- **Deux sources de signal asymétriques** : les features Home et Away ne sont pas symétriques (l'avantage du terrain est réel et documenté en football). Un modèle unique perd ce signal directionnel.

### Options considérées pour l'architecture de modèle

|Option|Profil|
|---|---|
|**LightGBM single model**|Rapide, performant, mais traite Home et Away symétriquement|
|**LightGBM Two-Stage Stacking**|Trois perspectives + méta-modèle, capture l'asymétrie H/A|
|**Random Forest**|Robuste, interprétable, moins performant que LightGBM sur tabular data|
|**XGBoost**|Concurrent de LightGBM, similaire, légèrement plus lent à entraîner|
|**Neural Network tabular**|Demande plus de données, hyperparamètres, pas d'avantage prouvé sur données football|
|**Poisson regression (Dixon-Coles)**|Modèle probabiliste football-spécifique, pas de features arbitraires|

---

## Décision

**LightGBM Two-Stage Stacking** avec calibration isotonique et optimisation du seuil Draw.

Architecture retenue :

- **Stage 1A** : LightGBM sur features `h_*` (perspective domicile)
- **Stage 1B** : LightGBM sur features `a_*` (perspective extérieur, labels inversés)
- **Stage 1C** : Logistic Regression baseline sur features `h_*` + `a_*`
- **Stage 2** : Logistic Regression méta sur [P_1A + P_1B + P_1C + P_marché + draw_feats]
- **Calibration** : IsotonicRegression par classe, fittée sur la première moitié du val set
- **Draw boost** : seuil optimisé sur F1-Draw du val set

---

## Justification

### Pourquoi LightGBM

LightGBM (Light Gradient Boosting Machine, Microsoft 2017) est un algorithme d'ensemble basé sur le gradient boosting d'arbres de décision, avec plusieurs optimisations clés :

**Histogram-based split finding** : au lieu de trier toutes les valeurs pour trouver le meilleur split (comme dans XGBoost), LightGBM discrétise les features en bins. Cela réduit la complexité de O(n × d) à O(b × d) où b est le nombre de bins (≈ 256 par défaut). Résultat : entraînement 5–10× plus rapide que XGBoost sur de larges datasets.

**Leaf-wise growth** : LightGBM fait croître l'arbre en choisissant la feuille qui réduit le plus la loss (leaf-wise), plutôt que de développer tous les nœuds d'un niveau avant de passer au suivant (level-wise). Cela produit des arbres plus profonds mais plus précis pour le même nombre de feuilles.

**Gestion native des valeurs manquantes** : LightGBM apprend automatiquement dans quelle direction envoyer les NaN lors des splits. Pas besoin d'imputation préalable pour les arbres (l'imputation reste nécessaire pour la LR baseline).

**Features catégorielles natives** : si déclarées, LightGBM peut utiliser un algorithme de split spécifique (Fisher's optimal partitioning) plus performant qu'un one-hot encoding.

> [!info] Gradient Boosting : principe fondamental Le gradient boosting construit une séquence d'arbres où chaque arbre corrige les erreurs du précédent. Formellement, on minimise une fonction de perte L(y, F(x)) en construisant F_m(x) = F_{m-1}(x) + η·h_m(x) où h_m est un arbre fitté sur les **résidus négatifs du gradient** de L par rapport à F_{m-1}. Pour une loss log-loss multiclasse, ces résidus sont les différences entre probabilités prédites et labels one-hot. L'arbre "corrige" les matchs les plus mal prédits par l'ensemble précédent.

### Pourquoi le Two-Stage Stacking

**Justification de l'asymétrie Home/Away** : un match de football n'est pas symétrique. Jouer à domicile apporte un avantage documenté (l'avantage du terrain est estimé à environ +0.3 but en faveur de l'équipe domicile). Un modèle unique entraîné sur la ligne complète (home + away) doit apprendre cette asymétrie implicitement depuis les features.

L'approche stacking entraîne trois modèles de façon explicitement asymétrique :

- `lgbm_home` voit uniquement les features de l'équipe domicile — il apprend "quand est-ce que l'équipe qui joue à domicile gagne ?"
- `lgbm_away` voit uniquement les features de l'équipe extérieure avec les labels inversés — il apprend "quand est-ce que l'équipe qui joue à l'extérieur renverse le pronostic ?"
- `lr_baseline` apporte un signal linéaire stable comme ancre

**Le méta-modèle** apprend comment combiner ces trois signaux asymétriques, enrichis des probabilités de marché (prior Pinnacle) et des features contextuelles spécifiques aux nuls.

> [!info] Stacking vs Bagging vs Boosting **Bagging** (Random Forest) : entraîne N arbres indépendants sur des sous-échantillons aléatoires, fait la moyenne. Réduit la variance. **Boosting** (LightGBM, XGBoost) : entraîne N arbres séquentiellement, chaque arbre corrigeant le précédent. Réduit le biais. **Stacking** : entraîne des modèles de base (Stage 1) et un méta-modèle (Stage 2) qui apprend à combiner les prédictions des modèles de base. Peut combiner des modèles de familles différentes. Le risque est le data leakage entre Stage 1 et Stage 2 — évité ici par le split temporel strict.

### Pourquoi la calibration isotonique

Un modèle bien discriminant n'est pas nécessairement bien calibré. LightGBM tend à produire des probabilités "trop extrêmes" — il est surconfiant sur les classes dominantes.

**La calibration isotonique** est une méthode non-paramétrique qui apprend une fonction monotone croissante f : p_brut → p_calibré. Elle minimise l'erreur quadratique (p_calibré - y)² sur les données de calibration. Par rapport à la calibration de Platt (régression logistique), la régression isotonique est plus flexible mais peut sur-apprendre sur de petits datasets.

**Protocole pour éviter le leakage** : le val set (2 saisons) est splitté en deux moitiés temporelles. La première moitié sert à fitter les calibrateurs, la deuxième à évaluer les performances finales (OOS par rapport à la calibration).

**Renormalisation obligatoire** : la régression isotonique calibre chaque classe indépendamment — la somme des probabilités calibrées ne vaut plus nécessairement 1. Une renormalisation explicite `p_cal / p_cal.sum(axis=1, keepdims=True)` est appliquée.

### Pourquoi le seuil Draw séparé

Les nuls (D ≈ 26%) sont sous-prédits par argmax standard car leur probabilité est diffuse. Quand P(H) = 0.40, P(D) = 0.35, P(A) = 0.25, l'argmax prédit Home — mais si ce match est réellement un nul, le modèle "a vu" que D était probable.

L'optimisation du seuil via **F1-score Draw** sur le val set permet de trouver un seuil `draw_threshold` tel que si P(D) > seuil ET P(D) > 0.7 × max(P(H), P(A)), on prédit Draw.

> [!tip] F1-score pour les classes déséquilibrées L'accuracy est trompeuse sur des classes déséquilibrées : un modèle qui prédit toujours Home obtient ~44% d'accuracy sans avoir rien appris. Le F1-score est la moyenne harmonique de la précision (parmi les matchs prédits nuls, combien le sont vraiment ?) et du recall (parmi les vrais nuls, combien sont prédits nuls ?). Optimiser le F1-Draw maximise le bon équilibre entre précision et recall pour la classe minoritaire.

### Optimisation des hyperparamètres via Optuna

**Optuna** utilise un sampler **TPE** (Tree-structured Parzen Estimator) pour optimiser les hyperparamètres LightGBM. TPE est un algorithme bayésien : il modélise la distribution des hyperparamètres qui ont donné de bons résultats (p(x|good)) et ceux qui ont donné de mauvais résultats (p(x|bad)), et propose de nouveaux candidats qui maximisent le ratio p(x|good)/p(x|bad).

C'est plus efficace qu'une grid search (exhaustive mais exponentielle) ou une random search (non adaptive) pour des espaces de recherche à 8+ dimensions.

**Early stopping** : chaque trial LightGBM utilise `early_stopping_rounds=50` — l'entraînement s'arrête si le Log Loss val ne s'améliore pas pendant 50 rounds consécutifs. Cela évite le sur-entraînement et accélère la recherche.

---

## Paramètres clés et leurs justifications

|Paramètre|Valeur|Justification|
|---|---|---|
|`n_estimators`|500 (max) + early stopping|Borne haute avec arrêt automatique|
|`learning_rate`|Optuna [0.01, 0.3]|Trade-off vitesse/précision|
|`num_leaves`|Optuna [20, 150]|Complexité de l'arbre|
|`min_child_samples`|Optuna [10, 50]|Régularisation — évite les feuilles sur de trop peu de matchs|
|`C` LR méta|0.1|Régularisation L2 forte — les 3 modèles Stage 1 sont corrélés|
|`KELLY_FRACTION`|0.5|Half Kelly — réduit la variance de la bankroll|
|`draw_threshold`|Optimisé sur F1-Draw|Varie selon les saisons disponibles (typiquement 0.27–0.32)|

---

## Conséquences

### Positives

- Capture explicite de l'asymétrie Home/Away via les perspectives séparées
- Probabilités calibrées utilisables directement pour le calcul d'edge
- Interprétabilité via SHAP pour les modèles Stage 1 LightGBM
- Flexible : on peut ajouter un Stage 1D (ex. LightGBM sur features draw uniquement) sans modifier l'architecture

### Négatives et limites

- **Durée d'entraînement** : 15–45 minutes selon `n_trials` Optuna (× 2 modèles LightGBM × 3 perspectives)
- **Complexité** : 5 modèles à sauvegarder, 3 préprocesseurs, 3 calibrateurs, 1 seuil — l'artefact `.joblib` est monolithique
- **Leakage potentiel Stage 1→Stage 2** : si Stage 1 et Stage 2 sont fittés sur les mêmes données, Stage 2 sur-apprend les erreurs de Stage 1. Ici, Stage 2 est fitté sur le val set (données que Stage 1 n'a jamais vues) — le protocole est correct.
- **Le méta-modèle n'a pas d'optimisation Optuna** : son `C=0.1` est fixé manuellement.

---

## Questions d'entretien anticipées

**"Qu'est-ce que LightGBM et pourquoi est-il populaire sur les données tabulaires ?"**

LightGBM est un algorithme de gradient boosting d'arbres développé par Microsoft. Il est populaire sur les données tabulaires pour trois raisons : (1) il est très rapide grâce à l'histogram-based split finding qui discrétise les features en bins au lieu de les trier entièrement, (2) il gère nativement les valeurs manquantes et les variables catégorielles, (3) il produit généralement de meilleures performances que Random Forest sur ce type de données, surtout avec de l'optimisation d'hyperparamètres. Sur les compétitions Kaggle de données tabulaires, LightGBM ou XGBoost dominent la plupart des leaderboards.

**"Qu'est-ce que le stacking et en quoi diffère-t-il du bagging ?"**

Le bagging (ex. Random Forest) entraîne N modèles indépendants et fait la moyenne de leurs prédictions — chaque modèle est de la même famille et l'agrégation est simple. Le stacking entraîne des modèles de base (Stage 1, potentiellement de familles différentes) et un méta-modèle (Stage 2) qui apprend à les combiner de façon optimale. Le méta-modèle peut apprendre que "dans les matchs à domicile, LightGBM Home est plus fiable que LightGBM Away" ou "quand les deux modèles divergent, la régression logistique est plus calibrée". L'avantage du stacking est sa flexibilité ; le risque est le data leakage entre Stage 1 et Stage 2.

**"Pourquoi calibrer les probabilités d'un modèle ML ?"**

Un modèle bien discriminant (il classe correctement) n'est pas nécessairement bien calibré (ses probabilités reflètent des fréquences réelles). LightGBM tend à produire des probabilités "trop confiantes". Si le modèle prédit P(H) = 0.95 mais que ces matchs ne finissent en victoire domicile que 70% du temps, les value bets calculés seront incorrects. La calibration aligne les probabilités prédites avec les fréquences observées. La régression isotonique est préférée à la calibration de Platt (régression logistique) quand on a suffisamment de données de calibration et qu'on ne veut pas faire d'hypothèse paramétrique sur la forme de la correction.

**"Comment gérez-vous les classes déséquilibrées en classification ?"**

Plusieurs approches existent : (1) ajuster les `class_weight` dans le modèle, (2) sur-échantillonner la classe minoritaire (SMOTE), (3) sous-échantillonner la classe majoritaire, (4) optimiser un seuil de décision post-entraînement. Dans ce projet, c'est la dernière approche qui est utilisée pour la classe Draw : le modèle est entraîné sans pondération (`class_weight=None`) pour ne pas biaiser les probabilités, et un seuil Draw est optimisé séparément sur le val set en maximisant le F1-Draw. Cela permet de contrôler précisément le trade-off précision/recall sur les nuls.

**"Qu'est-ce qu'Optuna et en quoi diffère-t-il d'une grid search ?"**

Optuna est un framework d'optimisation d'hyperparamètres utilisant des algorithmes bayésiens (TPE par défaut). Une grid search exhaustive teste toutes les combinaisons d'un espace fini — son coût est exponentiel en nombre de paramètres. Optuna est adaptatif : il modélise la distribution des bons hyperparamètres à partir des trials précédents et propose des candidats dans les régions prometteuses de l'espace de recherche. Sur un espace à 8 dimensions, 50 trials Optuna explorés intelligemment surpassent généralement une grid search de 50 points aléatoires.