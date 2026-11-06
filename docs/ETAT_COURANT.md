# ÉTAT COURANT — Projet 3-Étoiles
Source de vérité inter-sessions. Mis à jour par le skill `session-handoff`. Dernière mise à jour : 2026-11-04.

## Objectif (PMO, 04/11/2026)
Amener le modèle 1N2 vers la qualité de probabilités du marché (Pinnacle), puis chercher de l'information absente du marché. But final : value betting rentable (10 k€). Base de données gelée. Idée du PMO : prédire les buts (λ) et les injecter dans le 1N2.
Critères : RPS + log-loss (principaux), taux de réussite (secondaire, trop bruité : ±0,45 pt). Règle de décision : IC95 du gain hors 0 ET gain sur ≥ 5 saisons sur 7.
Rappel : égaler le marché ne rapporte rien (ROI ≈ −marge). Il faut de l'information que le marché n'a pas.

## Référence mesurée (04/11, 11 593 matchs Big5 avec cotes, 7 saisons 2019-20→2025-26)
| | Réussite | RPS | Log-loss |
|---|---|---|---|
| Modèle toutes_saisons, variante E | 51,5 % | 0,2049 | 1,0010 |
| Pinnacle ouverture | 53,6 % | 0,1958 | 0,9711 |
| Pinnacle clôture | 53,9 % | 0,1951 | 0,9687 |
Écart : ~0,030 de log-loss. Fréquence de base Big5 : 1,0787.

## Paliers (arrêt + compte rendu au PMO après chacun)
| # | Contenu | Statut |
|---|---|---|
| 0 | Audit fuite `h2h_win_rate_cutoff` / `h2h_loss_rate_cutoff` ; reproduire la référence A dans le Linux | TERMINÉ 04/11 : aucune fuite (10 355 lignes recalculées, 0 écart) ; reproduction exacte (écart max 0,0000) |
| 1 | Adopter E (`subsample_freq=1` dans train_1n2.py et rolling_eval.py) ; script versionné du tableau d'écart au marché | TERMINÉ 05/11 (PR #53 fusionnée, 3 modèles réentraînés sur le PC) |
| 2 | DÉTOUR diagnostic : probas Pinnacle en feature (jamais en production) pour mesurer ce que nos features ajoutent au marché | TERMINÉ 05/11 : aucun mélange modèle/marché ne bat Pinnacle ouverture ; voir JOURNAL |
| 3 | Réparer la chaîne de buts ; Poisson/Dixon-Coles dérivé vs 1N2 direct ; mélange simple (poids choisi sur la saison de validation) ; go/no-go | TERMINÉ 05/11 : GO pour améliorer le 1N2 (Dixon-Coles bat le direct), NO-GO pour un avantage face au marché ; décision PMO sur le palier 4 en attente ; `buts_equipe.joblib` à réentraîner sur le PC |
| 4 | Si go : λ hors échantillon en fenêtre croissante par saison (λ de S issu d'un modèle < S, NaN avant), features λ_dom, λ_ext, écart, total | À faire |
| 5 | Rendement historique « cote bookmaker contre Pinnacle démarginé », sans modèle (`tools/odds_edge_backtest.py`, CSV bruts football-data) | TERMINÉ 06/11 : NO-GO, pas d'avantage structurel robuste (voir JOURNAL) |
| 6 | Elo + classement avant chaque match (`tools/elo_features.py`, option `--extra` de `rolling_chunk.py`), test contre la variante E | TERMINÉ 06/11 : GO. Gain log-loss +0,0101 IC95 [+0,0074 ; +0,0130], 7/7 saisons. Reste à industrialiser (dbt + script Python) : palier 7 |
| 7 | Features v2 en trois groupes (`tools/features_v2.py`) comparés à F1 : A mémoire longue/forme vs attendu, B forme selon le lieu, C calendrier + enjeu | TERMINÉ 06/11 : A NO-GO (−0,0001), B NO-GO (−0,0005), C GO mais faible (+0,0009 IC95 [+0,0003 ; +0,0015], 7/7). Industrialisation Elo + classement (+ éventuellement C) = palier 8 |
Détours possibles ensuite : entraînement Big5 seul, rating de force type Elo, modélisation des nuls.

## Décisions actées
- Palier 2 (marché en feature) autorisé en diagnostic uniquement.
- Palier 5 : aucun bookmaker de l'historique ne bat Pinnacle de façon robuste ; seul Interwetten montre un gain, concentré sur 2017-19 puis disparu. Ne pas bâtir de stratégie de paris sur cette piste.
- Palier 6 : Elo et classement avant match améliorent nettement le 1N2 (écart à Pinnacle clôture : log-loss 0,0323 -> 0,0219, RPS 0,0097 -> 0,0064). Les features de force à mémoire longue étaient le trou principal. Paramètres Elo figés avant mesure (K=20, avantage du terrain 70, regression 25 %) et non optimisés.
- Palier 7 : l'Elo capture déjà la mémoire longue et la forme (groupes A et B sans gain). Seul l'enjeu de classement (écart au leader, à la 4e place, à la relégation, matchs restants) apporte un gain faible, +0,0009 de log-loss. Les prochains gains ne viendront probablement pas de nouvelles statistiques de forme d'équipe.
- Git : commit + push + PR par Claude ; Stéphane seul merge. Branche par thème (`feature/...`).
- Limites de coût : par palier ≤ 40 appels d'outils, ≤ 3 variantes d'expérience, ≤ 2 tentatives par blocage ; sorties tronquées ; pas de sous-agents ; à la limite : stop, point, question.
- Le détail des règles de données est dans `CLAUDE.md` (ne pas dupliquer ici).

## Contraintes d'environnement
- Linux de l'appareil : 2 cœurs, 3,9 Go RAM, 180 s par appel, aucun processus ne survit entre deux appels → découper en tâches avec sauvegarde (fichier par pli × graine). Env : `bash tools/vm_bootstrap.sh` (timeout ≥ 175 s).
- Exploration dans le Linux (1 graine, peu de plis) ; confirmations (7 plis, 3-5 graines) sur le PC de Stéphane ; écritures en base sur son PC.
- Base en lecture seule : `db/football.duckdb` (~80 Go). Le Linux reproduit exactement les résultats du PC (vérifié pli 2024-25, 3 graines) : les expériences y sont comparables aux CSV existants.
- À chaque session : ré-accorder l'accès au dossier et la permission de suppression (verrous Git).
- Push/PR : jeton fine-grained dans `.tmp/gh_token` (ignoré par Git, créé le 04/11, expiration ~30 jours : à vérifier sur GitHub). Procédure dans le skill `release-manager`. PR #52 ouverte (continuité + palier 0).

## À vérifier (ouvert)
- Doc LightGBM : `subsample_freq=0` désactive-t-il le tirage de lignes ? (E > A observé, mécanisme non confirmé).
- Écart 1,011 (passation 31/10) vs 1,025 mesuré en brut isotone sur 2025-26.
- 566 matchs avec cotes mais sans probas d'ouverture ; 863 matchs Big5 sans cotes dans le banc.
- `config/models.yml` : `buts_equipe` déclaré multiclasse alors que `train_goals.py` est Poisson.
- 4 fichiers avec `reindex(fill_value=0)` silencieux : eval_goals, predict_ensemble, predict_markets, serve_features.
- `config.yaml` mergé avec des réglages locaux (base prod, saison unique) ; CI : chemin `02_process.py` inexistant ; `dbt_project.yml` : temp_directory codé en dur ; `PIPELINE_ORDER.md` périmé.

## Prochaine action
Palier 1 : FAIT dans le code : `subsample_freq=1` ajouté (rolling_eval.py PARAMS, train_1n2.py params) ; `tools/market_benchmark.py <rolling_eval_*.csv>` versionné (reproduit E : 51,5 % / RPS 0,2049 / LL 1,0010 vs clôture 0,1951 / 0,9687). RESTE sur le PC de Stéphane : `python pipelines/models/train_1n2.py --model <clé>` pour resultat_1n2_toutes_saisons, resultat_1n2_avant_compo, resultat_1n2 ; puis `pipelines/prediction/predict_matches.py` par modèle. Mécanisme confirmé empiriquement 05/11 (LightGBM 4.6.0) : `subsample=0.8` seul = aucun tirage de lignes (écart entre graines 0,0) ; avec `subsample_freq=1` = actif (écart 0,27). Citation de la doc officielle non obtenue.

## Budget consommé
Palier 0 : 12 / 40 appels (terminé). Palier 1 : ~14 / 40 (code terminé, en attente du réentraînement PC).

## Fichiers clés
`docs/JOURNAL_RESULTATS.md` (résultats déjà obtenus, à lire avant toute expérience) · `pipelines/models/rolling_eval.py` · `pipelines/models/train_1n2.py` · `pipelines/ml_common.py` · `reports/rolling_eval_*.csv` (non suivis par Git) · `config/models.yml`.
