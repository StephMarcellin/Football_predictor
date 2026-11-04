# Journal des résultats — à lire avant de relancer une expérience
Une ligne par expérience : date · quoi · résultat · décision. Banc : toutes_saisons, 7 saisons 2019-20→2025-26, Big5 sauf mention.

| Date | Expérience | Résultat | Décision |
|---|---|---|---|
| 2026-11-01 | Calibration isotone vs sigmoïde | Isotone produit des P=0 exactes (log-loss gonflé) ; sigmoïde meilleure 6/6 (significatif 2/6) | Sigmoïde par défaut + plancher 0,005 |
| 2026-11-01 | Évaluation glissante 5 graines | Log-loss 1,0051 vs 1,0787 base ; gain +0,0735 [+0,067 ; +0,080] ; positif chaque saison | Référence |
| 2026-11-01 | xG/PPDA/compo vs toutes_saisons | Indiscernables (IC95 écart [−0,0095 ; +0,0073]) | Features saturées |
| 2026-11-01/02 | Grille hyperparamètres (3 graines), log-loss poolé : A 1,0054 · E 1,0022 · B 1,0075 · C 1,0035 · D 1,0061 | E : −0,0032 [−0,0046 ; −0,0019], 7/7 ; C −0,0019, 5/7 ; B +0,0021, 0/7 ; D +0,0007, 3/7 | E retenu, adoption au palier 1 |
| 2026-11-04 | Écart au marché (variante E, 11 593 matchs) | Modèle 1,0010 · Pinnacle ouv. 0,9711 · clôt. 0,9687 (RPS 0,2049 / 0,1958 / 0,1951) | Cible de qualité ; voir ETAT_COURANT |
| 2026-11-04 | Vitesse Linux (2 cœurs) | 50 arbres, 187 features, ~33 k lignes : 4,2 s | Banc complet = ~20 appels chunkés |
| 2026-11-04 | Audit fuite `h2h_win_rate_cutoff` (palier 0) | Fenêtre `UNBOUNDED PRECEDING AND 1 PRECEDING` par paire ; recalcul indépendant sur 2023-24 et 2024-25 : 10 355 lignes, 0 écart ; 0 paire avec 2 matchs le même jour ; 0 valeur non nulle sans historique | Pas de fuite sur ces 2 saisons (autres saisons non vérifiées) |
| 2026-11-04 | Reproduction de A dans le Linux (palier 0), pli 2024-25, 3 graines, `tools/rolling_chunk.py` | Probabilités identiques à A_ref (écart max 0,0000 sur 2 682 matchs) ; log-loss Big5 1,0097, tout 0,9885 ; 79 s par tâche | Le Linux fait foi pour explorer |
