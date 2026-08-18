---
schema: gold
rows: 1872
---
# gardien_saison

#gold #famille9 #famille11

Grain (keeper_id, season). Famille CDC 9 (profil gardien), classe SEASON-LAG. Chaque ligne (gardien, saison S) porte le profil de la saison S-1 (int_keeper_psxg est au grain saison, pas de PSxG au niveau match). Anti-leakage par construction. NULL = pas de saison N-1 (recrue) → imputation famille 11.

## Intégrité
**Clé déclarée :** (keeper_id, season) — ✅ aucun doublon

## Lineage
**Sources :** [[int_keeper_psxg]]
**Alimente :** —

## Features & profiling  (1872 lignes)

| Feature | Type | Complétion | Null | Distinct | Stats | Description |
|---|---|---|---|---|---|---|
| `keeper_id` | BIGINT | 100.0% | 0 | 635 |  | Identifiant du gardien. |
| `season` | VARCHAR | 100.0% | 0 | 8 | top: 2017-2018 (288), 2021-2022 (246), 2020-2021 (240) | Saison du match à venir (ex. 2023-2024) ; le profil porté est celui de N-1. |
| `keeper_psxg_plus_minus_lag` | DOUBLE | 56.9% | 807 | 1019 | moy 0.323 · méd 0.157 · min/max -17.492/15.848 · p10/p90 -3.733/4.911 | [Famille 9, SEASON-LAG, feature 64] Post-Shot xG +/- de la saison précédente (total : PSxG affronté − buts encaissés ; positif = arrête plus que la qualité des tirs ne le prédit). |
| `keeper_psxg_per_shot_lag` | DOUBLE | 56.9% | 807 | 1058 | moy 0.001 · méd 0.003 · min/max -0.713/0.419 · p10/p90 -0.079/0.08 | [Famille 9, SEASON-LAG] PSxG +/- normalisé par tir affronté (comparable entre gardiens de volumes différents). |
| `keeper_save_pct_lag` | DOUBLE | 56.9% | 807 | 580 | moy 0.707 · méd 0.711 · min/max 0.0/1.0 · p10/p90 0.604/0.8 | [Famille 9, SEASON-LAG, feature 65] Pourcentage d'arrêts de la saison précédente (saves / shots_faced, recalculé). |
| `keeper_shots_faced_lag` | HUGEINT | 56.9% | 807 | 192 | moy 86.354 · méd 97.0 · min/max 1.0/215.0 · p10/p90 7.0/155.0 | [Famille 9, SEASON-LAG, feature 66] Tirs affrontés la saison précédente. Sert de signal de confiance : un faible volume rend le PSxG bruité (seuil de bascule = calibration famille 11). |
| `profile_confidence_flag` | VARCHAR | 100.0% | 0 | 4 | top: none (807), high (730), low (183) | [Famille 11, feature 79] Fiabilité du profil gardien : high (shots_faced≥50) / medium (≥15) / low (≥1) / none (pas de saison N-1). Seuils par défaut, à calibrer. |