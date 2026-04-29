# 📊 Agent Analyste — Rapport d'Insights

**Généré le :** 2026-04-27 15:42  
**Source :** `models/predictions_val.csv` — top 50 outliers (Log Loss maximum)  
**Base :** `gold.features_final` × `silver.stg_whoscored_events`

---

## 1. Vue d'ensemble

| Métrique | Outliers (top 50) | Population globale |
|---|---|---|
| Matchs analysés | 50 | — |
| Log Loss moyen | **2.0616** | 1.0151 |
| Log Loss médian | **2.0438** | 0.9675 |
| Log Loss max | **2.6002** | — |
| Couverture WhoScored events | 78.0% | — |
| Matchs sans events WS | 11 | — |

---

## 2. Distribution par championnat

| Championnat | # Outliers |
|---|---|
| Premier League | 14 |
| La Liga | 10 |
| Ligue 1 | 10 |
| Serie A | 9 |
| Bundesliga | 7 |

---

## 3. Distribution par résultat réel

| Résultat | # | % |
|---|---|---|
| H | 4 | 8.0% |
| D | 0 | 0.0% |
| A | 46 | 92.0% |

**% Nuls :** 0.0% (baseline Big5 ≈ 26%) → ✅ Normal

---

## 4. Features diagnostiques — valeurs NULL (outliers uniquement)

| Feature | % NULL | Criticité |
|---|---|---|
| `np_xg_roll_5` | 0.0% | 🟢 OK |
| `ppda_roll_5` | 0.0% | 🟢 OK |
| `xg_overperformance_5` | 0.0% | 🟢 OK |
| `sterility_index_5` | 0.0% | 🟢 OK |
| `press_resistance_5` | 0.0% | 🟢 OK |
| `ws_shots_ot_pg` | 0.0% | 🟢 OK |

---

## 5. Signaux xG

**xG net moyen (outliers)** : `+0.3670`  
_Un xG net positif chez les équipes qui ont mal performé = occasion ratées / malchance_

**xG overperformance index moyen** : `-0.0012`

---

## 6. Les 50 matchs les plus catastrophiques

| Match ID | Équipe | Adversaire | Ligue | Saison | Résultat | Log Loss | WS |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fbref_2adbcf6223 | Roma | Milan | Serie A | 2023-2024 | A | 2.6002 | ✅ |
| fbref_c08951dd8f | Brighton & Hove Albion | Fulham | Premier League | 2022-2023 | A | 2.5672 | ✅ |
| fbref_57fce50593 | FC Cologne | Union Berlin | Bundesliga | 2022-2023 | A | 2.5672 | ✅ |
| fbref_ee68b1b8e1 | Monaco | Reims | Ligue 1 | 2022-2023 | A | 2.5603 | ✅ |
| fbref_0073004068 | Lens | Nice | Ligue 1 | 2022-2023 | A | 2.5603 | ✅ |
| fbref_0d883bffb6 | Barcelona | Villarreal | La Liga | 2023-2024 | A | 2.1326 | ❌ |
| fbref_1bd404a39f | Brighton & Hove Albion | Everton | Premier League | 2022-2023 | A | 2.1326 | ✅ |
| fbref_e5cb161fc2 | Angers | Brest | Ligue 1 | 2022-2023 | A | 2.1326 | ✅ |
| fbref_bd0c4b5252 | Chelsea | Nottingham Forest | Premier League | 2023-2024 | A | 2.1326 | ✅ |
| fbref_b209ae6ed8 | Aston Villa | West Ham United | Premier League | 2022-2023 | A | 2.1326 | ✅ |
| fbref_1128e21c02 | Napoli | Milan | Serie A | 2022-2023 | A | 2.1326 | ✅ |
| fbref_f755fa7a52 | Olympique de Marseille | Nice | Ligue 1 | 2022-2023 | A | 2.1228 | ✅ |
| fbref_dcfd2dcc72 | Chelsea | Aston Villa | Premier League | 2022-2023 | A | 2.1081 | ✅ |
| fbref_475d8cd596 | Las Palmas | Almería | La Liga | 2023-2024 | A | 2.0988 | ❌ |
| fbref_86b20d55a6 | Milan | Udinese | Serie A | 2023-2024 | A | 2.0988 | ✅ |
| fbref_17d54688f2 | Spezia | Hellas Verona | Serie A | 2022-2023 | A | 2.0938 | ❌ |
| fbref_8f9a255dbd | Luton | Sheffield United | Premier League | 2023-2024 | A | 2.0938 | ✅ |
| fbref_44b4344f95 | Lille | Reims | Ligue 1 | 2023-2024 | A | 2.0585 | ✅ |
| fbref_1ff2afda16 | Monaco | Montpellier | Ligue 1 | 2022-2023 | A | 2.0585 | ✅ |
| fbref_293e639d2a | Chelsea | Wolves | Premier League | 2023-2024 | A | 2.0585 | ✅ |
| fbref_f3c0294dc4 | FC Cologne | Bochum | Bundesliga | 2022-2023 | A | 2.0585 | ✅ |
| fbref_4e51a176dc | Cádiz | Valencia | La Liga | 2023-2024 | A | 2.0585 | ❌ |
| fbref_3d9a0ead18 | Olympique de Marseille | Brest | Ligue 1 | 2022-2023 | A | 2.0585 | ✅ |
| fbref_77d6639423 | Manchester City | Brentford | Premier League | 2022-2023 | A | 2.0578 | ✅ |
| fbref_7a2ecf59af | Chelsea | Aston Villa | Premier League | 2023-2024 | A | 2.0578 | ✅ |
| fbref_2dc1d95644 | Olympique de Marseille | Ajaccio | Ligue 1 | 2022-2023 | A | 2.0299 | ✅ |
| fbref_0a60aa1f82 | Brighton & Hove Albion | Aston Villa | Premier League | 2022-2023 | A | 2.0077 | ✅ |
| fbref_7b575ac6f2 | Lille | Lyon | Ligue 1 | 2023-2024 | A | 2.0077 | ✅ |
| fbref_31dfa69325 | Valencia | Mallorca | La Liga | 2022-2023 | A | 1.9985 | ❌ |
| fbref_f215ec06e1 | Angers | Reims | Ligue 1 | 2022-2023 | A | 1.9974 | ✅ |
| fbref_1c35695376 | Internazionale | Empoli | Serie A | 2022-2023 | A | 1.9794 | ✅ |
| fbref_adad0611f1 | FC Cologne | Freiburg | Bundesliga | 2022-2023 | A | 1.9566 | ✅ |
| fbref_1355ab975e | Wolfsburg | Hertha Berlin | Bundesliga | 2022-2023 | A | 1.9482 | ✅ |
| fbref_be02dda72f | Valencia | Cádiz | La Liga | 2022-2023 | A | 1.9482 | ❌ |
| fbref_13667d9ed2 | Real Betis | Cádiz | La Liga | 2022-2023 | A | 1.9219 | ❌ |
| fbref_aa9d2e8500 | Newcastle United | Liverpool | Premier League | 2023-2024 | A | 1.9219 | ✅ |
| fbref_8fe6c57537 | Sevilla | Cádiz | La Liga | 2023-2024 | A | 1.9219 | ❌ |
| fbref_b4461e1922 | Mallorca | Sevilla | La Liga | 2022-2023 | A | 1.9219 | ❌ |
| fbref_e6e5dd6c2b | Mainz 05 | Werder Bremen | Bundesliga | 2023-2024 | A | 1.9219 | ✅ |
| fbref_01ec19ec4e | Bologna | Monza | Serie A | 2022-2023 | A | 1.9219 | ✅ |
| fbref_76ce5a584c | Torino | Lazio | Serie A | 2023-2024 | A | 1.9219 | ✅ |
| fbref_1cf85a7c4c | Fulham | Brentford | Premier League | 2023-2024 | A | 1.9219 | ✅ |
| fbref_2a51fc6e54 | Real Sociedad | Real Valladolid | La Liga | 2022-2023 | A | 1.8990 | ❌ |
| fbref_b2b53b3e01 | Chelsea | Southampton | Premier League | 2022-2023 | A | 1.8990 | ✅ |
| fbref_e3e3ef7a21 | Fulham | Brighton & Hove Albion | Premier League | 2023-2024 | H | 1.8935 | ✅ |
| fbref_19fe51f6cc | Spezia | Internazionale | Serie A | 2022-2023 | H | 1.8866 | ✅ |
| fbref_e842c539c0 | FC Heidenheim | VfB Stuttgart | Bundesliga | 2023-2024 | H | 1.8866 | ✅ |
| fbref_0292457ce9 | Sassuolo | Atalanta | Serie A | 2022-2023 | H | 1.8866 | ✅ |
| fbref_1b69b26ba1 | Real Betis | Atlético Madrid | La Liga | 2022-2023 | A | 1.8723 | ❌ |
| fbref_d44239c23a | Augsburg | FC Cologne | Bundesliga | 2022-2023 | A | 1.8723 | ✅ |

---

## 7. Hypothèses et pistes d'action

- _Aucun pattern dominant. L'erreur est distribuée uniformément — considérer un audit par type de match ou par tranche de cotes._

---

> Rapport généré par `agents/auditor.py` — Projet 3-Étoiles  
> Features candidates : `python pipelines/03c_suggested_features.py --dry-run`