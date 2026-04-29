# 📈 Agent Stratégie — Rapport d'Audit

**Généré le :** 2026-04-27 16:34  
**Source :** `models/predictions_val.csv`  
**Matchs analysés :** 1789

---

## 1. Signal brut — Mise fixe

| Métrique | Valeur |
|---|---|
| Matchs | 1789 |
| Taux de réussite | 51.09% |
| ROI mise fixe | **+1.53%** |
| Profit total | +27.34 u |
| Cote moyenne jouée | 2.27 |
| Verdict | ✅ Signal positif |

### Par outcome

| Outcome | N | WR | ROI | Cote moy |
|---|---|---|---|---|
| ❌ **H** | 1116 | 54.12% | -0.85% | 2.11 |
| ✅ **D** | 149 | 38.26% | +32.81% | 3.55 |
| ❌ **A** | 524 | 48.28% | -2.30% | 2.24 |

### Par ligue

| Ligue | N | WR | ROI |
|---|---|---|---|
| ✅ La Liga | 380 | 48.95% | +3.18% |
| ✅ Premier League | 361 | 54.85% | +2.73% |
| ✅ Ligue 1 | 311 | 50.80% | +2.04% |
| ✅ Bundesliga | 319 | 49.53% | +1.90% |
| ❌ Serie A | 418 | 51.20% | -1.67% |

---

## 2. Modèle vs Marché

| Métrique | Valeur |
|---|---|
| Proba modèle moy (sur résultat réel) | 0.3911 |
| Proba marché moy (sur résultat réel) | 0.4203 |
| Gap moyen (modèle − marché) | **-0.0292** |
| % matchs modèle > marché | 40.92% |
| Verdict | ❌ Marché meilleur que le modèle |

### Gap par ligue

| Ligue | Gap moyen | N | Verdict |
|---|---|---|---|
| Bundesliga | -0.0196 | 319 | ❌ |
| Ligue 1 | -0.0240 | 311 | ❌ |
| Serie A | -0.0274 | 418 | ❌ |
| La Liga | -0.0294 | 380 | ❌ |
| Premier League | -0.0442 | 361 | ❌ |

---

## 3. Optimisation stratégie

### Config optimale recommandée

| Paramètre | Valeur |
|---|---|
| `edge_min` | **15%** |
| `confidence_min` | **60%** |
| Paris sélectionnés | 40.0 |
| Taux de réussite | 50.00% |
| ROI | ✅ **+8.35%** |
| Profit | +3.34 u |

### Top 10 configurations (grille complète)

| edge_min | conf_min | N | WR | ROI | Profit |
|---|---|---|---|---|---|
| 15% | 60% | 40 | 50.00% | ✅ +8.35% | +3.34 |
| 2% | 60% | 124 | 60.48% | ✅ +8.33% | +10.33 |
| 4% | 60% | 100 | 58.00% | ✅ +8.02% | +8.02 |
| 8% | 60% | 75 | 53.33% | ✅ +5.43% | +4.07 |
| 6% | 60% | 85 | 52.94% | ✅ +2.99% | +2.54 |
| 10% | 60% | 64 | 50.00% | ✅ +2.09% | +1.34 |
| 12% | 60% | 56 | 48.21% | ✅ +0.77% | +0.43 |
| 2% | 55% | 230 | 52.17% | ❌ -0.27% | -0.61 |
| 2% | 45% | 486 | 45.47% | ❌ -0.35% | -1.68 |
| 15% | 40% | 198 | 32.83% | ❌ -2.67% | -5.28 |

### ROI par ligue avec config optimale

| Ligue | N | WR | ROI | Profit |
|---|---|---|---|---|
| ✅ Ligue 1 | 11 | 54.55% | +15.73% | +1.73 |
| ❌ Bundesliga | 10 | 40.00% | -10.10% | -1.01 |

---

## 4. Recommandations

- **Ligues à cibler** : Bundesliga, La Liga, Ligue 1, Premier League sont profitables en mise fixe.
- **⚠️ Gap modèle−marché : -0.0292**. Le marché est meilleur que le modèle. Priorité : enrichir les features avant d'optimiser la stratégie.
- **Config suggérée pour `config.yaml`** : `edge_min: 0.15` | `confidence_min: 0.6`

---

> Rapport généré par `pipeline_agents/strategy_auditor.py` — Projet 3-Étoiles