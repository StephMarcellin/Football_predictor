import os

VAULT = r"C:\Obsidian\3_étoiles_obsidian"

structure = {
    "00 - Index": {
        "Home.md": """# 🧠 ML Pipeline — Documentation

## Pipelines
[[01_ingest]] → [[02_process]] → [[03_features]] → [[04_train]] → [[05_predict]] → [[06_backtest]]

## Modules Features
[[draw]] · [[rolling]] · [[whoscored]] · [[columns]]

## Scraping
[[scrape_fbref]] · [[scrape_transfermarkt]] · [[scrape_whoscored]] · [[scrape_understat]]

## Agents & Orchestration
[[agent_manager]] · [[agent_gemini]] · [[run_pipeline]]

## Décisions & Debug
[[journal des choix techniques]] · [[audit_draw_residuals]]
"""
    },
    "01 - Pipelines": {
        "01_ingest.md": """# 01_ingest.py

## Rôle
Ingestion des données brutes depuis les sources externes.

## Entrées
- Sources scrappées (fbref, transfermarkt, whoscored, understat)

## Sorties
- Données brutes structurées

## Dépendances
- [[scrape_fbref]]
- [[scrape_transfermarkt]]
- [[scrape_whoscored]]
- [[scrape_understat]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "01b_odds.md": """# 01b_odds.py

## Rôle
Ingestion des cotes (odds) pour les matchs.

## Entrées


## Sorties


## Dépendances
- [[01_ingest]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "02_process.md": """# 02_process.py

## Rôle
Nettoyage et transformation des données brutes.

## Entrées
- Sortie de [[01_ingest]]

## Sorties


## Dépendances
- [[01_ingest]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "03_features.md": """# 03_features.py

## Rôle
Calcul des features pour le modèle ML.

## Entrées
- Sortie de [[02_process]]

## Sorties


## Dépendances
- [[02_process]]
- [[draw]]
- [[rolling]]
- [[whoscored]]
- [[columns]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "04_train.md": """# 04_train.py

## Rôle
Entraînement du modèle de prédiction.

## Entrées
- Features issues de [[03_features]]

## Sorties
- Modèle entraîné

## Dépendances
- [[03_features]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "05_predict.md": """# 05_predict.py

## Rôle
Génération des prédictions sur de nouvelles données.

## Entrées
- Modèle de [[04_train]]
- Nouvelles données

## Sorties
- Prédictions

## Dépendances
- [[04_train]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "06_backtest.md": """# 06_backtest.py

## Rôle
Évaluation des performances des prédictions sur données historiques.

## Entrées
- Prédictions de [[05_predict]]

## Sorties
- Métriques de performance

## Dépendances
- [[05_predict]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
    },
    "02 - Features": {
        "draw.md": """# draw.py

## Rôle
Features liées au comportement nul / draw.

## Utilisé dans
- [[03_features]]

## Variables produites


## Remarques


""",
        "rolling.md": """# rolling.py

## Rôle
Calcul des features glissantes (rolling windows).

## Utilisé dans
- [[03_features]]

## Variables produites


## Remarques


""",
        "whoscored.md": """# whoscored.py

## Rôle
Features issues des données WhoScored.

## Utilisé dans
- [[03_features]]

## Variables produites


## Remarques


""",
        "columns.md": """# columns.py

## Rôle
Définition et gestion des colonnes du dataset.

## Utilisé dans
- [[03_features]]
- [[config_columns]]

## Remarques


""",
    },
    "03 - Scraping": {
        "scrape_fbref.md": """# scrape_fbref.py

## Rôle
Scraping des données depuis FBref.

## Données collectées


## Fréquence de mise à jour


## Problèmes connus
- [ ] 
""",
        "scrape_transfermarkt.md": """# scrape_transfermarkt.py

## Rôle
Scraping des données depuis Transfermarkt.

## Données collectées


## Fréquence de mise à jour


## Problèmes connus
- [ ] 
""",
        "scrape_whoscored.md": """# scrape_whoscored.py

## Rôle
Scraping des données depuis WhoScored.

## Données collectées


## Fréquence de mise à jour


## Problèmes connus
- [ ] 
""",
        "scrape_understat.md": """# scrape_understat.py

## Rôle
Scraping des données depuis Understat (xG, etc.).

## Données collectées


## Fréquence de mise à jour


## Problèmes connus
- [ ] 
""",
    },
    "04 - Architecture": {
        "Vue d'ensemble.md": """# Vue d'ensemble du projet

## Schéma du pipeline

[[01_ingest]] → [[01b_odds]] → [[02_process]] → [[03_features]] → [[04_train]] → [[05_predict]] → [[06_backtest]]

## Sources de données
| Source | Script | Type |
|--------|--------|------|
| FBref | [[scrape_fbref]] | Stats matchs |
| Transfermarkt | [[scrape_transfermarkt]] | Valeurs joueurs |
| WhoScored | [[scrape_whoscored]] | Stats avancées |
| Understat | [[scrape_understat]] | xG / xA |

## Modules features
| Module | Rôle |
|--------|------|
| [[draw]] | Features nul |
| [[rolling]] | Fenêtres glissantes |
| [[whoscored]] | Stats WhoScored |
| [[columns]] | Gestion colonnes |

## Agents
- [[agent_manager]] — orchestration
- [[agent_gemini]] — IA Gemini
""",
    },
    "05 - Agents": {
        "agent_manager.md": """# agent_manager.py

## Rôle
Orchestration des agents du pipeline.

## Dépendances
- [[agent_gemini]]
- [[run_pipeline]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "agent_gemini.md": """# agent_gemini.py

## Rôle
Agent basé sur Gemini pour l'analyse ou la génération.

## Dépendances
- [[agent_manager]]

## Comportement attendu


## Problèmes connus / TODO
- [ ] 
""",
        "run_pipeline.md": """# run_pipeline.py

## Rôle
Point d'entrée principal pour lancer le pipeline complet.

## Usage
```bash
python run_pipeline.py
```

## Dépendances
- [[01_ingest]] → [[06_backtest]]

## Options / Arguments


""",
    },
    "06 - Décisions": {
        "journal des choix techniques.md": """# Journal des choix techniques

## Format d'entrée
```
### YYYY-MM-DD — Titre du choix
**Contexte** : ...
**Décision** : ...
**Raison** : ...
**Alternatives écartées** : ...
```

---

### 2026-05-11 — Initialisation du vault Obsidian
**Contexte** : Besoin de documenter le projet ML pipelines  
**Décision** : Utiliser Obsidian avec une structure par domaine  
**Raison** : Liens entre notes, graph view, fichiers locaux  
**Alternatives écartées** : Notion (cloud), Confluence (trop lourd)
""",
    },
    "07 - Debug & Diagnostics": {
        "audit_draw_residuals.md": """# audit_draw_residuals.py

## Rôle
Audit des résidus du modèle sur les matchs nuls.

## Lié à
- [[draw]]
- [[06_backtest]]

## Observations


## Actions correctives
- [ ] 
""",
        "diagnostic_model_signal.md": """# diagnostic_model_signal.py

## Rôle
Diagnostic du signal du modèle ML.

## Lié à
- [[04_train]]
- [[05_predict]]

## Observations


## Actions correctives
- [ ] 
""",
    },
}

def create_vault(vault_path, structure):
    created_dirs = 0
    created_files = 0

    for folder, files in structure.items():
        folder_path = os.path.join(vault_path, folder)
        os.makedirs(folder_path, exist_ok=True)
        created_dirs += 1

        for filename, content in files.items():
            file_path = os.path.join(folder_path, filename)
            if not os.path.exists(file_path):
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                created_files += 1

    return created_dirs, created_files

if __name__ == "__main__":
    print(f"📁 Création du vault dans : {VAULT}")
    dirs, files = create_vault(VAULT, structure)
    print(f"✅ {dirs} dossiers créés")
    print(f"✅ {files} notes créées")
    print(f"\n🚀 Ouvre Obsidian et recharge ton vault — tout est prêt !")