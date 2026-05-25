# ADR-007 — Docker comme environnement de déploiement

## Statut

Adopté — 17/05/2026

## Contexte

Le pipeline 3-Étoiles tourne localement sur Windows 11 avec un venv Python. Ce setup fonctionne en développement mais pose deux problèmes dès qu'on veut aller plus loin :

1. **Reproductibilité** : "ça marche sur ma machine" — les dépendances système, les versions de librairies, et les variables d'environnement diffèrent entre machines
2. **Déploiement** : pour faire tourner le pipeline sur un serveur cloud (GCS, GCE), il faut garantir que l'environnement est identique à celui du développement

Les besoins identifiés :

- Empaqueter le pipeline avec tout son environnement
- Faire tourner Prefect et MLflow comme services séparés mais coordonnés
- Préparer le projet pour un déploiement cloud (Bloc GCS/Terraform)

### Options considérées

|Option|Profil|
|---|---|
|**Docker + docker-compose**|Standard industrie, léger, reproductible, excellent support cloud|
|**Machine virtuelle**|Isolation totale, lourd (OS complet), lent à démarrer|
|**venv seul**|Simple, pas d'isolation OS, dépendances système non gérées|
|**Conda**|Gestion des dépendances Python + système, pas de déploiement natif|
|**Podman**|Alternative Docker sans daemon root, moins répandu|

---

## Décision

**Docker avec docker-compose** est adopté pour l'environnement de déploiement.

Trois services sont définis dans `docker-compose.yml` :

- `pipeline` — construit depuis le `Dockerfile` du projet
- `prefect` — image officielle `prefecthq/prefect:3-latest`
- `mlflow` — image dédiée construite depuis `Dockerfile.mlflow`

---

## Justification

### Docker vs Machine Virtuelle

Une VM virtualise le **matériel** — elle émule un ordinateur complet avec un OS entier. Docker virtualise l'**OS** — il partage le noyau Linux de la machine hôte et isole seulement les processus, le réseau, et le système de fichiers.

Conséquences pratiques :

|Dimension|VM|Docker|
|---|---|---|
|Temps de démarrage|Minutes|Secondes|
|Taille|10-20 Go (OS complet)|100-500 Mo|
|RAM au repos|1-4 Go (OS invité)|Quasi nulle|
|Isolation|Totale (noyau séparé)|Processus (noyau partagé)|
|Portabilité|Fichier .vmdk lourd|Image légère sur registry|

Pour notre usage (déployer une application Python), Docker est le bon outil. Une VM serait justifiée si on avait besoin d'un OS différent ou d'une isolation de sécurité totale.

### Dockerfile dédié pour MLflow

L'approche initiale (installer MLflow via `command` au démarrage) posait deux problèmes :

- Installation longue à chaque démarrage (1-2 minutes de `pip install`)
- Healthcheck qui expirait avant que le serveur soit prêt

La solution adoptée est un `Dockerfile.mlflow` dédié avec MLflow pré-installé. Le démarrage du service est immédiat — MLflow est dans l'image.

### Volumes — bind mounts pour les données du projet

Les données produites par le pipeline (modèles, logs, métriques MLflow) sont montées via des **bind mounts** — des dossiers de la machine hôte montés dans le conteneur. Les données survivent à l'arrêt des conteneurs et sont directement accessibles depuis la machine de développement.

La base de données Prefect utilise un **volume nommé** géré par Docker — elle n'a pas besoin d'être inspectée directement.

### Réseau bridge et communication inter-services

docker-compose crée automatiquement un réseau bridge privé. Les services se parlent via leurs noms de service comme hostnames (`http://mlflow:5000`, `http://prefect:4200/api`). Les ports sont publiés vers la machine hôte uniquement pour l'accès depuis le navigateur.

---

## Conséquences

### Positives

- Environnement reproductible sur n'importe quelle machine avec Docker installé
- Prefect et MLflow démarrés en une commande (`docker-compose up -d prefect mlflow`)
- Prêt pour le déploiement cloud — les images peuvent être poussées sur GCR
- Isolation : le pipeline ne pollue pas l'environnement système

### Négatives et limites

- **Overhead Windows** : Docker Desktop sur Windows utilise une VM Linux légère (WSL2) en coulisses — légère mais pas nulle
- **Healthcheck Prefect** : l'image officielle Prefect ne contient pas `curl` — le healthcheck est désactivé. Le service fonctionne mais Docker ne peut pas vérifier automatiquement son état
- **MLflow filesystem deprecated** : MLflow 3.x recommande un backend SQLite plutôt que le filesystem `mlruns/`. Migration à prévoir si le projet évolue vers un MLflow Server partagé
- **Pas de multi-stage build** : l'image pipeline embarque les outils de compilation (gcc, g++). Une optimisation future serait un multi-stage build pour une image de production plus légère

---

## Évolutions prévues

- **Multi-stage build** pour réduire la taille de l'image pipeline
- **Registry GCR** — pousser les images sur Google Container Registry pour le déploiement cloud
- **MLflow SQLite backend** — migrer de `mlruns/` filesystem vers `sqlite:///mlflow.db`
- **Kubernetes** — si le projet évolue vers plusieurs workers ou un déploiement multi-machine

---

## Questions d'entretien anticipées

**"Quelle est la différence entre une image et un conteneur Docker ?"**

Une image est un instantané figé et immuable de l'environnement — le moule. Un conteneur est une instance en cours d'exécution de cette image — le gâteau. Plusieurs conteneurs peuvent tourner depuis la même image simultanément. L'image est construite une fois avec `docker build`, le conteneur est instancié à la demande avec `docker run`.

**"Pourquoi l'ordre des instructions dans un Dockerfile est-il important ?"**

À cause du système de cache par couches. Chaque instruction crée une couche mise en cache par Docker. Dès qu'une couche change, toutes les suivantes sont reconstruites. On place donc ce qui change rarement en haut (image de base, dépendances système, librairies Python) et ce qui change souvent en bas (code source). Sans cette discipline, `pip install` se réexécute à chaque modification de code.

**"Quelle est la différence entre `docker run` et `docker-compose` ?"**

`docker run` lance un seul conteneur. `docker-compose` orchestre plusieurs conteneurs qui fonctionnent ensemble — il gère le réseau entre eux, les volumes partagés, l'ordre de démarrage via `depends_on`, et les dépendances. Pour une application avec plusieurs services, `docker-compose` est indispensable.

**"Qu'est-ce qu'un multi-stage build ?"**

Un multi-stage build sépare la phase de compilation de la phase d'exécution dans un seul Dockerfile. On utilise une image lourde avec tous les outils de compilation pour le premier stage, et on copie uniquement les artefacts compilés dans une image légère pour le stage final. Résultat : des images de production significativement plus légères, avec moins de surface d'attaque de sécurité.

**"Quelle est la différence entre Docker et Kubernetes ?"**

Docker fait tourner des conteneurs sur une machine. Kubernetes orchestre des centaines de conteneurs sur des dizaines de machines — il gère la répartition de charge, le redémarrage automatique en cas de panne, et la mise à l'échelle automatique selon la charge. Docker est l'outil, Kubernetes est le chef d'orchestre à grande échelle. Pour un projet mono-machine, `docker-compose` suffit largement.