#!/usr/bin/env bash
# Recrée/valide l'environnement d'entraînement Linux (miroir du .venv Windows).
# Idempotent. Première exécution ~2-3 min : lancer avec un timeout >= 175 s, relancer si coupé.
# Usage : bash tools/vm_bootstrap.sh [dossier_env]   (défaut : $HOME/ml313)
set -euo pipefail
ENV_DIR="${1:-$HOME/ml313}"
[ -x "$ENV_DIR/bin/python" ] || uv venv -q --python 3.13 "$ENV_DIR"
uv pip install -q --python "$ENV_DIR/bin/python" \
  duckdb==1.5.5 lightgbm==4.6.0 pandas==2.3.3 scikit-learn==1.8.0 \
  pyarrow==23.0.1 numpy==2.4.4 joblib==1.5.3 pyyaml python-dotenv loguru
"$ENV_DIR/bin/python" - <<'PY' || { echo "Env incohérent : rm -rf $ENV_DIR puis relancer."; exit 1; }
import sys, duckdb, lightgbm, sklearn
print("OK", sys.version.split()[0], "duckdb", duckdb.__version__, "lightgbm", lightgbm.__version__, "sklearn", sklearn.__version__)
PY
