#!/usr/bin/env bash
# Enchaîne les tâches rolling_chunk manquantes, s'arrête avant la limite de 180 s de l'appareil.
# Usage : bash tools/run_chunks.sh <tag> <n_seeds> [options rolling_chunk, ex. --market open]
tag=$1; ns=$2; shift 2
start=$(date +%s)
for s in 2019-2020 2020-2021 2021-2022 2022-2023 2023-2024 2024-2025 2025-2026; do
  for ((k=0; k<ns; k++)); do
    f="reports/chunks/$tag/${s}_s${k}.csv"; [ -f "$f" ] && continue
    [ $(( $(date +%s) - start )) -gt 85 ] && { echo "PAUSE : relancer la même commande"; exit 0; }
    "$HOME/ml313/bin/python" "${SCRIPT:-tools/rolling_chunk.py}" --test-season "$s" --seed-index "$k" --tag "$tag" "$@" 2>&1 | tail -1
  done
done
echo "TOUT TERMINÉ"
