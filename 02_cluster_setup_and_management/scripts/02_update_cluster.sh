#!/usr/bin/env bash
# ==============================================================================
# Chapter 02 — Script 02: Update a Running Dataproc Cluster
# Reference Notebook: ../02_cluster_setup_and_management.ipynb ("Update a cluster")
#
# What this script does:
#   Demonstrates the four live cluster update operations covered in the notebook
#   without recreating the cluster:
#     1. Scale out primary workers (`--num-workers=3`) and add secondary
#        (preemptible/Spot) workers (`--num-secondary-workers=2`).
#     2. Scale back down to 2 primary workers and 0 secondary workers using
#        YARN Graceful Decommissioning (`--graceful-decommission-timeout=3600s`)
#        so in-flight Spark tasks complete before nodes are terminated.
#     3. Add / update FinOps resource labels (`--update-labels`).
#     4. Remove a specific resource label (`--remove-labels`).
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CHAPTER_DIR}/.." && pwd)"

if command -v uv >/dev/null 2>&1; then
  UV_PY312="$(uv python find 3.12 2>/dev/null || true)"
  if [[ -n "${UV_PY312}" ]]; then
    export CLOUDSDK_PYTHON="${UV_PY312}"
  fi
fi

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
else
  echo "Error: .env file not found at ${REPO_ROOT}/.env. Run 'cp .env.example .env' first."
  exit 1
fi

SCALE_UP_WORKERS="${SCALE_UP_WORKERS:-3}"
SECONDARY_WORKERS="${SECONDARY_WORKERS:-2}"
BASE_WORKERS="${BASE_WORKERS:-2}"
DECOMMISSION_TIMEOUT="${DECOMMISSION_TIMEOUT:-3600s}"

echo "==> [1/4] Scaling out cluster '${DATAPROC_CLUSTER_NAME}' to ${SCALE_UP_WORKERS} primary workers and ${SECONDARY_WORKERS} secondary (preemptible) workers..."
gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --num-workers="${SCALE_UP_WORKERS}" \
  --num-secondary-workers="${SECONDARY_WORKERS}"

echo "==> [2/4] Scaling down cluster '${DATAPROC_CLUSTER_NAME}' to ${BASE_WORKERS} primary workers (0 secondary) with Graceful Decommissioning (${DECOMMISSION_TIMEOUT})..."
gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --num-workers="${BASE_WORKERS}" \
  --num-secondary-workers=0 \
  --graceful-decommission-timeout="${DECOMMISSION_TIMEOUT}"

echo "==> [3/4] Adding/updating FinOps labels (env=production, cost_center=research, user=eilinlux)..."
gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --update-labels="env=production,cost_center=research,user=eilinlux"

echo "==> [4/4] Removing temporary label 'cost_center'..."
gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --remove-labels="cost_center"

echo "Cluster '${DATAPROC_CLUSTER_NAME}' update operations completed successfully!"
