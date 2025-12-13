#!/usr/bin/env bash
# ==============================================================================
# Chapter 02 — Script 02: Delete Dataproc Cluster (Cost Cleanup)
# Reference Notebook: ../02_cluster_setup_and_management.ipynb
#
# What this script does:
#   Deletes the Dataproc cluster (`DATAPROC_CLUSTER_NAME`) while preserving your
#   data and scripts in Cloud Storage (`gs://${DATAPROC_BUCKET}`).
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CHAPTER_DIR}/.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
else
  echo "Error: .env file not found at ${REPO_ROOT}/.env."
  exit 1
fi

echo "==> Deleting Dataproc cluster '${DATAPROC_CLUSTER_NAME}' in '${GCP_REGION}'..."
gcloud dataproc clusters delete "${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --quiet

echo "Cluster '${DATAPROC_CLUSTER_NAME}' deleted."
