#!/usr/bin/env bash
# ==============================================================================
# Chapter 02 — Script 04: Lifecycle Auto-Termination (--max-idle / --max-age) & Delete Cluster
# Reference Notebook: ../02_cluster_setup_and_management.ipynb ("Delete a cluster")
#
# What this script does:
#   1. If `MODE=configure-lifecycle` is passed, updates the running cluster with
#      `--max-idle` (default: `30m`, deletes after 30 minutes of inactivity) or
#      `--max-age` (scheduled deletion timer) so you never pay for forgotten VMs.
#   2. Otherwise (default `MODE=delete`), immediately deletes the Dataproc
#      cluster (`${DATAPROC_CLUSTER_NAME}`) while preserving your data and
#      scripts in Cloud Storage (`gs://${DATAPROC_BUCKET}`).
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
  echo "Error: .env file not found at ${REPO_ROOT}/.env."
  exit 1
fi

MODE="${1:-delete}"
MAX_IDLE_DURATION="${MAX_IDLE_DURATION:-30m}"

if [[ "${MODE}" == "configure-lifecycle" ]]; then
  echo "==> Configuring idle auto-deletion (--max-idle=${MAX_IDLE_DURATION}) on cluster '${DATAPROC_CLUSTER_NAME}'..."
  gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" \
    --region="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --max-idle="${MAX_IDLE_DURATION}"
  echo "Cluster '${DATAPROC_CLUSTER_NAME}' will now auto-delete after ${MAX_IDLE_DURATION} of idle time."
else
  echo "==> Deleting Dataproc cluster '${DATAPROC_CLUSTER_NAME}' in '${GCP_REGION}'..."
  gcloud dataproc clusters delete "${DATAPROC_CLUSTER_NAME}" \
    --region="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet
  echo "Cluster '${DATAPROC_CLUSTER_NAME}' deleted."
fi
