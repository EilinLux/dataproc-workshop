#!/usr/bin/env bash
# ==============================================================================
# Chapter 02 — Script 03: Import & Attach a Dataproc Autoscaling Policy
# Reference Notebook: ../02_cluster_setup_and_management.ipynb ("Autoscaling")
#
# What this script does:
#   1. Imports the declarative YAML autoscaling policy
#      (`../config/my_autoscaling_policy.yaml`) into Google Cloud Dataproc in
#      `${GCP_REGION}` using `gcloud dataproc autoscaling-policies import`.
#   2. Attaches the policy (`my-workshop-autoscaling-policy-yaml`) to the
#      running Dataproc cluster (`${DATAPROC_CLUSTER_NAME}`).
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

AUTOSCALING_POLICY_ID="${AUTOSCALING_POLICY_ID:-my-workshop-autoscaling-policy-yaml}"
POLICY_FILE="${CHAPTER_DIR}/config/my_autoscaling_policy.yaml"

echo "==> [1/2] Importing Dataproc Autoscaling Policy '${AUTOSCALING_POLICY_ID}' from '${POLICY_FILE}'..."
gcloud dataproc autoscaling-policies import "${AUTOSCALING_POLICY_ID}" \
  --source="${POLICY_FILE}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --quiet

echo "==> [2/2] Attaching Autoscaling Policy '${AUTOSCALING_POLICY_ID}' to cluster '${DATAPROC_CLUSTER_NAME}'..."
gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --autoscaling-policy="${AUTOSCALING_POLICY_ID}"

echo "Autoscaling policy '${AUTOSCALING_POLICY_ID}' is now active on cluster '${DATAPROC_CLUSTER_NAME}'!"
