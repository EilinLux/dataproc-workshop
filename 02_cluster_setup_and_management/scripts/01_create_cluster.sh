#!/usr/bin/env bash
# ==============================================================================
# Chapter 02 — Script 01: Provision Dataproc Cluster & Upload Sample CSV Data
# Reference Notebook: ../02_cluster_setup_and_management.ipynb
#
# What this script does:
#   1. Loads project, region, cluster, and bucket variables from the root `.env`.
#   2. Enables Private Google Access on the target subnetwork so `--no-address`
#      (internal-IP-only) cluster nodes can reach Cloud Storage and BigQuery APIs.
#   3. Creates the Cloud Storage bucket (`gs://${DATAPROC_BUCKET}`) if needed and
#      uploads `../data/sales_data.csv`.
#   4. Creates the Dataproc cluster (`${DATAPROC_CLUSTER_NAME}`) with Component
#      Gateway and the Jupyter optional component enabled.
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
  echo "Error: .env file not found at ${REPO_ROOT}/.env. Run 'cp .env.example .env' first."
  exit 1
fi

echo "==> [1/4] Configuring gcloud project '${GCP_PROJECT_ID}' and region '${GCP_REGION}'..."
gcloud config set project "${GCP_PROJECT_ID}"
gcloud config set dataproc/region "${GCP_REGION}"

echo "==> [2/4] Ensuring Private Google Access is enabled on subnetwork '${SUBNET_NAME}'..."
gcloud compute networks subnets update "${SUBNET_NAME}" \
  --region="${GCP_REGION}" \
  --enable-private-ip-google-access \
  --project="${GCP_PROJECT_ID}" || true

echo "==> [3/4] Creating GCS bucket 'gs://${DATAPROC_BUCKET}' (if needed) and uploading sales_data.csv..."
if ! gcloud storage buckets describe "gs://${DATAPROC_BUCKET}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${DATAPROC_BUCKET}" \
    --location="${GCP_REGION}" \
    --uniform-bucket-level-access \
    --project="${GCP_PROJECT_ID}"
fi

gcloud storage cp "${CHAPTER_DIR}/data/sales_data.csv" \
  "gs://${DATAPROC_BUCKET}/raw_data/sales/sales_data.csv" \
  --project="${GCP_PROJECT_ID}"

echo "==> [4/4] Creating Dataproc cluster '${DATAPROC_CLUSTER_NAME}'..."
gcloud dataproc clusters create "${DATAPROC_CLUSTER_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --subnet="${SUBNET_NAME}" \
  --bucket="${DATAPROC_BUCKET}" \
  --enable-component-gateway \
  --optional-components=JUPYTER \
  --image-version="${DATAPROC_IMAGE_VERSION}" \
  --master-machine-type="${MASTER_MACHINE_TYPE}" \
  --worker-machine-type="${WORKER_MACHINE_TYPE}" \
  --worker-boot-disk-size="${WORKER_BOOT_DISK_SIZE}" \
  --no-address \
  --labels="mode=workshop,user=zelda"

echo "Dataproc cluster '${DATAPROC_CLUSTER_NAME}' created successfully!"
