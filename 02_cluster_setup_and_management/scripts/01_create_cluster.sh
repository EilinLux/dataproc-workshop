#!/usr/bin/env bash
# ==============================================================================
# Chapter 02 — Script 01: Provision Dataproc Cluster & Upload Sample CSV Data
# Reference Notebook: ../02_cluster_setup_and_management.ipynb
#
# What this script does:
#   1. Loads project, region, cluster, and bucket variables from the root `.env`.
#   2. Configures `gcloud` project, region, and Application Default Credentials
#      (ADC) quota project (`gcloud auth application-default set-quota-project`)
#      to prevent quota project mismatch warnings.
#   3. Enables required Google Cloud APIs (`compute`, `dataproc`, `storage`,
#      `bigquery`, `iam`, `cloudresourcemanager`).
#   4. Ensures the target VPC subnetwork exists with Private Google Access enabled
#      so `--no-address` (internal-IP-only) cluster nodes can reach GCP APIs.
#   5. Creates the Cloud Storage bucket (`gs://${DATAPROC_BUCKET}`) if needed,
#      grants bucket-level `roles/storage.admin` to the Compute Engine default
#      service account (`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`),
#      and uploads `../data/sales_data.csv`.
#   6. Creates the Dataproc cluster (`${DATAPROC_CLUSTER_NAME}`) with Component
#      Gateway and the Jupyter optional component enabled.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CHAPTER_DIR}/.." && pwd)"

# Ensure gcloud uses Python 3.12 (via uv) if the system default is Python 3.14+
# (prevents `gcloud storage` from failing with `module 'ssl' has no attribute 'PROTOCOL_TLSv1'`)
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

echo "==> [1/6] Configuring gcloud project '${GCP_PROJECT_ID}', ADC quota project, and region '${GCP_REGION}'..."
gcloud config set project "${GCP_PROJECT_ID}"
gcloud config set dataproc/region "${GCP_REGION}"
gcloud config set billing/quota_project "${GCP_PROJECT_ID}" >/dev/null 2>&1 || true

echo "==> [2/6] Enabling required Google Cloud APIs (Compute, Dataproc, Storage, BigQuery, IAM)..."
gcloud services enable \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  compute.googleapis.com \
  dataproc.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  --project="${GCP_PROJECT_ID}"

# Set the Application Default Credentials (ADC) quota project after cloudresourcemanager is enabled
gcloud auth application-default set-quota-project "${GCP_PROJECT_ID}" --quiet >/dev/null 2>&1 || true

echo "==> [3/6] Ensuring subnetwork '${SUBNET_NAME}' exists and Private Google Access is enabled..."
VPC_NETWORK_NAME="${VPC_NETWORK_NAME:-composer-workshop-vpc}"
if ! gcloud compute networks subnets describe "${SUBNET_NAME}" --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "    Subnetwork '${SUBNET_NAME}' not found. Creating VPC '${VPC_NETWORK_NAME}' and subnetwork '${SUBNET_NAME}'..."
  if ! gcloud compute networks describe "${VPC_NETWORK_NAME}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute networks create "${VPC_NETWORK_NAME}" \
      --subnet-mode=custom \
      --project="${GCP_PROJECT_ID}"
  fi
  gcloud compute networks subnets create "${SUBNET_NAME}" \
    --network="${VPC_NETWORK_NAME}" \
    --region="${GCP_REGION}" \
    --range="10.10.0.0/20" \
    --enable-private-ip-google-access \
    --project="${GCP_PROJECT_ID}"
  if ! gcloud compute firewall-rules describe "${VPC_NETWORK_NAME}-allow-internal" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "${VPC_NETWORK_NAME}-allow-internal" \
      --network="${VPC_NETWORK_NAME}" \
      --allow=tcp,udp,icmp \
      --source-ranges="10.10.0.0/20" \
      --project="${GCP_PROJECT_ID}"
  fi
else
  gcloud compute networks subnets update "${SUBNET_NAME}" \
    --region="${GCP_REGION}" \
    --enable-private-ip-google-access \
    --project="${GCP_PROJECT_ID}"
fi

echo "==> [4/6] Creating GCS bucket 'gs://${DATAPROC_BUCKET}' (if needed) and uploading sales_data.csv..."
if ! gcloud storage buckets describe "gs://${DATAPROC_BUCKET}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${DATAPROC_BUCKET}" \
    --location="${GCP_REGION}" \
    --uniform-bucket-level-access \
    --project="${GCP_PROJECT_ID}"
fi

# Upload sales_data.csv to both /data/ (used by Chapter 02 interactive notebooks)
# and /raw_data/sales/ (used by Chapter 03 batch PySpark jobs)
gcloud storage cp "${CHAPTER_DIR}/data/sales_data.csv" \
  "gs://${DATAPROC_BUCKET}/data/sales_data.csv" \
  --project="${GCP_PROJECT_ID}"

gcloud storage cp "${CHAPTER_DIR}/data/sales_data.csv" \
  "gs://${DATAPROC_BUCKET}/raw_data/sales/sales_data.csv" \
  --project="${GCP_PROJECT_ID}"

echo "==> [5/6] Granting Storage & Dataproc permissions to Compute Engine Service Account..."
PROJECT_NUMBER="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Always grant bucket-level Storage Admin on gs://${DATAPROC_BUCKET} (works even in lab environments without project-level setIamPolicy)
gcloud storage buckets add-iam-policy-binding "gs://${DATAPROC_BUCKET}" \
  --member="serviceAccount:${COMPUTE_SA}" \
  --role="roles/storage.admin" \
  --project="${GCP_PROJECT_ID}" >/dev/null

# Attempt project-level role bindings if the caller has resourcemanager.projects.setIamPolicy
if gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member="serviceAccount:${COMPUTE_SA}" \
  --role="roles/dataproc.worker" \
  --project="${GCP_PROJECT_ID}" \
  --quiet >/dev/null 2>&1; then
  for ROLE in "roles/storage.admin" "roles/bigquery.dataEditor" "roles/bigquery.jobUser"; do
    gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
      --member="serviceAccount:${COMPUTE_SA}" \
      --role="${ROLE}" \
      --project="${GCP_PROJECT_ID}" \
      --quiet >/dev/null 2>&1 || true
  done
else
  echo "    (Project-level setIamPolicy restricted in lab environment; relying on bucket-level IAM + default Editor role)"
fi

echo "==> [6/6] Creating Dataproc cluster '${DATAPROC_CLUSTER_NAME}'..."
gcloud dataproc clusters create "${DATAPROC_CLUSTER_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --subnet="${SUBNET_NAME}" \
  --bucket="${DATAPROC_BUCKET}" \
  --temp-bucket="${DATAPROC_BUCKET}" \
  --enable-component-gateway \
  --optional-components=JUPYTER \
  --image-version="${DATAPROC_IMAGE_VERSION}" \
  --master-machine-type="${MASTER_MACHINE_TYPE}" \
  --worker-machine-type="${WORKER_MACHINE_TYPE}" \
  --worker-boot-disk-size="${WORKER_BOOT_DISK_SIZE}" \
  --no-address \
  --labels="mode=workshop,user=eilinlux"

echo "Dataproc cluster '${DATAPROC_CLUSTER_NAME}' created successfully!"
