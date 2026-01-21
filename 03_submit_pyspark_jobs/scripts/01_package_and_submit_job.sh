#!/usr/bin/env bash
# ==============================================================================
# Chapter 03 — Script 01: Upload PySpark Scripts & Submit Job to Dataproc Cluster
# Reference Notebook: ../03_submit_pyspark_jobs.ipynb
# Reference Code:     ../spark_apps/process_data.py, ../spark_apps/utils.py
#
# What this script does:
#   1. Uploads `sales_data.csv`, `utils.py`, and `process_data.py` to your
#      Cloud Storage bucket (`gs://${DATAPROC_BUCKET}`).
#   2. Submits `process_data.py` to your running Dataproc cluster
#      (`${DATAPROC_CLUSTER_NAME}`), passing `utils.py` via `--py-files`.
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

echo "==> [1/2] Uploading data and PySpark scripts to 'gs://${DATAPROC_BUCKET}'..."
gcloud storage cp "${CHAPTER_DIR}/data/sales_data.csv" \
  "gs://${DATAPROC_BUCKET}/raw_data/sales/sales_data.csv" \
  --project="${GCP_PROJECT_ID}"

gcloud storage cp \
  "${CHAPTER_DIR}/spark_apps/utils.py" \
  "${CHAPTER_DIR}/spark_apps/process_data.py" \
  "gs://${DATAPROC_BUCKET}/spark_apps/" \
  --project="${GCP_PROJECT_ID}"

echo "==> [2/2] Submitting PySpark job to Dataproc cluster '${DATAPROC_CLUSTER_NAME}'..."
gcloud dataproc jobs submit pyspark \
  "gs://${DATAPROC_BUCKET}/spark_apps/process_data.py" \
  --cluster="${DATAPROC_CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT_ID}" \
  --py-files="gs://${DATAPROC_BUCKET}/spark_apps/utils.py" \
  -- \
  --input-path="gs://${DATAPROC_BUCKET}/raw_data/sales/sales_data.csv" \
  --output-table="${BQ_OUTPUT_TABLE_ID}" \
  --processing-date="${DEFAULT_PROCESSING_DATE}" \
  --gcs-temp-bucket="${SPARK_TEMP_BUCKET}"

echo "PySpark job completed successfully!"
