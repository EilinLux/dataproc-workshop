#!/usr/bin/env bash
# ==============================================================================
# Chapter 04 — Script 01: Submit PySpark Batch Job to Dataproc Serverless
# Reference Notebook: ../04_dataproc_serverless.ipynb
# Reference Code:     ../spark_apps/serverless_bq_etl.py
#
# What this script does:
#   1. Uploads `serverless_bq_etl.py` to `gs://${DATAPROC_BUCKET}/spark_apps/`.
#   2. Submits a serverless Spark batch job (`gcloud dataproc batches submit pyspark`)
#      that reads `BQ_INPUT_TABLE_ID` and writes `BQ_OUTPUT_TABLE_ID` without
#      needing a running Dataproc cluster.
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

BATCH_ID="serverless-sales-etl-$(date +%Y%m%d-%H%M%S)"

echo "==> [1/2] Uploading 'serverless_bq_etl.py' to 'gs://${DATAPROC_BUCKET}/spark_apps/'..."
gcloud storage cp \
  "${CHAPTER_DIR}/spark_apps/serverless_bq_etl.py" \
  "gs://${DATAPROC_BUCKET}/spark_apps/serverless_bq_etl.py" \
  --project="${GCP_PROJECT_ID}"

echo "==> [2/2] Submitting Dataproc Serverless batch '${BATCH_ID}'..."
gcloud dataproc batches submit pyspark \
  "gs://${DATAPROC_BUCKET}/spark_apps/serverless_bq_etl.py" \
  --batch="${BATCH_ID}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --version="1.1" \
  --subnet="${SUBNET_NAME}" \
  --deps-bucket="gs://${SPARK_TEMP_BUCKET}" \
  -- \
  --input-table="${BQ_INPUT_TABLE_ID}" \
  --output-table="${BQ_OUTPUT_TABLE_ID}" \
  --processing-date="${DEFAULT_PROCESSING_DATE}" \
  --gcs-temp-bucket="${SPARK_TEMP_BUCKET}"

echo "Dataproc Serverless batch '${BATCH_ID}' completed!"
