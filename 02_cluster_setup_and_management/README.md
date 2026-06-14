# Chapter 02 — Dataproc Cluster Setup, Component Gateway, Live Updates & Autoscaling

## Overview

In this chapter, we provision a private-IP **Google Cloud Dataproc** (**Managed Apache Spark**) cluster on Compute Engine with the **Component Gateway** and **JupyterLab** optional component enabled, run interactive PySpark queries on `sales_data.csv`, perform live cluster updates (scaling primary/secondary workers, YARN graceful decommissioning, and FinOps labels), attach a declarative **YAML Autoscaling Policy**, and configure automatic cluster lifecycle deletion (`--max-idle` / `--max-age`).

---

## Prerequisites Before Running the Scripts

Before executing the scripts in [`scripts/`](./scripts/), complete these three one-time setup steps from the repository root:

1. **Sync your `uv` Python 3.12 environment** (also ensures `gcloud storage` uses Python 3.12 instead of crashing on macOS Python 3.14):
   ```bash
   uv sync
   uv run python -m ipykernel install --user --name dataproc-workshop --display-name "Python (dataproc-workshop)"
   ```
2. **Authenticate with Google Cloud & set your Quota Project**:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   PROJECT_ID=$(gcloud config get-value project)
   gcloud config set project "$PROJECT_ID"
   gcloud config set billing/quota_project "$PROJECT_ID"
   gcloud auth application-default set-quota-project "$PROJECT_ID"
   ```
3. **Create and configure your `.env` file**:
   ```bash
   cp .env.example .env
   # Update GCP_PROJECT_ID, DATAPROC_BUCKET, and SPARK_TEMP_BUCKET in .env
   ```

---

## Step-by-Step Breakdown of the Chapter 02 Scripts

All four sections of [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) have corresponding automation scripts in [`scripts/`](./scripts/):

### 1. Provision Cluster & Upload Sample CSV Data (`scripts/01_create_cluster.sh`)

```bash
bash 02_cluster_setup_and_management/scripts/01_create_cluster.sh
```

What this script does step-by-step:
1. **Step `[1/6]` — Environment & `CLOUDSDK_PYTHON` Configuration**:
   * Loads `GCP_PROJECT_ID`, `GCP_REGION`, `SUBNET_NAME`, `DATAPROC_BUCKET`, and cluster sizing variables from the root `.env` file.
   * Automatically points `CLOUDSDK_PYTHON` to `uv`'s Python 3.12 interpreter so `gcloud storage` commands never fail with `module 'ssl' has no attribute 'PROTOCOL_TLSv1'` on systems with Python 3.14.
   * Sets the active `gcloud` project, Dataproc region, and billing quota project.
2. **Step `[2/6]` — Enable Required Google Cloud APIs & ADC Quota Project**:
   * Enables `cloudresourcemanager.googleapis.com`, `iam.googleapis.com`, `compute.googleapis.com`, `dataproc.googleapis.com`, `storage.googleapis.com`, and `bigquery.googleapis.com`.
   * Synchronizes the local Application Default Credentials (ADC) quota project (`gcloud auth application-default set-quota-project`).
3. **Step `[3/6]` — Provision VPC Network, Subnetwork & Private Google Access**:
   * Checks whether `${SUBNET_NAME}` (e.g., `composer-workshop-subnet`) exists in `${GCP_REGION}`.
   * If missing, creates the custom VPC (`composer-workshop-vpc`), creates `${SUBNET_NAME}` (`10.10.0.0/20`) with **Private Google Access** (`--enable-private-ip-google-access`) enabled, and adds an internal firewall rule (`composer-workshop-vpc-allow-internal`) allowing `tcp,udp,icmp` between Spark master and worker nodes.
   * If the subnet already exists, ensures `--enable-private-ip-google-access` is enabled so `--no-address` (internal-IP-only) Dataproc VMs can reach Cloud Storage and BigQuery APIs without public IPs.
4. **Step `[4/6]` — Create Cloud Storage Bucket & Upload `sales_data.csv`**:
   * Creates `gs://${DATAPROC_BUCKET}` in `${GCP_REGION}` with Uniform Bucket-Level Access (if it doesn't already exist).
   * Uploads [`data/sales_data.csv`](./data/sales_data.csv) to **both**:
     * `gs://${DATAPROC_BUCKET}/data/sales_data.csv` — so the interactive PySpark cells (`gs://<BUCKET_NAME>/data/*.csv`) in [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) and [`notebooks/csv_reader_interactive.ipynb`](./notebooks/csv_reader_interactive.ipynb) work immediately!
     * `gs://${DATAPROC_BUCKET}/raw_data/sales/sales_data.csv` — used by Chapter 03 batch PySpark jobs.
5. **Step `[5/6]` — Configure Service Account IAM Permissions**:
   * Resolves the project's numeric `PROJECT_NUMBER` and identifies the Compute Engine default service account (`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`).
   * Grants `roles/storage.admin` **directly on `gs://${DATAPROC_BUCKET}`** (`gcloud storage buckets add-iam-policy-binding`) so Dataproc's preflight check passes even in sandbox/lab environments where project-level `setIamPolicy` is restricted.
   * Also attempts project-level bindings (`roles/dataproc.worker`, `roles/storage.admin`, `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`) when the caller has `resourcemanager.projects.setIamPolicy`.
6. **Step `[6/6]` — Provision the Dataproc Cluster with Component Gateway & Jupyter**:
   * Runs `gcloud dataproc clusters create "${DATAPROC_CLUSTER_NAME}"` passing:
     * `--bucket="${DATAPROC_BUCKET}"` and `--temp-bucket="${DATAPROC_BUCKET}"` (so both staging and temporary scratch space use your pre-authorized GCS bucket).
     * `--enable-component-gateway` and `--optional-components=JUPYTER` (enabling one-click browser access to **JupyterLab** and the **Spark History Server** in the Cloud Console without SSH tunnels).
     * `--no-address` (enforcing internal-IP-only VMs for security).

---

### 2. Update a Running Cluster (`scripts/02_update_cluster.sh`)

```bash
bash 02_cluster_setup_and_management/scripts/02_update_cluster.sh
```

Automates the **Update a cluster** section of [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) on your live cluster without recreating it:
1. **Step `[1/4]` — Scale Out Primary & Secondary (Preemptible/Spot) Workers**:
   * Runs `gcloud dataproc clusters update` with `--num-workers=3` and `--num-secondary-workers=2` to add burst compute capacity using low-cost Spot/preemptible VMs.
2. **Step `[2/4]` — Scale Down with YARN Graceful Decommissioning**:
   * Scales back down to `--num-workers=2` and `--num-secondary-workers=0` using `--graceful-decommission-timeout=3600s` so YARN stops scheduling new containers on retiring nodes and waits for in-flight Spark stages to complete before terminating VMs.
3. **Step `[3/4]` — Add/Update FinOps Resource Labels**:
   * Applies `--update-labels="env=production,cost_center=research,user=eilinlux"` for Cloud Billing cost attribution.
4. **Step `[4/4]` — Remove a Resource Label**:
   * Runs `--remove-labels="cost_center"` to demonstrate label removal on a live cluster.

---

### 3. Import & Attach a YAML Autoscaling Policy (`scripts/03_autoscaling_policy.sh`)

```bash
bash 02_cluster_setup_and_management/scripts/03_autoscaling_policy.sh
```

Automates the **Autoscaling** section of [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) using [`config/my_autoscaling_policy.yaml`](./config/my_autoscaling_policy.yaml):
1. **Step `[1/2]` — Import the YAML Policy into Dataproc**:
   * Runs `gcloud dataproc autoscaling-policies import my-workshop-autoscaling-policy-yaml --source=02_cluster_setup_and_management/config/my_autoscaling_policy.yaml --region="${GCP_REGION}"`.
   * Configures `cooldownPeriod: 120s`, `gracefulDecommissionTimeout: 60s`, `scaleUpFactor: 1.0`, `scaleDownFactor: 0.5`, `workerConfig` (`2`–`10` primary nodes), and `secondaryWorkerConfig` (`0`–`10` secondary Spot nodes).
2. **Step `[2/2]` — Attach the Policy to the Live Cluster**:
   * Runs `gcloud dataproc clusters update "${DATAPROC_CLUSTER_NAME}" --autoscaling-policy="my-workshop-autoscaling-policy-yaml"` so YARN dynamically scales worker nodes based on pending memory demand.

---

### 4. Lifecycle Auto-Termination (`--max-idle` / `--max-age`) & Delete Cluster (`scripts/04_delete_cluster.sh`)

```bash
# Option A: Configure 30-minute idle auto-termination (--max-idle=30m) on the running cluster
bash 02_cluster_setup_and_management/scripts/04_delete_cluster.sh configure-lifecycle

# Option B: Immediately delete the Dataproc cluster to stop compute billing
bash 02_cluster_setup_and_management/scripts/04_delete_cluster.sh
```

Automates the **Delete a cluster** section of [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb):
* When passed `configure-lifecycle`, updates the running cluster with `--max-idle=30m` so Dataproc automatically deletes the cluster after 30 minutes with no active Spark/YARN jobs.
* When run without arguments (default `delete`), immediately deletes `${DATAPROC_CLUSTER_NAME}` while preserving your datasets in `gs://${DATAPROC_BUCKET}`.

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) | Complete guide covering cluster creation, Component Gateway, live updates, YAML Autoscaling Policies, and lifecycle deletion. |
| [`notebooks/csv_reader_interactive.ipynb`](./notebooks/csv_reader_interactive.ipynb) | Interactive Jupyter notebook designed to run either locally with `uv` or on the Dataproc cluster's JupyterLab UI. |
| [`data/sales_data.csv`](./data/sales_data.csv) | Sample retail sales CSV dataset (`transaction_id`, `product_id`, `amount`, `transaction_date`). |
| [`config/my_autoscaling_policy.yaml`](./config/my_autoscaling_policy.yaml) | Declarative Dataproc Autoscaling Policy YAML (`2`–`10` primary workers, `0`–`10` secondary workers). |
| [`scripts/01_create_cluster.sh`](./scripts/01_create_cluster.sh) | Enables APIs, configures VPC/IAM, creates `gs://${DATAPROC_BUCKET}`, uploads `sales_data.csv` to `/data/` and `/raw_data/sales/`, and creates the cluster. |
| [`scripts/02_update_cluster.sh`](./scripts/02_update_cluster.sh) | Scales primary & secondary workers, demonstrates graceful decommissioning, and updates/removes FinOps labels. |
| [`scripts/03_autoscaling_policy.sh`](./scripts/03_autoscaling_policy.sh) | Imports `config/my_autoscaling_policy.yaml` and attaches the autoscaling policy to the running cluster. |
| [`scripts/04_delete_cluster.sh`](./scripts/04_delete_cluster.sh) | Configures `--max-idle` auto-termination (`configure-lifecycle`) or immediately deletes the cluster. |

---

## Running the Interactive PySpark Notebook (`gs://<BUCKET_NAME>/data/*.csv` vs. Local `uv`)

Both [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) and [`notebooks/csv_reader_interactive.ipynb`](./notebooks/csv_reader_interactive.ipynb) read `sales_data.csv` into a PySpark DataFrame:

1. **On the Dataproc Cluster's JupyterLab UI (`gs://${DATAPROC_BUCKET}/data/*.csv`)**:
   * Running [`scripts/01_create_cluster.sh`](./scripts/01_create_cluster.sh) creates `gs://${DATAPROC_BUCKET}` and uploads [`data/sales_data.csv`](./data/sales_data.csv) to `gs://${DATAPROC_BUCKET}/data/sales_data.csv`.
   * Inside the cluster's **JupyterLab** web interface (under **Dataproc / Managed Apache Spark $\rightarrow$ Clusters $\rightarrow$ Web Interfaces**), replace `<BUCKET_NAME>` with your `DATAPROC_BUCKET` value (or rely on `.env`) to read `gs://${DATAPROC_BUCKET}/data/*.csv` via the pre-installed Cloud Storage connector.
2. **Locally in your IDE with `uv` (`Python (dataproc-workshop)` kernel)**:
   * Local PySpark installations do not bundle the Google Cloud Storage connector JAR by default, so the notebook automatically detects the local [`data/sales_data.csv`](./data/sales_data.csv) file when run locally in VS Code / Cursor / Antigravity and falls back to `gs://${DATAPROC_BUCKET}/data/*.csv` when run on the cluster.
   * Make sure the **`Python (dataproc-workshop)`** kernel (`.venv/bin/python`) is selected in the top-right corner of the notebook editor.
