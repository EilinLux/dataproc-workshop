# Chapter 02 — Dataproc Cluster Setup, Component Gateway & Autoscaling

## Overview

In this chapter, we provision a private-IP **Google Cloud Dataproc** cluster on Compute Engine with the **Component Gateway** and **JupyterLab** optional component enabled, explore how **Autoscaling Policies** scale secondary workers dynamically, and run interactive PySpark queries on `sales_data.csv`.

### What We Are Going to Do
1. **Configure Private Google Access & IAM**: Ensure the Compute Engine / Dataproc worker service account has access to Cloud Storage and enable Private Google Access on the subnet.
2. **Provision the Dataproc Cluster ([`scripts/01_create_cluster.sh`](./scripts/01_create_cluster.sh))**:
   * Enable `--enable-component-gateway` and `--optional-components=JUPYTER` so you can open JupyterLab and the Spark History Server directly from the Google Cloud Console without SSH tunnels.
   * Use `--no-address` for enhanced security (internal IP only).
3. **Interactive PySpark Exploration ([`notebooks/csv_reader_interactive.ipynb`](./notebooks/csv_reader_interactive.ipynb))**:
   * Upload [`data/sales_data.csv`](./data/sales_data.csv) to Cloud Storage and read it into a Spark DataFrame inside JupyterLab.
4. **Delete the Cluster ([`scripts/02_delete_cluster.sh`](./scripts/02_delete_cluster.sh))**:
   * Because data lives in Cloud Storage and BigQuery, you can safely delete the cluster when finished.

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`02_cluster_setup_and_management.ipynb`](./02_cluster_setup_and_management.ipynb) | Guide covering cluster sizing, image versions, Component Gateway, and YAML Autoscaling Policies. |
| [`notebooks/csv_reader_interactive.ipynb`](./notebooks/csv_reader_interactive.ipynb) | Interactive Jupyter notebook designed to run directly on the Dataproc cluster's JupyterLab UI. |
| [`data/sales_data.csv`](./data/sales_data.csv) | Sample retail sales CSV dataset (`transaction_id`, `product_id`, `amount`, `transaction_date`). |
| [`scripts/01_create_cluster.sh`](./scripts/01_create_cluster.sh) | Provisions the Dataproc cluster and uploads `sales_data.csv` to your GCS bucket. |
| [`scripts/02_delete_cluster.sh`](./scripts/02_delete_cluster.sh) | Deletes the Dataproc cluster to stop compute billing. |

---

## How to Run the Scripts

```bash
# Provision the cluster and upload sample data
bash 02_cluster_setup_and_management/scripts/01_create_cluster.sh

# Delete the cluster when finished
bash 02_cluster_setup_and_management/scripts/02_delete_cluster.sh
```
