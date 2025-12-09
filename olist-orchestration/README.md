# Dagster + Meltano + dbt: Automated CSV to BigQuery ELT Pipeline

This is the repository for DASI3 Module 2 project, completed by Group 7.
https://github.com/chenchaosg/dsai3-m2-group7-project.git

## Group 7 Members:
  - Chen Chao
  - Chin Guan Xun
  - Darwin
  - Joseph
  - Nathaniel

This project demonstrates a robust, automated data pipeline that extracts data from multiple CSV files, and loads it into Google BigQuery. The entire workflow is orchestrated by [Dagster](https://dagster.io/) and leverages [Meltano](https://meltano.com/) and [dbt](https://www.getdbt.com/) for the core ELT (Extract, Load, Transform) process.

## Overview

The pipeline is designed to be dynamic and scalable. It automatically discovers CSV files in a designated directory, generates a manifest, and uses Meltano to load the data into corresponding tables in BigQuery. The entire process is defined as a Dagster Job and can be scheduled to run periodically based on CRON expression.

Then it uses dbt to make necessary data transform and save the facts and dimensions tables in BigQuery. The entire process is defined as another Dagster Job and can be scheduled to run periodically based on CRON expression.

### Technology Stack

*   **Orchestrator**: [Dagster](https://dagster.io/)
*   **ELT Framework**: [dbt](https://www.getdbt.com/)
*   **ELT Framework**: [Meltano](https://meltano.com/)
*   **Extractor (Tap)**: `tap-csv`
*   **Loader (Target)**: `target-bigquery`
*   **Data Warehouse**: [Google BigQuery](https://cloud.google.com/bigquery)
*   **Environment Management**: [Conda](https://docs.conda.io/en/latest/)

## Project Structure
.
├── dbt_orchestration/
├── meltano_orchestration/
├── dagster_environment.yml # Dagster environment file
├── elt_environment.yml # ELT environment file
├── pyproject.toml # Defines the Python project structure and dependencies for Dagster
├── setup.cfg
├── setup.py
└── README.md # This file

## Setup and Installation

Follow these steps to set up and run the project locally.

### 1. Prerequisites

*   [Conda](https://docs.conda.io/en/latest/miniconda.html) installed.
*   Access to a Google Cloud Platform (GCP) project with BigQuery enabled.
*   A GCP Service Account with **BigQuery Data Editor** and **BigQuery Job User** permissions. Download its JSON key file.

### 2. Clone the Repository

Clone your project repository to your local environment:

https://github.com/chenchaosg/dsai3-m2-group7-project.git


### 3. Create and Activate Conda Environment

```bash
cd olist-orchestration

# Create a new conda environment
conda env create -f dagster_environment.yml

# Activate the environment
conda activate dagster
```

### 4. configure some yml files.

In meltano_orchestration/meltano.yml file, edit following lines:
    - credentials_path: 
        <your-gcp-credentials-path>
    - dataset: <your-dataset-name>
    - project: <your-gcp-project-id>

In dbt_orchestration/profiles.yml file, edit following lines:
    - dataset: <your GCP dataset>
    - keyfile: <your-gcp-credentials-path>
    - project: <your-gcp-project-id>

In dbt_orchestration/models/staging/stg_sources.yml file, edit following lines:
  - name: <your GCP Meltano dataset>
    database: <your GCP project ID>
    schema: <your GCP Meltano dataset>    
    tables:
      - name: <all table names in your gcp dataset>

In dbt_orchestration/models/staging/stg_olist_xxx.sql file, edit your own dataset and table name
In dbt_orchestration/models/staging/stg_olist_xxx.yml file, edit your stage table name

### 5. (Optional) System Requirement: File Descriptors
Modern data tools can open many files simultaneously. To prevent OSError: [Errno 24] Too many open files, you must increase the file descriptor limit in the shell session where you run Dagster.

```bash
# Check your current limit
ulimit -n

# Set a new, higher limit for the current session
ulimit -n 65536
```

### 6. How to RUn

#### 6.1. Start the Dagster

```bash
cd olist-orchestration

dagster dev
```

#### 6.2. Start the Dagster UI
Open http://localhost:3000 with your browser to see the project.

##### 1. Run Meltano job Manually
In the Dagster UI, navigate to Overview > Jobs.

Click on the meltano_job.

Click the "Launch run" button to manually trigger the pipeline.

Enable the Schedule: In the Dagster UI, navigate to Overview > Schedules.

You will see meltano_schedule, which is configured to run daily at midnight.

Click the toggle switch to turn the schedule on.

📖 Workflow Explained
The entire pipeline is defined in definitions.py as a Dagster Job named meltano_job. It consists of two main steps (Ops):

build_json_manifest:

This Op scans the ./data/ directory for all *.csv files.

It generates a JSON file (olist_files_definition.json) that acts as a manifest, listing each found CSV file, its target table name, and its primary keys.

This makes the pipeline dynamic—simply add or remove CSV files from the data directory, and the pipeline will adapt on the next run.

run_meltano_extract_load:

This Op waits for the manifest to be created.

It then uses the Dagster MeltanoResource to execute the shell command: meltano run tap-csv target-bigquery --force.

Meltano reads the configuration from meltano.yml, finds the path to the JSON manifest, and proceeds to extract and load the data.

🔍 Troubleshooting
Throughout the development of this pipeline, several key issues were resolved. If you encounter errors, check these first.

```bash
# Activate conda env and cd into the inner project directory
conda activate dagster
cd meltano_orchestration/
meltano run tap-csv target-bigquery --force
```
The output of this command will give you the specific Meltano error.

##### 1. Run dbt job Manually
In the Dagster UI, navigate to Overview > Jobs.

Click on the dbt_job.

Click the "Launch run" button to manually trigger the pipeline.

Enable the Schedule: In the Dagster UI, navigate to Overview > Schedules.

You will see dbt_schedule, which is configured to run daily at midnight.
