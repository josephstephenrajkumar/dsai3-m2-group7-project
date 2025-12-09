
# Sub Meltano Module Structure
.
├── meltano_orchestration/
| ├── data/
│   ├── olist_customers_dataset.csv
│   └── ... (other olist csv files)
│ ├── __init__.py
│ ├── assets.py
│ |── definitions.py # Core Dagster definitions (Ops, Jobs, Schedules)
| ├── meltano.yml # Meltano project configuration
  └── README.md # This file

*   `data/`: Directory where all source CSV files should be placed.
*   `definitions.py`: The heart of the Dagster application. It defines the Ops that make up the pipeline, the Job that connects them, and the Schedule that runs the Job. Both Meltano and dbt jobs are placed here.
*   `meltano.yml`: Configures the Meltano plugins (`tap-csv`, `target-bigquery`) and their settings.
*   `pyproject.toml`: Defines the Python project structure and dependencies for Dagster.

## How to manually load the data from csv to bigquery
```bash
# Activate conda env and cd into the inner project directory
conda activate dagster
cd meltano_orchestration/
meltano run tap-csv target-bigquery --force
```

The output of this command will give you the specific Meltano error.