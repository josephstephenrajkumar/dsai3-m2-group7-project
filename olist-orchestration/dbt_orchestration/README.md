
## Sub dbt Module Structure
```
.
├── dbt_orchestration/
| ├── models/
|   ├── marts/
|   ├── staging/
│ ├── dbt_project.yml
│ |── profiles.yml
│ └── packages.yml
  └── README.md # This file
```
*   `profiles`: files containing bigquery connection information.
*   `dbt_project.yml`: The heart of the dbt configuration file.
*   `packages.yml`: packages used by dbt.

## How to manually run dbt job
```bash
# Activate conda env and cd into the inner project directory
conda activate dagster
cd dbt_orchestration/
dbt deps
dbt run
```

The output of this command will give you the specific dbt error.