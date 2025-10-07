# Data Pipelines

This directory contains data pipeline scripts for the data engineering project.

## Available Pipelines

### Landing to Bronze (`landing_to_bronze.py`)

Moves data from the landing zone to the bronze layer with added metadata.

#### Features:
- Reads CSV, Parquet, or JSON files from the landing zone
- Adds processing timestamp and load date
- Writes to the bronze layer with date partitioning
- Includes basic error handling and logging

#### Usage:
```bash
# From the project root directory
python -m src.pipelines.landing_to_bronze
```

#### Configuration:
Update `config/pipeline_config_wsl.yaml` to set the correct paths for your environment.

#### Dependencies:
- PySpark
- PyYAML (for config parsing)

#### Output:
- Data is written to the bronze layer (configured in `pipeline_config_wsl.yaml`)
- Partitioned by `load_date` for efficient time-based queries
- Includes metadata columns: `processing_timestamp` and `load_date`
