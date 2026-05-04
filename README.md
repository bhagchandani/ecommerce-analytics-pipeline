## Data Pipeline Architecture

### Extract & Load Layer
In a production environment, this pipeline would use:
- **Fivetran** to sync data from PostgreSQL → Snowflake RAW schema (hourly)
- **Snowpipe** for real-time ingestion of event data from S3

For this learning project:
- Generated sample eCommerce data using Python
- Manually loaded to Snowflake RAW schema to simulate production state

### Transform Layer (dbt)
- **Staging models**: Clean and standardize raw data (views)
- **Marts models**: Star schema for analytics (tables)
  - `fct_orders`: Order fact table (15K+ records)
  - `dim_customers`: Customer dimension (1K records)
  - `dim_products`: Product dimension with profit margins
  - `dim_date`: Date dimension for time-series analysis
  

### Orchestration
In production, this would run on:
- **Apache Airflow** scheduling dbt runs after Fivetran sync
- DAG would run: Fivetran sync → dbt run → dbt test → notify on failure

For this project:
- Models run on-demand via `dbt run`
- Tests validate data quality via `dbt test`

### Key Features Implemented
✅ Incremental models for efficient processing
✅ Data quality tests (unique, not_null, referential integrity)
✅ Reusable macros for standardized transformations
✅ Auto-generated documentation with lineage graphs