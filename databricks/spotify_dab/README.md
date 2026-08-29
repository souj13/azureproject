# Databricks Processing Layer

This directory contains the Databricks implementation of the Spotify Azure Data Engineering Pipeline.

Databricks is responsible for processing data from the Bronze layer, creating standardized Silver Delta tables, building the Gold analytical layer using Lakeflow Declarative Pipelines, and producing the final enriched analytical dataset.

---

## Processing Flow

```text
ADLS Gen2 Bronze Layer
        |
        v
Databricks Auto Loader
        |
        v
Silver Layer
Standardized Delta Tables
        |
        v
Lakeflow Declarative Pipeline
        |
        v
Gold Layer
Dimension and Fact Tables
        |
        v
Final Analytical Joins
        |
        v
factstream_enriched
```

---

## Project Structure

```text
spotify_dab/
|
|-- resources/
|   |-- final_joins.yml
|   `-- gold_pipeline.yml
|
|-- src/
|   |-- gold/
|   |   `-- final_joins/
|   |       `-- jinja_notebook.py
|   |
|   |-- silver/
|   |   `-- silver_dimensions.py
|   |
|   `-- utils/
|
|-- .databricks/
|-- .vscode/
|-- .gitignore
|-- databricks.yml
|-- manifest.mf
|-- pyproject.toml
`-- README.md
```

---

## Silver Layer

The Silver layer processes data from the Bronze layer and creates standardized Delta tables for downstream transformations.

The Silver transformation logic is implemented in:

`src/silver/silver_dimensions.py`

The Silver layer acts as the cleaned and standardized processing layer between the raw Bronze files and the analytical Gold layer.

---

## Gold Layer

The Gold layer contains the analytical data model used for reporting and downstream analysis.

The pipeline creates the following Gold tables:

### Dimension Tables

- `dimuser`
- `dimtrack`
- `dimdate`

### Fact Table

- `factstream`

The Gold pipeline configuration is defined in:

`resources/gold_pipeline.yml`

The Gold layer follows a dimensional modeling approach, separating descriptive dimension data from streaming event data stored in the fact table.

---

## Lakeflow Declarative Pipeline

Lakeflow Declarative Pipelines are used to transform staging tables into the Gold analytical layer.

The transformation flow is:

```text
dimuser_stg      ->      dimuser
dimtrack_stg     ->      dimtrack
dimdate_stg      ->      dimdate
factstream_stg   ->      factstream
```

This separates the staging and analytical layers and provides a structured transformation workflow.

---

## Final Analytical Joins

After the Gold tables are created, the final processing step joins the dimension and fact tables to create an enriched analytical dataset.

The transformation logic is located in:

`src/gold/final_joins/jinja_notebook.py`

The final transformation uses:

- Apache Spark
- Spark SQL
- Jinja templating

The resulting analytical dataset is:

`factstream_enriched`

---

## Job Orchestration

The Databricks workflow orchestrates the execution of the Gold pipeline and the final analytical joins.

The execution order is:

```text
Gold Pipeline
      |
      v
Final Analytical Joins
```

This dependency ensures that the final analytical joins execute only after the Gold pipeline has completed successfully.

The job configuration is defined in:

`resources/final_joins.yml`

---

## Databricks Asset Bundle

This project is structured and deployed using Databricks Asset Bundles.

The main bundle configuration is defined in:

`databricks.yml`

The bundle manages the Databricks resources defined in the `resources` directory and provides a structured approach for deploying the pipeline.

---

## Technologies Used

- Azure Databricks
- Apache Spark
- Delta Lake
- Databricks Auto Loader
- Lakeflow Declarative Pipelines
- Databricks Workflows
- Databricks Asset Bundles
- Python
- Spark SQL
- Jinja

---

## End-to-End Architecture

For the complete end-to-end architecture, including Azure Data Factory ingestion, Azure SQL Database, ADLS Gen2 Bronze storage, and the complete Bronze to Silver to Gold data flow, see the [root README](../../README.md).
