# Spotify Databricks Data Pipeline

This folder contains the Databricks implementation of an end-to-end Azure data engineering project.

The Databricks layer processes data that has already been incrementally ingested into ADLS Gen2 Bronze using Azure Data Factory. It handles the Silver and Gold layers, performs analytical joins, and uses Databricks Asset Bundles for deployment and orchestration.

## Architecture

```text
Azure SQL
    |
    v
Azure Data Factory
Incremental Ingestion
    |
    v
ADLS Gen2 Bronze
Parquet Files
    |
    v
Databricks Auto Loader
    |
    v
Silver Delta Tables
    |
    v
Lakeflow Declarative Pipelines
Auto CDC
    |
    v
Gold Fact and Dimension Tables
    |
    v
Jinja SQL Generation
    |
    v
Spark SQL Joins
    |
    v
factstream_enriched