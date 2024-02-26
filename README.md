# Ingestor Comparison

A simple Databricks notebook designed to conduct basic comparisons between two distinct ingestion methods.

## Description

In the realm of ETL processes, teams frequently assess various ingestion techniques, such as Spark, Databricks, ADF, Azure Functions, and others, for myriad reasons, primarily focusing on aspects like performance and associated costs. The fundamental challenge lies in ensuring that the data ingested through the new method precisely matches the format of the original data. This Databricks notebook, crafted in Scala and Spark-SQL, has proven to be highly effective in this context, facilitating standard checks and comparisons, and serving as a solid foundation for initiating an analytical exploration in this domain.

## Features

- Utilizes DELTA for underlying data management
- Configurable by schema and table for targeted analysis
- Employs DELTA HISTORY to autonomously identify the most recent versions for comparison between the two ingestion methods
- Enables manual selection of versions for debugging purposes
- Incorporates schema validation to ensure data structure consistency
- Performs bidirectional difference analysis to identify discrepancies between datasets
- Includes additional checks, such as the detection of problematic characters, provided as an example
