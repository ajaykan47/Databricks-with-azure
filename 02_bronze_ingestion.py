# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Data Ingestion
# MAGIC 
# MAGIC **Purpose**: Ingest raw data from Azure Data Lake into bronze layer with metadata
# MAGIC **Author**: ajaykan47
# MAGIC **Date**: 2025-07-25
# MAGIC **Dependencies**: Raw data from data generation step, Delta Lake

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

print("Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Ingestion Functions

# COMMAND ----------

def ingest_to_bronze(source_path, target_path, table_name):
    """
    Ingest data from raw layer to bronze layer with metadata enrichment
    
    Args:
        source_path (str): Source data path in raw layer
        target_path (str): Target path in bronze layer  
        table_name (str): Name of the table for metadata
        
    Returns:
        int: Number of records processed
    """
    log_step(f"Bronze Ingestion - {table_name}", "STARTED", f"Source: {source_path}")
    
    try:
        # Read source data
        df = spark.read.format("delta").load(source_path)
        
        # Get record count
        record_count = df.count()
        
        # Add bronze layer metadata columns
        df_with_metadata = df.withColumn("bronze_ingestion_timestamp", current_timestamp()) \
                            .withColumn("bronze_ingestion_date", current_date()) \
                            .withColumn("source_system", lit("data_generation")) \
                            .withColumn("source_table", lit(table_name)) \
                            .withColumn("data_quality_status", lit("raw")) \
                            .withColumn("record_hash", hash(concat_ws("|", *df.columns))) \
                            .withColumn("bronze_record_id", monotonically_increasing_id())
        
        # Write to bronze layer with optimizations
        df_with_metadata.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .save(target_path)
        
        log_step(f"Bronze Ingestion - {table_name}", "SUCCESS", f"Processed {record_count:,} records")
        
        # Create monitoring entry
        create_monitoring_entry(f"Bronze Ingestion - {table_name}", "SUCCESS", record_count)
        
        return record_count
        
    except Exception as e:
        error_msg = f"Failed to ingest {table_name}: {str(e)}"
        log_step(f"Bronze Ingestion - {table_name}", "ERROR", error_msg)
        create_monitoring_entry(f"Bronze Ingestion - {table_name}", "ERROR", error_message=error_msg)
        raise e

# COMMAND ----------

def validate_bronze_data(bronze_path, table_name, expected_columns):
    """
    Validate bronze layer data quality
    
    Args:
        bronze_path (str): Path to bronze table
        table_name (str): Name of the table
        expected_columns (list): Expected column names
        
    Returns:
        dict: Validation results
    """
    log_step(f"Bronze Validation - {table_name}", "STARTED", "Performing data validation")
    
    try:
        df = spark.read.format("delta").load(bronze_path)
        
        validation_results = {
            "table_name": table_name,
            "record_count": df.count(),
            "column_count": len(df.columns),
            "expected_columns": expected_columns,
            "actual_columns": df.columns,
            "missing_columns": [],
            "extra_columns": [],
            "null_counts": {},
            "duplicate_count": 0,
            "validation_timestamp": datetime.now()
        }
        
        # Check for missing/extra columns
        expected_set = set(expected_columns)
        actual_set = set(df.columns)
        validation_results["missing_columns"] = list(expected_set - actual_set)
        validation_results["extra_columns"] = list(actual_set - expected_set)
        
        # Check for null values in key columns
        for column in df.columns:
            if not column.startswith("bronze_"):  # Skip metadata columns
                null_count = df.filter(col(column).isNull()).count()
                validation_results["null_counts"][column] = null_count
        
        # Check for duplicates