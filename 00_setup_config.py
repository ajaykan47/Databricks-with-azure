# Databricks notebook source
# MAGIC %md
# MAGIC # Initial Setup and Configuration
# MAGIC 
# MAGIC **Purpose**: Initial setup and configuration for E-commerce Analytics Project
# MAGIC **Author**: ajaykan47
# MAGIC **Date**: 2025-07-25
# MAGIC **Dependencies**: Azure Data Lake Storage Gen2, Databricks Runtime 11.3 LTS+

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging
from datetime import datetime, timedelta
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Libraries

# COMMAND ----------

# Install additional packages
%pip install faker pandas matplotlib seaborn plotly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Settings

# COMMAND ----------

# Storage Configuration
storage_account_name = "ecommercedata[yourname]"  # Replace [yourname] with your actual name
storage_account_key = "your-storage-key"  # Get from Azure Portal

# Container names
containers = {
    "raw": "raw-data",
    "processed": "processed-data",
    "analytics": "analytics-data"
}

# Data paths
storage_paths = {
    "raw": f"abfss://{containers['raw']}@{storage_account_name}.dfs.core.windows.net",
    "bronze": f"abfss://{containers['processed']}@{storage_account_name}.dfs.core.windows.net/bronze",
    "silver": f"abfss://{containers['processed']}@{storage_account_name}.dfs.core.windows.net/silver",
    "gold": f"abfss://{containers['analytics']}@{storage_account_name}.dfs.core.windows.net/gold"
}

print("Configuration loaded successfully!")
print(f"Storage Account: {storage_account_name}")
print(f"Available paths: {list(storage_paths.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Azure Storage Connection

# COMMAND ----------

def configure_azure_storage():
    """Configure Spark to connect to Azure Data Lake Storage"""
    try:
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            storage_account_key
        )
        logger.info("Azure Storage configuration successful")
        return True
    except Exception as e:
        logger.error(f"Failed to configure Azure Storage: {str(e)}")
        return False

# Configure storage
if configure_azure_storage():
    print("✅ Azure Storage configured successfully")
else:
    print("❌ Failed to configure Azure Storage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Storage Connection

# COMMAND ----------

def test_storage_connection():
    """Test connection to all storage containers"""
    test_results = {}
    
    for container_type, path in storage_paths.items():
        try:
            # Try to list the path (this will create it if it doesn't exist)
            files = dbutils.fs.ls(path)
            test_results[container_type] = "✅ Connected"
            logger.info(f"Successfully connected to {container_type} storage")
        except Exception as e:
            test_results[container_type] = f"❌ Failed: {str(e)}"
            logger.error(f"Failed to connect to {container_type} storage: {str(e)}")
    
    return test_results

# Test all connections
connection_results = test_storage_connection()
print("\n=== Storage Connection Test Results ===")
for container, result in connection_results.items():
    print(f"{container.capitalize()}: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_storage_path(layer):
    """Get storage path for specified layer"""
    return storage_paths.get(layer, None)

def log_step(step_name, status, details=None):
    """Log pipeline step with consistent format"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"[{timestamp}] {step_name}: {status}"
    if details:
        message += f" - {details}"
    print(message)
    logger.info(message)

def create_monitoring_entry(step_name, status, record_count=None, error_message=None):
    """Create monitoring entry for pipeline tracking"""
    monitoring_data = [(
        step_name,
        status,
        record_count,
        error_message,
        current_timestamp()
    )]
    
    schema = StructType([
        StructField("step_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("record_count", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    monitoring_df = spark.createDataFrame(monitoring_data, schema)
    
    try:
        monitoring_path = f"{storage_paths['gold']}/pipeline_monitoring"
        monitoring_df.write.format("delta").mode("append").save(monitoring_path)
        return True
    except Exception as e:
        logger.error(f"Failed to write monitoring entry: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Validation

# COMMAND ----------

def validate_environment():
    """Validate that environment is ready for the project"""
    validation_results = {
        "spark_session": False,
        "delta_support": False,
        "storage_access": False,
        "required_libraries": False
    }
    
    # Check Spark session
    try:
        spark.sql("SELECT 1").collect()
        validation_results["spark_session"] = True
    except:
        pass
    
    # Check Delta support
    try:
        spark.sql("CREATE TABLE IF NOT EXISTS test_delta (id INT) USING DELTA LOCATION '/tmp/test_delta'")
        spark.sql("DROP TABLE IF EXISTS test_delta")
        dbutils.fs.rm("/tmp/test_delta", True)
        validation_results["delta_support"] = True
    except:
        pass
    
    # Check storage access
    try:
        dbutils.fs.ls(storage_paths["raw"])
        validation_results["storage_access"] = True
    except:
        pass
    
    # Check required libraries
    try:
        import faker
        import pandas
        import matplotlib
        import seaborn
        import plotly
        validation_results["required_libraries"] = True
    except:
        pass
    
    return validation_results

# Run validation
validation_results = validate_environment()
print("\n=== Environment Validation Results ===")
for check, passed in validation_results.items():
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{check.replace('_', ' ').title()}: {status}")

all_passed = all(validation_results.values())
if all_passed:
    print("\n🎉 Environment is ready for the project!")
    log_step("Environment Setup", "SUCCESS", "All validations passed")
else:
    print("\n⚠️ Some validations failed. Please fix the issues before proceeding.")
    log_step("Environment Setup", "WARNING", "Some validations failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Summary

# COMMAND ----------

print("=== Setup Complete ===")
print(f"Project: E-commerce Analytics Pipeline")
print(f"Storage Account: {storage_account_name}")
print(f"Timestamp: {datetime.now()}")
print("\nNext Steps:")
print("1. Run 01_data_generation.py to create sample data")
print("2. Run 02_bronze_ingestion.py to ingest data to bronze layer")
print("3. Continue with the remaining pipeline steps")

# Create initial monitoring entry
create_monitoring_entry("Project Setup", "SUCCESS", details="Environment configured and validated")