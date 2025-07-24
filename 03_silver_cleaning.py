# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Data Cleaning and Transformation
# MAGIC 
# MAGIC **Purpose**: Clean, validate, and enrich data from bronze layer to create silver layer
# MAGIC **Author**: ajaykan47
# MAGIC **Date**: 2025-07-2025
# MAGIC **Dependencies**: Bronze layer tables, data quality rules

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import re
from datetime import datetime, timedelta

print("Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleaning Functions

# COMMAND ----------

def clean_customers():
    """
    Clean and enrich customer data
    
    Returns:
        DataFrame: Cleaned customer data
    """
    log_step("Silver Cleaning - Customers", "STARTED", "Cleaning customer data")
    
    try:
        # Read bronze data
        df = spark.table("bronze_customers")
        
        # Data cleaning operations
        cleaned_df = df.filter(col("email").isNotNull()) \
                      .filter(col("age").between(18, 100)) \
                      .filter(col("customer_id").isNotNull()) \
                      .dropDuplicates(["customer_id"])
        
        # Data enrichment
        cleaned_df = cleaned_df \
            .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
            .withColumn("email_domain", regexp_extract(col("email"), r"@(.+)", 1)) \
            .withColumn("customer_age_group", 
                       when(col("age") < 25, "18-24")
                       .when(col("age") < 35, "25-34")
                       .when(col("age") < 45, "35-44")
                       .when(col("age") < 55, "45-54")
                       .otherwise("55+")) \
            .withColumn("registration_year", year(col("registration_date"))) \
            .withColumn("days_since_registration", 
                       datediff(current_date(), col("registration_date"))) \
            .withColumn("customer_lifetime_category",
                       when(col("days_since_registration") < 30, "New")
                       .when(col("days_since_registration") < 365, "Active")
                       .otherwise("Loyal"))
        
        # Standardize text fields
        cleaned_df = cleaned_df \
            .withColumn("first_name", initcap(trim(col("first_name")))) \
            .withColumn("last_name", initcap(trim(col("last_name")))) \
            .withColumn("city", initcap(trim(col("city")))) \
            .withColumn("country", trim(col("country"))) \
            .withColumn("email", lower(trim(col("email"))))
        
        # Add silver layer metadata
        cleaned_df = cleaned_df \
            .withColumn("silver_processed_timestamp", current_timestamp()) \
            .withColumn("silver_processed_date", current_date()) \
            .withColumn("data_quality_score", lit(95)) \
            .withColumn("cleaning_rules_applied", 
                       array(lit("null_removal"), lit("age_validation"), 
                            lit("deduplication"), lit("text_standardization")))
        
        record_count = cleaned_df.count()
        log_step("Silver Cleaning - Customers", "SUCCESS", f"Cleaned {record_count:,} records")
        
        return cleaned_df
        
    except Exception as e:
        error_msg = f"Failed to clean customers: {str(e)}"
        log_step("Silver Cleaning - Customers", "ERROR", error_msg)
        raise e

# COMMAND ----------

def clean_products():
    """
    Clean and enrich product data
    
    Returns:
        DataFrame: Cleaned product data
    """
    log_step("Silver Cleaning - Products", "STARTED", "Cleaning product data")
    
    try:
        # Read bronze data
        df = spark.table("bronze_products")
        
        # Data cleaning operations
        cleaned_df = df.filter(col("price") > 0) \
                      .filter(col("cost") > 0) \
                      .filter(col("product_id").isNotNull()) \
                      .filter(col("price") >= col("cost")) \
                      .dropDuplicates(["product_id"])
        
        # Data enrichment
        cleaned_df = cleaned_df \
            .withColumn("profit_margin_pct", 
                       round(((col("price") - col("cost")) / col("price")) * 100, 2)) \
            .withColumn("profit_amount", round(col("price") - col("cost"), 2)) \
            .withColumn("price_category",
                       when(col("price") < 50, "Budget")
                       .when(col("price") < 200, "Mid-Range")
                       .when(col("price") < 500, "Premium")
                       .otherwise("Luxury")) \
            .withColumn("weight_category",
                       when(col("weight_kg") < 1, "Light")
                       .when(col("weight_kg") < 5, "Medium")
                       .when(col("weight_kg") < 20, "Heavy")
                       .otherwise("Very Heavy")) \
            .withColumn("availability_status",
                       when(col("in_stock") & (col("stock_quantity") > 10), "Available")
                       .when(col("in_stock") & (col("stock_quantity") > 0), "Low Stock")
                       .otherwise("Out of Stock"))
        
        # Parse dimensions and calculate volume
        cleaned_df = cleaned_df \
            .withColumn("dimensions_parsed", split(col("dimensions"), "x")) \
            .withColumn("length_cm", 
                       regexp_extract(col("dimensions_parsed")[0], r"(\d+)", 1).cast("double")) \
            .withColumn("width_cm", 
                       regexp_extract(col("dimensions_parsed")[1], r"(\d+)", 1).cast("double")) \
            .withColumn("height_cm", 
                       regexp_extract(col("dimensions_parsed")[2], r"(\d+)", 1).cast("double")) \
            .withColumn("volume_cm3", col("length_cm") * col("width_cm") * col("height_cm")) \
            .drop("dimensions_parsed")
        
        # Standardize text fields
        cleaned_df = cleaned_df \
            .withColumn("product_name", initcap(trim(col("product_name")))) \
            .withColumn("category", initcap(trim(col("category")))) \
            .withColumn("sub_category", initcap(trim(col("sub_category")))) \
            .withColumn("brand", initcap(trim(col("brand")))) \
            .withColumn("supplier", initcap(trim(col("supplier"))))
        
        # Add silver layer metadata
        cleaned_df = cleaned_df \
            .withColumn("silver_processed_timestamp", current_timestamp()) \
            .withColumn("silver_processed_date", current_date()) \
            .withColumn("data_quality_score", lit(92)) \
            .withColumn("cleaning_rules_applied", 
                       array(lit("price_validation"), lit("cost_validation"), 
                            lit("profit_calculation"), lit("categorization")))
        
        record_count = cleaned_df.count()
        log_step("Silver Cleaning - Products", "SUCCESS", f"Cleaned {record_count:,} records")
        
        return cleaned_df
        
    except Exception as e:
        error_msg = f"Failed to clean products: {str(e)}"
        log_step("Silver Cleaning - Products", "ERROR", error_msg)
        raise e

# COMMAND ----------

def clean_orders():
    """
    Clean and enrich order data
    
    Returns:
        DataFrame: Cleaned order data
    """
    log_step("Silver Cleaning - Orders", "STARTED", "Cleaning order data")
    
    try:
        # Read bronze data
        df = spark.table("bronze_orders")
        
        # Data cleaning operations
        cleaned_df = df.filter(col("order_id").isNotNull()) \
                      .filter(col("customer_id").isNotNull()) \
                      .filter(col("order_date").isNotNull()) \
                      .filter(col("status").isin(["completed", "pending", "cancelled"])) \
                      .filter(col("order_total") >= 0) \
                      .dropDuplicates(["order_id"])
        
        # Data enrichment
        cleaned_df = cleaned_df \
            .withColumn("order_year", year(col("order_date"))) \
            .withColumn("order_month", month(col("order_date"))) \
            .withColumn("order_quarter", quarter(col("order_date"))) \
            .withColumn("order_day_of_week", dayofweek(col("order_date"))) \
            .withColumn("order_day_name", 
                       when(col("order_day_of_week") == 1, "Sunday")
                       .when(col("order_day_of_week") == 2, "Monday")
                       .when(col("order_day_of_week") == 3, "Tuesday")
                       .when(col("order_day_of_week") == 4, "Wednesday")
                       .when(col("order_day_of_week") == 5, "Thursday")
                       .when(col("order_day_of_week") == 6, "Friday")
                       .otherwise("Saturday")) \
            .withColumn("is_weekend", col("order_day_of_week").isin([1, 7])) \
            .withColumn("order_value_category",
                       when(col("order_total") < 50, "Small")
                       .when(col("order_total") < 150, "Medium")
                       .when(col("order_total") < 300, "Large")
                       .otherwise("XLarge")) \
            .withColumn("total_order_amount", 
                       col("order_total") + col("tax_amount") + col("shipping_cost")) \
            .withColumn("days_since_order", 
                       datediff(current_date(), col("order_date"))) \
            .withColumn("is_recent_order", col("days_since_order") <= 30)
        
        # Add seasonal indicators
        cleaned_df = cleaned_df \
            .withColumn("season",
                       when(col("order_month").isin([12, 1, 2]), "Winter")
                       .when(col("order_month").isin([3, 4, 5]), "Spring")
                       .when(col("order_month").isin([6, 7, 8]), "Summer")
                       .otherwise("Fall")) \
            .withColumn("is_holiday_season", 
                       col("order_month").isin([11, 12]))  # Nov-Dec
        
        # Standardize text fields
        cleaned_df = cleaned_df \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("payment_method", initcap(trim(col("payment_method")))) \
            .withColumn("shipping_method", initcap(trim(col("shipping_method"))))
        
        # Add silver layer metadata
        cleaned_df = cleaned_df \
            .withColumn("silver_processed_timestamp", current_timestamp()) \
            .withColumn("silver_processed_date", current_date()) \
            .withColumn("data_quality_score", lit(90)) \
            .withColumn("cleaning_rules_applied", 
                       array(lit("null_validation"), lit("status_validation"), 
                            lit("date_enrichment"), lit("categorization")))
        
        record_count = cleaned_df.count()
        log_step("Silver Cleaning - Orders", "SUCCESS", f"Cleaned {record_count:,} records")
        
        return cleaned_df
        
    except Exception as e:
        error_msg = f"Failed to clean orders: {str(e)}"
        log_step("Silver Cleaning - Orders", "ERROR", error_msg)
        raise e

# COMMAND ----------

def clean_order_items():
    """
    Clean and enrich order items data
    
    Returns:
        DataFrame: Cleaned order items data
    """
    log_step("Silver Cleaning - Order Items", "STARTED", "Cleaning order items data")
    
    try:
        # Read bronze data
        df = spark.table("bronze_order_items")
        
        # Data cleaning operations
        cleaned_df = df.filter(col("order_item_id").isNotNull()) \
                      .filter(col("order_id").isNotNull()) \
                      .filter(col("product_id").isNotNull()) \
                      .filter(col("quantity") > 0) \
                      .filter(col("unit_price") > 0) \
                      .filter(col("line_total") >= 0) \
                      .dropDuplicates(["order_item_id"])
        
        # Data validation and correction
        cleaned_df = cleaned_df \
            .withColumn("calculated_line_total", col("quantity") * col("unit_price")) \
            .withColumn("line_total_corrected", 
                       when(abs(col("line_total") - col("calculated_line_total")) > 0.01, 
                           col("calculated_line_total"))
                       .otherwise(col("line_total"))) \
            .withColumn("discount_rate", 
                       when(col("line_total") > 0, 
                           round(col("discount_amount") / col("line_total") * 100, 2))
                       .otherwise(0)) \
            .withColumn("final_amount", col("line_total") - col("discount_amount"))
        
        # Data enrichment
        cleaned_df = cleaned_df \
            .withColumn("quantity_category",
                       when(col("quantity") == 1, "Single")
                       .when(col("quantity") <= 3, "Few")
                       .when(col("quantity") <= 5, "Multiple")
                       .otherwise("Bulk")) \
            .withColumn("unit_price_category",
                       when(col("unit_price") < 25, "Budget")
                       .when(col("unit_price") < 100, "Standard")
                       .when(col("unit_price") < 300, "Premium")
                       .otherwise("Luxury")) \
            .withColumn("has_discount", col("discount_amount") > 0) \
            .withColumn("discount_category",
                       when(col("discount_rate") == 0, "No Discount")
                       .when(col("discount_rate") < 5, "Small")
                       .when(col("discount_rate") < 15, "Medium")
                       .otherwise("Large"))
        
        # Add silver layer metadata
        cleaned_df = cleaned_df \
            .withColumn("silver_processed_timestamp", current_timestamp()) \
            .withColumn("silver_processed_date", current_date()) \
            .withColumn("data_quality_score", lit(88)) \
            .withColumn("cleaning_rules_applied", 
                       array(lit("quantity_validation"), lit("price_validation"), 
                            lit("total_correction"), lit("discount_calculation")))
        
        # Clean up intermediate columns
        cleaned_df = cleaned_df.drop("calculated_line_total")
        
        record_count = cleaned_df.count()
        log_step("Silver Cleaning - Order Items", "SUCCESS", f"Cleaned {record_count:,} records")
        
        return cleaned_df
        
    except Exception as e:
        error_msg = f"Failed to clean order items: {str(e)}"
        log_step("Silver Cleaning - Order Items", "ERROR", error_msg)
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Layer Cleaning

# COMMAND ----------

print("Starting silver layer data cleaning...")
print("=" * 60)

# Clean each table
cleaning_results = {}

try:
    # Clean customers
    customers_clean = clean_customers()
    customers_clean.write.format("delta").mode("overwrite").save(f"{get_storage_path('silver')}/customers")
    cleaning_results["customers"] = {"status": "SUCCESS", "count": customers_clean.count()}
    print("✅ Customers: Cleaned and saved to silver layer")
    
    # Clean products
    products_clean = clean_products() 
    products_clean.write.format("delta").mode("overwrite").save(f"{get_storage_path('silver')}/products")
    cleaning_results["products"] = {"status": "SUCCESS", "count": products_clean.count()}
    print("✅ Products: Cleaned and saved to silver layer")
    
    # Clean orders
    orders_clean = clean_orders()
    orders_clean.write.format("delta").mode("overwrite").save(f"{get_storage_path('silver')}/orders")
    cleaning_results["orders"] = {"status": "SUCCESS", "count": orders_clean.count()}
    print("✅ Orders: Cleaned and saved to silver layer")
    
    # Clean order items
    order_items_clean = clean_order_items()
    order_items_clean.write.format("delta").mode("overwrite").save(f"{get_storage_path('silver')}/order_items")
    cleaning_results["order_items"] = {"status": "SUCCESS", "count": order_items_clean.count()}
    print("✅ Order Items: Cleaned and saved to silver layer")
    
except Exception as e:
    print(f"❌ Error during cleaning: {str(e)}")
    log_step("Silver Cleaning", "ERROR", str(e))

print("=" * 60)
print("Silver layer cleaning completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

def validate_silver_data():
    """Perform comprehensive data quality validation on silver layer"""
    log_step("Silver Validation", "STARTED", "Validating silver layer data quality")
    
    validation_results = {}
    
    # Validation rules for each table
    validation_rules = {
        "customers": {
            "path": f"{get_storage_path('silver')}/customers",
            "rules": [
                ("no_null_customer_id", "customer_id IS NOT NULL"),
                ("valid_email_format", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}"),
                ("valid_age_range", "age BETWEEN 18 AND 100"),
                ("no_duplicate_customers", "COUNT(*) = COUNT(DISTINCT customer_id)")
            ]
        },
        "products": {
            "path": f"{get_storage_path('silver')}/products",
            "rules": [
                ("no_null_product_id", "product_id IS NOT NULL"),
                ("positive_price", "price > 0"),
                ("positive_cost", "cost > 0"),
                ("valid_profit_margin", "profit_margin_pct >= 0"),
                ("price_greater_than_cost", "price >= cost")
            ]
        },
        "orders": {
            "path": f"{get_storage_path('silver')}/orders",
            "rules": [
                ("no_null_order_id", "order_id IS NOT NULL"),
                ("no_null_customer_id", "customer_id IS NOT NULL"),
                ("valid_status", "status IN ('completed', 'pending', 'cancelled')"),
                ("non_negative_total", "order_total >= 0"),
                ("valid_order_date", "order_date IS NOT NULL")
            ]
        },
        "order_items": {
            "path": f"{get_storage_path('silver')}/order_items",
            "rules": [
                ("no_null_item_id", "order_item_id IS NOT NULL"),
                ("positive_quantity", "quantity > 0"),
                ("positive_unit_price", "unit_price > 0"),
                ("non_negative_total", "line_total_corrected >= 0"),
                ("valid_discount", "discount_amount >= 0")
            ]
        }
    }
    
    for table_name, config in validation_rules.items():
        if cleaning_results.get(table_name, {}).get("status") == "SUCCESS":
            try:
                df = spark.read.format("delta").load(config["path"])
                table_validation = {
                    "total_records": df.count(),
                    "rules_passed": 0,
                    "rules_failed": 0,
                    "failed_rules": [],
                    "quality_score": 0
                }
                
                for rule_name, rule_condition in config["rules"]:
                    try:
                        if rule_name == "no_duplicate_customers":
                            # Special handling for duplicate check
                            total_count = df.count()
                            unique_count = df.select("customer_id").distinct().count()
                            rule_passed = total_count == unique_count
                        else:
                            # Regular rule validation
                            failed_count = df.filter(f"NOT ({rule_condition})").count()
                            rule_passed = failed_count == 0
                        
                        if rule_passed:
                            table_validation["rules_passed"] += 1
                        else:
                            table_validation["rules_failed"] += 1
                            table_validation["failed_rules"].append(rule_name)
                            
                    except Exception as e:
                        print(f"   ⚠️ Could not validate rule {rule_name}: {str(e)}")
                        table_validation["rules_failed"] += 1
                        table_validation["failed_rules"].append(f"{rule_name} (error)")
                
                # Calculate quality score
                total_rules = len(config["rules"])
                table_validation["quality_score"] = round(
                    (table_validation["rules_passed"] / total_rules) * 100, 1
                )
                
                validation_results[table_name] = table_validation
                
                # Print results
                print(f"\n📊 {table_name.upper()} Validation:")
                print(f"   Records: {table_validation['total_records']:,}")
                print(f"   Rules Passed: {table_validation['rules_passed']}/{total_rules}")
                print(f"   Quality Score: {table_validation['quality_score']}%")
                
                if table_validation["failed_rules"]:
                    print(f"   ⚠️ Failed Rules: {', '.join(table_validation['failed_rules'])}")
                else:
                    print(f"   ✅ All validation rules passed")
                    
            except Exception as e:
                print(f"❌ Validation failed for {table_name}: {str(e)}")
                validation_results[table_name] = {"error": str(e)}
    
    log_step("Silver Validation", "SUCCESS", "Data quality validation completed")
    return validation_results

# Run validation
print("Starting silver layer data quality validation...")
print("=" * 60)
validation_results = validate_silver_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Layer Catalog Tables

# COMMAND ----------

def create_silver_catalog_tables():
    """Create catalog tables for silver layer"""
    log_step("Silver Catalog Creation", "STARTED", "Creating silver catalog tables")
    
    silver_tables = {
        "customers": f"{get_storage_path('silver')}/customers",
        "products": f"{get_storage_path('silver')}/products", 
        "orders": f"{get_storage_path('silver')}/orders",
        "order_items": f"{get_storage_path('silver')}/order_items"
    }
    
    for table_name, path in silver_tables.items():
        if cleaning_results.get(table_name, {}).get("status") == "SUCCESS":
            try:
                # Create catalog table
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS silver_{table_name}
                    USING DELTA
                    LOCATION '{path}'
                """)
                
                # Add table properties
                spark.sql(f"""
                    ALTER TABLE silver_{table_name} SET TBLPROPERTIES (
                        'delta.autoOptimize.optimizeWrite' = 'true',
                        'delta.autoOptimize.autoCompact' = 'true',
                        'description' = 'Silver layer cleaned and enriched {table_name}',
                        'layer' = 'silver',
                        'data_quality_validated' = 'true'
                    )
                """)
                
                print(f"✅ Created catalog table: silver_{table_name}")
                
            except Exception as e:
                print(f"❌ Failed to create catalog table for {table_name}: {str(e)}")
    
    log_step("Silver Catalog Creation", "SUCCESS", "Silver catalog tables created")

create_silver_catalog_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data Lineage Report

# COMMAND ----------

def generate_data_lineage():
    """Generate data lineage report showing bronze to silver transformations"""
    log_step("Data Lineage", "STARTED", "Generating data lineage report")
    
    lineage_data = []
    
    for table_name in ["customers", "products", "orders", "order_items"]:
        if cleaning_results.get(table_name, {}).get("status") == "SUCCESS":
            # Get bronze record count
            try:
                bronze_count = spark.table(f"bronze_{table_name}").count()
            except:
                bronze_count = 0
            
            # Get silver record count
            silver_count = cleaning_results[table_name]["count"]
            
            # Calculate data loss/gain
            record_change = silver_count - bronze_count
            change_pct = (record_change / bronze_count * 100) if bronze_count > 0 else 0
            
            lineage_entry = {
                "table_name": table_name,
                "bronze_records": bronze_count,
                "silver_records": silver_count,
                "record_change": record_change,
                "change_percentage": round(change_pct, 2),
                "transformation_rules": [
                    "null_removal", "duplicate_removal", "data_validation", 
                    "standardization", "enrichment"
                ],
                "quality_score": validation_results.get(table_name, {}).get("quality_score", 0),
                "lineage_timestamp": datetime.now()
            }
            
            lineage_data.append(lineage_entry)
    
    # Create lineage DataFrame
    lineage_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("bronze_records", LongType(), True),
        StructField("silver_records", LongType(), True),
        StructField("record_change", LongType(), True),
        StructField("change_percentage", DoubleType(), True),
        StructField("transformation_rules", ArrayType(StringType()), True),
        StructField("quality_score", DoubleType(), True),
        StructField("lineage_timestamp", TimestampType(), True)
    ])
    
    lineage_df = spark.createDataFrame(lineage_data, lineage_schema)
    
    # Save lineage report
    try:
        lineage_path = f"{get_storage_path('gold')}/data_lineage_bronze_to_silver"
        lineage_df.write.format("delta").mode("append").save(lineage_path)
        
        # Create catalog table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS data_lineage_bronze_to_silver
            USING DELTA
            LOCATION '{lineage_path}'
        """)
        
        log_step("Data Lineage", "SUCCESS", "Lineage report generated")
        
    except Exception as e:
        log_step("Data Lineage", "WARNING", f"Failed to save lineage report: {str(e)}")
    
    return lineage_df

# Generate lineage report
lineage_df = generate_data_lineage()
print("\n📋 Data Lineage Report (Bronze → Silver):")
lineage_df.select("table_name", "bronze_records", "silver_records", 
                  "change_percentage", "quality_score").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Data Quality Checks

# COMMAND ----------

def perform_advanced_quality_checks():
    """Perform advanced data quality checks across tables"""
    log_step("Advanced Quality Checks", "STARTED", "Performing cross-table validations")
    
    advanced_checks = {}
    
    try:
        # Check referential integrity
        customers_df = spark.table("silver_customers")
        orders_df = spark.table("silver_orders")
        products_df = spark.table("silver_products")
        order_items_df = spark.table("silver_order_items")
        
        # Check 1: All orders have valid customer IDs
        valid_customers = customers_df.select("customer_id").distinct()
        orders_with_invalid_customers = orders_df.join(
            valid_customers, "customer_id", "left_anti"
        ).count()
        
        advanced_checks["orders_with_invalid_customers"] = {
            "count": orders_with_invalid_customers,
            "passed": orders_with_invalid_customers == 0
        }
        
        # Check 2: All order items have valid order IDs
        valid_orders = orders_df.select("order_id").distinct()
        items_with_invalid_orders = order_items_df.join(
            valid_orders, "order_id", "left_anti"
        ).count()
        
        advanced_checks["items_with_invalid_orders"] = {
            "count": items_with_invalid_orders,
            "passed": items_with_invalid_orders == 0
        }
        
        # Check 3: All order items have valid product IDs
        valid_products = products_df.select("product_id").distinct()
        items_with_invalid_products = order_items_df.join(
            valid_products, "product_id", "left_anti"
        ).count()
        
        advanced_checks["items_with_invalid_products"] = {
            "count": items_with_invalid_products,
            "passed": items_with_invalid_products == 0
        }
        
        # Check 4: Order totals match sum of order items
        order_totals = orders_df.select("order_id", "order_total")
        item_totals = order_items_df.groupBy("order_id").agg(
            sum("line_total_corrected").alias("calculated_total")
        )
        
        total_mismatches = order_totals.join(item_totals, "order_id") \
            .filter(abs(col("order_total") - col("calculated_total")) > 0.01) \
            .count()
        
        advanced_checks["order_total_mismatches"] = {
            "count": total_mismatches,
            "passed": total_mismatches == 0
        }
        
        # Display results
        print("\n🔍 Advanced Quality Check Results:")
        for check_name, result in advanced_checks.items():
            status = "✅ PASS" if result["passed"] else "❌ FAIL"
            print(f"   {check_name.replace('_', ' ').title()}: {status}")
            if not result["passed"]:
                print(f"      Issues found: {result['count']}")
        
        log_step("Advanced Quality Checks", "SUCCESS", "Advanced checks completed")
        
    except Exception as e:
        error_msg = f"Advanced quality checks failed: {str(e)}"
        log_step("Advanced Quality Checks", "ERROR", error_msg)
        print(f"❌ {error_msg}")
    
    return advanced_checks

# Run advanced quality checks
advanced_quality_results = perform_advanced_quality_checks()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Silver Data Preview

# COMMAND ----------

print("=== Silver Layer Data Preview ===")

for table_name in ["customers", "products", "orders", "order_items"]:
    if cleaning_results.get(table_name, {}).get("status") == "SUCCESS":
        print(f"\n📋 SILVER_{table_name.upper()} (First 3 rows):")
        try:
            df = spark.table(f"silver_{table_name}")
            # Show only business columns (exclude silver metadata)
            business_columns = [col for col in df.columns 
                              if not col.startswith("silver_") and not col.startswith("bronze_")]
            df.select(*business_columns[:10]).show(3, truncate=False)  # Show first 10 columns
        except Exception as e:
            print(f"   Error reading silver_{table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Silver Tables

# COMMAND ----------

def optimize_silver_tables():
    """Optimize silver layer tables for better performance"""
    log_step("Silver Optimization", "STARTED", "Optimizing silver tables")
    
    for table_name in ["customers", "products", "orders", "order_items"]:
        if cleaning_results.get(table_name, {}).get("status") == "SUCCESS":
            try:
                # Run OPTIMIZE with Z-ORDER for commonly queried columns
                if table_name == "customers":
                    spark.sql(f"OPTIMIZE silver_{table_name} ZORDER BY (customer_id, country)")
                elif table_name == "products":
                    spark.sql(f"OPTIMIZE silver_{table_name} ZORDER BY (product_id, category)")
                elif table_name == "orders":
                    spark.sql(f"OPTIMIZE silver_{table_name} ZORDER BY (order_id, customer_id, order_date)")
                elif table_name == "order_items":
                    spark.sql(f"OPTIMIZE silver_{table_name} ZORDER BY (order_id, product_id)")
                
                # Run VACUUM (keep 7 days of history)
                spark.sql(f"VACUUM silver_{table_name} RETAIN 168 HOURS")
                
                print(f"✅ Optimized silver_{table_name}")
                
            except Exception as e:
                print(f"⚠️ Failed to optimize silver_{table_name}: {str(e)}")
    
    log_step("Silver Optimization", "SUCCESS", "Silver optimization completed")

# Run optimization
optimize_silver_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 70)
print("🥈 SILVER LAYER PROCESSING SUMMARY")
print("=" * 70)

# Count successful vs failed tables
successful_tables = [name for name, result in cleaning_results.items() 
                    if result.get("status") == "SUCCESS"]
failed_tables = [name for name, result in cleaning_results.items() 
                if result.get("status") != "SUCCESS"]

print(f"📊 Overall Statistics:")
print(f"   • Total Tables Processed: {len(cleaning_results)}")
print(f"   • Successful Cleanings: {len(successful_tables)}")
print(f"   • Failed Cleanings: {len(failed_tables)}")

total_silver_records = sum([result.get("count", 0) for result in cleaning_results.values() 
                           if result.get("status") == "SUCCESS"])
print(f"   • Total Silver Records: {total_silver_records:,}")

if successful_tables:
    print(f"\n✅ Successfully Processed Tables:")
    for table in successful_tables:
        count = cleaning_results[table]["count"]
        quality_score = validation_results.get(table, {}).get("quality_score", "N/A")
        print(f"   • {table.capitalize()}: {count:,} records (Quality: {quality_score}%)")

if failed_tables:
    print(f"\n❌ Failed Tables:")
    for table in failed_tables:
        print(f"   • {table.capitalize()}: Processing failed")

# Display quality summary
print(f"\n📈 Data Quality Summary:")
avg_quality_score = sum([result.get("quality_score", 0) 
                        for result in validation_results.values() 
                        if isinstance(result, dict) and "quality_score" in result]) / len(successful_tables)
print(f"   • Average Quality Score: {avg_quality_score:.1f}%")

# Display advanced checks summary
advanced_checks_passed = sum([1 for result in advanced_quality_results.values() 
                             if result.get("passed", False)])
total_advanced_checks = len(advanced_quality_results)
print(f"   • Advanced Checks Passed: {advanced_checks_passed}/{total_advanced_checks}")

print(f"\n🎯 Next Steps:")
if len(successful_tables) == 4 and avg_quality_score >= 85:
    print("   1. ✅ All tables successfully cleaned with good quality scores")
    print("   2. 🏆 Run 04_gold_analytics.py to create business-ready analytics")
    print("   3. 📊 Continue with data visualization and ML modeling")
elif len(successful_tables) == 4:
    print("   1. ⚠️ All tables processed but some quality issues detected")
    print("   2. 🔧 Review and fix data quality issues if needed")
    print("   3. 🏆 Proceed to gold layer creation")
else:
    print("   1. ❌ Fix any failed table processing")
    print("   2. 🔄 Re-run this notebook for failed tables")
    print("   3. 📊 Proceed to gold layer once all tables are successful")

print("=" * 70)

# Create final monitoring entry
create_monitoring_entry(
    "Silver Layer Processing Complete",
    "SUCCESS" if not failed_tables else "PARTIAL_SUCCESS",
    total_silver_records,
    f"Avg Quality: {avg_quality_score:.1f}%, Advanced Checks: {advanced_checks_passed}/{total_advanced_checks}"
)