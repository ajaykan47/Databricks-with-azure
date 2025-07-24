# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Analytics and Business Intelligence
# MAGIC 
# MAGIC **Purpose**: Create business-ready analytics tables and KPIs from silver layer data
# MAGIC **Author**: ajaykan47
# MAGIC **Date**: 2025-07-25
# MAGIC **Dependencies**: Silver layer tables, business requirements

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
from datetime import datetime, timedelta
import json

print("Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Analytics Functions

# COMMAND ----------

def create_customer_analytics():
    """
    Create comprehensive customer analytics table
    
    Returns:
        DataFrame: Customer analytics with KPIs and segments
    """
    log_step("Gold Analytics - Customer", "STARTED", "Creating customer analytics")
    
    try:
        # Read silver layer data
        customers = spark.table("silver_customers")
        orders = spark.table("silver_orders")
        order_items = spark.table("silver_order_items")
        
        # Calculate customer order metrics
        customer_orders = orders.filter(col("status") == "completed") \
            .groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                sum("order_total").alias("total_spent"),
                avg("order_total").alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                min("order_date").alias("first_order_date"),
                sum("tax_amount").alias("total_tax_paid"),
                sum("shipping_cost").alias("total_shipping_paid"),
                countDistinct("order_year").alias("active_years"),
                countDistinct("order_month").alias("active_months")
            )
        
        # Calculate customer product preferences
        customer_products = orders.filter(col("status") == "completed") \
            .join(order_items, "order_id") \
            .join(spark.table("silver_products").select("product_id", "category"), "product_id") \
            .groupBy("customer_id") \
            .agg(
                countDistinct("product_id").alias("unique_products_bought"),
                countDistinct("category").alias("categories_shopped"),
                sum("quantity").alias("total_items_bought")
            )
        
        # Find most frequent category per customer
        customer_fav_category = orders.filter(col("status") == "completed") \
            .join(order_items, "order_id") \
            .join(spark.table("silver_products").select("product_id", "category"), "product_id") \
            .groupBy("customer_id", "category") \
            .agg(count("*").alias("category_orders")) \
            .withColumn("rank", row_number().over(
                Window.partitionBy("customer_id").orderBy(desc("category_orders"))
            )) \
            .filter(col("rank") == 1) \
            .select("customer_id", col("category").alias("favorite_category"))
        
        # Join all customer metrics
        customer_analytics = customers.alias("c") \
            .join(customer_orders.alias("co"), col("c.customer_id") == col("co.customer_id"), "left") \
            .join(customer_products.alias("cp"), col("c.customer_id") == col("cp.customer_id"), "left") \
            .join(customer_fav_category.alias("cf"), col("c.customer_id") == col("cf.customer_id"), "left")
        
        # Calculate derived metrics and segments
        customer_analytics = customer_analytics.select(
            col("c.customer_id"),
            col("c.full_name"),
            col("c.email"),
            col("c.country"),
            col("c.city"),
            col("c.customer_age_group"),
            col("c.customer_segment"),
            col("c.registration_date"),
            col("c.days_since_registration"),
            col("c.customer_lifetime_category"),
            
            # Order metrics
            coalesce(col("co.total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("co.total_spent"), lit(0.0)).alias("total_spent"),
            coalesce(col("co.avg_order_value"), lit(0.0)).alias("avg_order_value"),
            col("co.last_order_date"),
            col("co.first_order_date"),
            coalesce(col("co.total_tax_paid"), lit(0.0)).alias("total_tax_paid"),
            coalesce(col("co.total_shipping_paid"), lit(0.0)).alias("total_shipping_paid"),
            coalesce(col("co.active_years"), lit(0)).alias("active_years"),
            coalesce(col("co.active_months"), lit(0)).alias("active_months"),
            
            # Product metrics
            coalesce(col("cp.unique_products_bought"), lit(0)).alias("unique_products_bought"),
            coalesce(col("cp.categories_shopped"), lit(0)).alias("categories_shopped"),
            coalesce(col("cp.total_items_bought"), lit(0)).alias("total_items_bought"),
            col("cf.favorite_category"),
            
            # Calculated KPIs
            when(col("co.last_order_date").isNotNull(), 
                 datediff(current_date(), col("co.last_order_date")))
            .otherwise(lit(null)).alias("days_since_last_order"),
            
            when(col("co.first_order_date").isNotNull() & col("co.last_order_date").isNotNull(),
                 datediff(col("co.last_order_date"), col("co.first_order_date")))
            .otherwise(lit(0)).alias("customer_lifespan_days"),
            
            when(col("co.total_orders") > 0,
                 col("co.total_spent") / col("co.total_orders"))
            .otherwise(lit(0.0)).alias("calculated_aov"),
            
            # Customer value segments
            when(col("co.total_spent") >= 1000, "High Value")
            .when(col("co.total_spent") >= 500, "Medium Value")  
            .when(col("co.total_spent") >= 100, "Low Value")
            .when(col("co.total_spent") > 0, "Minimal Value")
            .otherwise("No Purchase").alias("value_segment"),
            
            # Recency segments
            when(col("co.last_order_date").isNull(), "Never Purchased")
            .when(datediff(current_date(), col("co.last_order_date")) <= 30, "Recent")
            .when(datediff(current_date(), col("co.last_order_date")) <= 90, "Active") 
            .when(datediff(current_date(), col("co.last_order_date")) <= 365, "At Risk")
            .otherwise("Churned").alias("recency_segment"),
            
            # Frequency segments
            when(col("co.total_orders") >= 10, "Frequent")
            .when(col("co.total_orders") >= 5, "Regular")
            .when(col("co.total_orders") >= 2, "Occasional")
            .when(col("co.total_orders") == 1, "One-time")
            .otherwise("No Purchase").alias("frequency_segment")
        )
        
        # Add gold layer metadata
        customer_analytics = customer_analytics \
            .withColumn("gold_created_timestamp", current_timestamp()) \
            .withColumn("gold_created_date", current_date()) \
            .withColumn("analytics_version", lit("1.0")) \
            .withColumn("last_updated", current_timestamp())
        
        record_count = customer_analytics.count()
        log_step("Gold Analytics - Customer", "SUCCESS", f"Created {record_count:,} customer analytics records")
        
        return customer_analytics
        
    except Exception as e:
        error_msg = f"Failed to create customer analytics: {str(e)}"
        log_step("Gold Analytics - Customer", "ERROR", error_msg)
        raise e

# COMMAND ----------

def create_product_analytics():
    """
    Create comprehensive product analytics table
    
    Returns:
        DataFrame: Product analytics with performance metrics
    """
    log_step("Gold Analytics - Product", "STARTED", "Creating product analytics")
    
    try:
        # Read silver layer data
        products = spark.table("silver_products")
        orders = spark.table("silver_orders").filter(col("status") == "completed")
        order_items = spark.table("silver_order_items")
        
       # Calculate product sales metrics
        product_sales = order_items.alias("oi") \
            .join(orders.alias("o"), "order_id") \
            .filter(col("o.status") == "completed") \
            .groupBy("oi.product_id") \
            .agg(
                sum("oi.quantity").alias("total_quantity_sold"),
                sum("oi.line_total_corrected").alias("total_revenue"),
                sum("oi.discount_amount").alias("total_discounts_given"),
                count("oi.order_id").alias("total_order_appearances"),
                countDistinct("oi.order_id").alias("unique_orders"),
                countDistinct("o.customer_id").alias("unique_customers"),
                avg("oi.unit_price").alias("avg_selling_price"),
                max("o.order_date").alias("last_sold_date"),
                min("o.order_date").alias("first_sold_date")
            )
        
        # Calculate product rankings and performance metrics
        window_spec = Window.orderBy(desc("total_revenue"))
        
        product_analytics = products.alias("p") \
            .join(product_sales.alias("ps"), col("p.product_id") == col("ps.product_id"), "left") \
            .select(
                col("p.product_id"),
                col("p.product_name"),
                col("p.category"),
                col("p.sub_category"),
                col("p.brand"),
                col("p.price"),
                col("p.cost"),
                col("p.profit_margin_pct"),
                col("p.profit_amount"),
                col("p.price_category"),
                col("p.weight_category"),
                col("p.availability_status"),
                col("p.stock_quantity"),
                col("p.in_stock"),
                
                # Sales metrics
                coalesce(col("ps.total_quantity_sold"), lit(0)).alias("total_quantity_sold"),
                coalesce(col("ps.total_revenue"), lit(0.0)).alias("total_revenue"),
                coalesce(col("ps.total_discounts_given"), lit(0.0)).alias("total_discounts_given"),
                coalesce(col("ps.total_order_appearances"), lit(0)).alias("total_order_appearances"),
                coalesce(col("ps.unique_orders"), lit(0)).alias("unique_orders"),
                coalesce(col("ps.unique_customers"), lit(0)).alias("unique_customers"),
                coalesce(col("ps.avg_selling_price"), lit(0.0)).alias("avg_selling_price"),
                col("ps.last_sold_date"),
                col("ps.first_sold_date"),
                
                # Calculated metrics
                when(col("ps.total_quantity_sold") > 0,
                     col("ps.total_revenue") / col("ps.total_quantity_sold"))
                .otherwise(lit(0.0)).alias("revenue_per_unit"),
                
                when(col("ps.total_quantity_sold") > 0,
                     (col("ps.total_revenue") - (col("p.cost") * col("ps.total_quantity_sold"))))
                .otherwise(lit(0.0)).alias("total_profit"),
                
                when(col("ps.unique_orders") > 0,
                     col("ps.total_quantity_sold") / col("ps.unique_orders"))
                .otherwise(lit(0.0)).alias("avg_quantity_per_order"),
                
                # Performance categories
                when(col("ps.total_quantity_sold") >= 100, "Top Seller")
                .when(col("ps.total_quantity_sold") >= 50, "Good Seller")
                .when(col("ps.total_quantity_sold") >= 10, "Average Seller")
                .when(col("ps.total_quantity_sold") > 0, "Slow Seller")
                .otherwise("No Sales").alias("sales_performance"),
                
                # Demand categories
                when(col("ps.unique_customers") >= 50, "High Demand")
                .when(col("ps.unique_customers") >= 20, "Medium Demand")
                .when(col("ps.unique_customers") >= 5, "Low Demand")
                .when(col("ps.unique_customers") > 0, "Very Low Demand")
                .otherwise("No Demand").alias("demand_level"),
                
                # Days since last sale
                when(col("ps.last_sold_date").isNotNull(),
                     datediff(current_date(), col("ps.last_sold_date")))
                .otherwise(lit(null)).alias("days_since_last_sale")
            )
        
        # Add revenue ranking
        product_analytics = product_analytics \
            .withColumn("revenue_rank", 
                       row_number().over(Window.orderBy(desc("total_revenue")))) \
            .withColumn("gold_created_timestamp", current_timestamp()) \
            .withColumn("gold_created_date", current_date()) \
            .withColumn("analytics_version", lit("1.0")) \
            .withColumn("last_updated", current_timestamp())
        
        record_count = product_analytics.count()
        log_step("Gold Analytics - Product", "SUCCESS", f"Created {record_count:,} product analytics records")
        
        return product_analytics
        
    except Exception as e:
        error_msg = f"Failed to create product analytics: {str(e)}"
        log_step("Gold Analytics - Product", "ERROR", error_msg)
        raise e

# COMMAND ----------

def create_sales_summary():
    """
    Create comprehensive sales summary by different dimensions
    
    Returns:
        tuple: (monthly_sales, daily_sales, category_sales)
    """
    log_step("Gold Analytics - Sales Summary", "STARTED", "Creating sales summaries")
    
    try:
        # Read silver layer data
        orders = spark.table("silver_orders").filter(col("status") == "completed")
        order_items = spark.table("silver_order_items")
        products = spark.table("silver_products")
        customers = spark.table("silver_customers")
        
        # Monthly Sales Summary
        monthly_sales = orders.groupBy("order_year", "order_month") \
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("order_total").alias("total_revenue"),
                avg("order_total").alias("avg_order_value"),
                sum("tax_amount").alias("total_tax"),
                sum("shipping_cost").alias("total_shipping"),
                min("order_date").alias("month_start_date"),
                max("order_date").alias("month_end_date")
            ) \
            .withColumn("revenue_rank", 
                       row_number().over(Window.orderBy(desc("total_revenue")))) \
            .withColumn("month_name", 
                       when(col("order_month") == 1, "January")
                       .when(col("order_month") == 2, "February")
                       .when(col("order_month") == 3, "March")
                       .when(col("order_month") == 4, "April")
                       .when(col("order_month") == 5, "May")
                       .when(col("order_month") == 6, "June")
                       .when(col("order_month") == 7, "July")
                       .when(col("order_month") == 8, "August")
                       .when(col("order_month") == 9, "September")
                       .when(col("order_month") == 10, "October")
                       .when(col("order_month") == 11, "November")
                       .otherwise("December")) \
            .withColumn("year_month", concat(col("order_year"), lit("-"), 
                       lpad(col("order_month"), 2, "0"))) \
            .orderBy("order_year", "order_month")
        
        # Daily Sales Summary
        daily_sales = orders.groupBy("order_date", "order_day_name", "is_weekend") \
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("order_total").alias("total_revenue"),
                avg("order_total").alias("avg_order_value")
            ) \
            .withColumn("weekday_performance",
                       when(col("is_weekend"), "Weekend")
                       .otherwise("Weekday")) \
            .orderBy("order_date")
        
        # Category Sales Summary
        category_sales = orders.alias("o") \
            .join(order_items.alias("oi"), "order_id") \
            .join(products.alias("p"), "product_id") \
            .groupBy("p.category") \
            .agg(
                sum("oi.quantity").alias("total_quantity_sold"),
                sum("oi.line_total_corrected").alias("total_revenue"),
                count("oi.order_item_id").alias("total_items"),
                countDistinct("o.order_id").alias("unique_orders"),
                countDistinct("o.customer_id").alias("unique_customers"),
                countDistinct("p.product_id").alias("unique_products"),
                avg("oi.unit_price").alias("avg_selling_price"),
                sum("oi.discount_amount").alias("total_discounts")
            ) \
            .withColumn("revenue_rank", 
                       row_number().over(Window.orderBy(desc("total_revenue")))) \
            .withColumn("avg_revenue_per_product", 
                       col("total_revenue") / col("unique_products")) \
            .orderBy(desc("total_revenue"))
        
        # Add metadata to all summaries
        for df_name, df in [("monthly_sales", monthly_sales), ("daily_sales", daily_sales), ("category_sales", category_sales)]:
            df = df.withColumn("gold_created_timestamp", current_timestamp()) \
                  .withColumn("gold_created_date", current_date()) \
                  .withColumn("analytics_version", lit("1.0"))
            
            if df_name == "monthly_sales":
                monthly_sales = df
            elif df_name == "daily_sales":
                daily_sales = df
            else:
                category_sales = df
        
        log_step("Gold Analytics - Sales Summary", "SUCCESS", "Created all sales summary tables")
        
        return monthly_sales, daily_sales, category_sales
        
    except Exception as e:
        error_msg = f"Failed to create sales summaries: {str(e)}"
        log_step("Gold Analytics - Sales Summary", "ERROR", error_msg)
        raise e

# COMMAND ----------

def create_cohort_analysis():
    """
    Create customer cohort analysis for retention insights
    
    Returns:
        DataFrame: Customer cohort analysis
    """
    log_step("Gold Analytics - Cohort Analysis", "STARTED", "Creating cohort analysis")
    
    try:
        # Read silver layer data
        customers = spark.table("silver_customers")
        orders = spark.table("silver_orders").filter(col("status") == "completed")
        
        # Define customer first purchase (cohort) month
        customer_cohorts = customers.alias("c") \
            .join(orders.alias("o"), "customer_id") \
            .groupBy("c.customer_id", "c.registration_date") \
            .agg(min("o.order_date").alias("first_purchase_date")) \
            .withColumn("cohort_month", 
                       concat(year("first_purchase_date"), lit("-"), 
                             lpad(month("first_purchase_date"), 2, "0")))
        
        # Calculate customer activity by month
        customer_monthly_activity = orders.alias("o") \
            .join(customer_cohorts.alias("cc"), "customer_id") \
            .withColumn("activity_month", 
                       concat(col("order_year"), lit("-"), 
                             lpad(col("order_month"), 2, "0"))) \
            .withColumn("months_since_first_purchase",
                       months_between(col("o.order_date"), col("cc.first_purchase_date")))
        
        # Create cohort table
        cohort_analysis = customer_monthly_activity \
            .groupBy("cohort_month", "months_since_first_purchase") \
            .agg(
                countDistinct("customer_id").alias("active_customers"),
                sum("order_total").alias("cohort_revenue"),
                count("order_id").alias("cohort_orders")
            )
        
        # Calculate cohort sizes (total customers in each cohort)
        cohort_sizes = customer_cohorts.groupBy("cohort_month") \
            .agg(countDistinct("customer_id").alias("cohort_size"))
        
        # Join with cohort sizes to calculate retention rates
        cohort_analysis = cohort_analysis.alias("ca") \
            .join(cohort_sizes.alias("cs"), "cohort_month") \
            .withColumn("retention_rate", 
                       round((col("active_customers") / col("cohort_size")) * 100, 2)) \
            .withColumn("revenue_per_customer", 
                       round(col("cohort_revenue") / col("active_customers"), 2)) \
            .select(
                "cohort_month",
                "months_since_first_purchase", 
                "cohort_size",
                "active_customers",
                "retention_rate",
                "cohort_revenue",
                "revenue_per_customer",
                "cohort_orders"
            ) \
            .withColumn("gold_created_timestamp", current_timestamp()) \
            .withColumn("gold_created_date", current_date()) \
            .withColumn("analytics_version", lit("1.0")) \
            .orderBy("cohort_month", "months_since_first_purchase")
        
        record_count = cohort_analysis.count()
        log_step("Gold Analytics - Cohort Analysis", "SUCCESS", f"Created {record_count:,} cohort records")
        
        return cohort_analysis
        
    except Exception as e:
        error_msg = f"Failed to create cohort analysis: {str(e)}"
        log_step("Gold Analytics - Cohort Analysis", "ERROR", error_msg)
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Layer Creation

# COMMAND ----------

print("Starting gold layer analytics creation...")
print("=" * 60)

# Create analytics tables
analytics_results = {}

try:
    # Create customer analytics
    customer_analytics = create_customer_analytics()
    customer_analytics.write.format("delta").mode("overwrite") \
        .save(f"{get_storage_path('gold')}/customer_analytics")
    analytics_results["customer_analytics"] = {"status": "SUCCESS", "count": customer_analytics.count()}
    print("✅ Customer Analytics: Created and saved")
    
    # Create product analytics
    product_analytics = create_product_analytics()
    product_analytics.write.format("delta").mode("overwrite") \
        .save(f"{get_storage_path('gold')}/product_analytics")
    analytics_results["product_analytics"] = {"status": "SUCCESS", "count": product_analytics.count()}
    print("✅ Product Analytics: Created and saved")
    
    # Create sales summaries
    monthly_sales, daily_sales, category_sales = create_sales_summary()
    
    monthly_sales.write.format("delta").mode("overwrite") \
        .save(f"{get_storage_path('gold')}/monthly_sales")
    analytics_results["monthly_sales"] = {"status": "SUCCESS", "count": monthly_sales.count()}
    
    daily_sales.write.format("delta").mode("overwrite") \
        .save(f"{get_storage_path('gold')}/daily_sales")
    analytics_results["daily_sales"] = {"status": "SUCCESS", "count": daily_sales.count()}
    
    category_sales.write.format("delta").mode("overwrite") \
        .save(f"{get_storage_path('gold')}/category_sales")
    analytics_results["category_sales"] = {"status": "SUCCESS", "count": category_sales.count()}
    
    print("✅ Sales Summaries: All created and saved")
    
    # Create cohort analysis
    cohort_analysis = create_cohort_analysis()
    cohort_analysis.write.format("delta").mode("overwrite") \
        .save(f"{get_storage_path('gold')}/cohort_analysis")
    analytics_results["cohort_analysis"] = {"status": "SUCCESS", "count": cohort_analysis.count()}
    print("✅ Cohort Analysis: Created and saved")
    
except Exception as e:
    print(f"❌ Error during gold layer creation: {str(e)}")
    log_step("Gold Layer Creation", "ERROR", str(e))

print("=" * 60)
print("Gold layer analytics creation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Layer Catalog Tables

# COMMAND ----------

def create_gold_catalog_tables():
    """Create catalog tables for gold layer analytics"""
    log_step("Gold Catalog Creation", "STARTED", "Creating gold catalog tables")
    
    gold_tables = {
        "customer_analytics": f"{get_storage_path('gold')}/customer_analytics",
        "product_analytics": f"{get_storage_path('gold')}/product_analytics",
        "monthly_sales": f"{get_storage_path('gold')}/monthly_sales",
        "daily_sales": f"{get_storage_path('gold')}/daily_sales", 
        "category_sales": f"{get_storage_path('gold')}/category_sales",
        "cohort_analysis": f"{get_storage_path('gold')}/cohort_analysis"
    }
    
    for table_name, path in gold_tables.items():
        if analytics_results.get(table_name, {}).get("status") == "SUCCESS":
            try:
                # Create catalog table
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS gold_{table_name}
                    USING DELTA
                    LOCATION '{path}'
                """)
                
                # Add table properties
                spark.sql(f"""
                    ALTER TABLE gold_{table_name} SET TBLPROPERTIES (
                        'delta.autoOptimize.optimizeWrite' = 'true',
                        'delta.autoOptimize.autoCompact' = 'true',
                        'description' = 'Gold layer business analytics for {table_name}',
                        'layer' = 'gold',
                        'business_ready' = 'true',
                        'analytics_version' = '1.0'
                    )
                """)
                
                print(f"✅ Created catalog table: gold_{table_name}")
                
            except Exception as e:
                print(f"❌ Failed to create catalog table for {table_name}: {str(e)}")
    
    log_step("Gold Catalog Creation", "SUCCESS", "Gold catalog tables created")

create_gold_catalog_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Business KPIs Dashboard

# COMMAND ----------

def generate_business_kpis():
    """Generate key business KPIs for executive dashboard"""
    log_step("Business KPIs", "STARTED", "Calculating business KPIs")
    
    try:
        # Overall business metrics
        total_customers = spark.table("gold_customer_analytics").count()
        active_customers = spark.table("gold_customer_analytics") \
            .filter(col("recency_segment").isin(["Recent", "Active"])).count()
        
        total_products = spark.table("gold_product_analytics").count()
        products_with_sales = spark.table("gold_product_analytics") \
            .filter(col("total_quantity_sold") > 0).count()
        
        # Revenue metrics
        total_revenue = spark.table("gold_monthly_sales") \
            .agg(sum("total_revenue")).collect()[0][0] or 0
        
        total_orders = spark.table("gold_monthly_sales") \
            .agg(sum("total_orders")).collect()[0][0] or 0
        
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        
        # Customer metrics
        high_value_customers = spark.table("gold_customer_analytics") \
            .filter(col("value_segment") == "High Value").count()
        
        # Product metrics
        top_category = spark.table("gold_category_sales") \
            .orderBy(desc("total_revenue")).select("category").first()
        top_category_name = top_category[0] if top_category else "N/A"
        
        # Create KPI summary
        kpi_data = [{
            "metric_name": "Total Customers",
            "metric_value": total_customers,
            "metric_type": "count",
            "category": "Customer"
        }, {
            "metric_name": "Active Customers",
            "metric_value": active_customers,
            "metric_type": "count", 
            "category": "Customer"
        }, {
            "metric_name": "Customer Activation Rate",
            "metric_value": round((active_customers / total_customers * 100), 2),
            "metric_type": "percentage",
            "category": "Customer"
        }, {
            "metric_name": "Total Revenue",
            "metric_value": round(total_revenue, 2),
            "metric_type": "currency",
            "category": "Revenue"
        }, {
            "metric_name": "Total Orders",
            "metric_value": total_orders,
            "metric_type": "count",
            "category": "Orders"
        }, {
            "metric_name": "Average Order Value",
            "metric_value": round(avg_order_value, 2),
            "metric_type": "currency",
            "category": "Orders"
        }, {
            "metric_name": "Total Products",
            "metric_value": total_products,
            "metric_type": "count",
            "category": "Product"
        }, {
            "metric_name": "Products with Sales",
            "metric_value": products_with_sales,
            "metric_type": "count",
            "category": "Product"
        }, {
            "metric_name": "Product Sales Rate",
            "metric_value": round((products_with_sales / total_products * 100), 2),
            "metric_type": "percentage",
            "category": "Product"
        }, {
            "metric_name": "High Value Customers",
            "metric_value": high_value_customers,
            "metric_type": "count",
            "category": "Customer"
        }, {
            "metric_name": "Top Category",
            "metric_value": top_category_name,
            "metric_type": "text",
            "category": "Product"
        }]
        
        # Add metadata
        for kpi in kpi_data:
            kpi["kpi_date"] = datetime.now().date()
            kpi["kpi_timestamp"] = datetime.now()
        
        kpi_schema = StructType([
            StructField("metric_name", StringType(), True),
            StructField("metric_value", StringType(), True),  # Store as string for flexibility
            StructField("metric_type", StringType(), True),
            StructField("category", StringType(), True),
            StructField("kpi_date", DateType(), True),
            StructField("kpi_timestamp", TimestampType(), True)
        ])
        
        # Convert values to strings
        for kpi in kpi_data:
            kpi["metric_value"] = str(kpi["metric_value"])
        
        kpi_df = spark.createDataFrame(kpi_data, kpi_schema)
        
        # Save KPI dashboard
        kpi_path = f"{get_storage_path('gold')}/business_kpis"
        kpi_df.write.format("delta").mode("append").save(kpi_path)
        
        # Create catalog table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold_business_kpis
            USING DELTA
            LOCATION '{kpi_path}'
        """)
        
        log_step("Business KPIs", "SUCCESS", f"Generated {len(kpi_data)} KPIs")
        
        return kpi_df
        
    except Exception as e:
        error_msg = f"Failed to generate business KPIs: {str(e)}"
        log_step("Business KPIs", "ERROR", error_msg)
        print(f"❌ {error_msg}")
        return None

# Generate KPIs
business_kpis = generate_business_kpis()
if business_kpis:
    print("\n📊 Business KPIs Generated:")
    business_kpis.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Analytics Queries

# COMMAND ----------

print("=== Sample Business Analytics Queries ===")

# Customer Analytics
print("\n📈 TOP 5 CUSTOMERS BY TOTAL SPENT:")
spark.table("gold_customer_analytics") \
    .select("full_name", "total_spent", "total_orders", "value_segment") \
    .orderBy(desc("total_spent")) \
    .show(5)

# Product Analytics  
print("\n🏆 TOP 5 PRODUCTS BY REVENUE:")
spark.table("gold_product_analytics") \
    .select("product_name", "category", "total_revenue", "total_quantity_sold") \
    .orderBy(desc("total_revenue")) \
    .show(5)

# Monthly Sales Trend
print("\n📅 MONTHLY SALES PERFORMANCE:")
spark.table("gold_monthly_sales") \
    .select("year_month", "total_orders", "total_revenue", "avg_order_value") \
    .orderBy("order_year", "order_month") \
    .show(10)

# Category Performance
print("\n🛍️ CATEGORY PERFORMANCE:")
spark.table("gold_category_sales") \
    .select("category", "total_revenue", "unique_customers", "avg_selling_price") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Assessment

# COMMAND ----------

def assess_gold_layer_quality():
    """Assess data quality of gold layer analytics"""
    log_step("Gold Quality Assessment", "STARTED", "Assessing gold layer quality")
    
    quality_results = {}
    
    for table_name in ["customer_analytics", "product_analytics", "monthly_sales", 
                      "daily_sales", "category_sales", "cohort_analysis"]:
        if analytics_results.get(table_name, {}).get("status") == "SUCCESS":
            try:
                df = spark.table(f"gold_{table_name}")
                
                # Basic quality checks
                total_records = df.count()
                
                # Check for nulls in key business metrics
                null_checks = {}
                key_columns = {
                    "customer_analytics": ["customer_id", "total_spent", "total_orders"],
                    "product_analytics": ["product_id", "total_revenue", "sales_performance"],
                    "monthly_sales": ["order_year", "order_month", "total_revenue"],
                    "daily_sales": ["order_date", "total_revenue"],
                    "category_sales": ["category", "total_revenue"],
                    "cohort_analysis": ["cohort_month", "retention_rate"]
                }
                
                for col_name in key_columns.get(table_name, []):
                    if col_name in df.columns:
                        null_count = df.filter(col(col_name).isNull()).count()
                        null_checks[col_name] = null_count
                
                # Calculate quality score
                total_null_issues = sum(null_checks.values())
                quality_score = max(0, 100 - (total_null_issues / total_records * 100)) if total_records > 0 else 0
                
                quality_results[table_name] = {
                    "total_records": total_records,
                    "null_issues": total_null_issues,
                    "quality_score": round(quality_score, 1),
                    "status": "PASS" if quality_score >= 90 else "WARNING" if quality_score >= 70 else "FAIL"
                }
                
            except Exception as e:
                quality_results[table_name] = {"error": str(e), "status": "ERROR"}
    
    # Display quality results
    print("\n📋 Gold Layer Quality Assessment:")
    for table, result in quality_results.items():
        if "error" not in result:
            status_emoji = "✅" if result["status"] == "PASS" else "⚠️" if result["status"] == "WARNING" else "❌"
            print(f"   {status_emoji} {table}: {result['total_records']:,} records, "
                  f"Quality Score: {result['quality_score']}%")
        else:
            print(f"   ❌ {table}: Error - {result['error']}")
    
    log_step("Gold Quality Assessment", "SUCCESS", "Quality assessment completed")
    return quality_results

# Run quality assessment
gold_quality_results = assess_gold_layer_quality()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Tables

# COMMAND ----------

def optimize_gold_tables():
    """Optimize gold layer tables for query performance"""
    log_step("Gold Optimization", "STARTED", "Optimizing gold tables")
    
    optimization_config = {
        "customer_analytics": ["customer_id", "value_segment", "country"],
        "product_analytics": ["product_id", "category", "sales_performance"],
        "monthly_sales": ["order_year", "order_month"],
        "daily_sales": ["order_date"],
        "category_sales": ["category"],
        "cohort_analysis": ["cohort_month"]
    }
    
    for table_name, z_order_cols in optimization_config.items():
        if analytics_results.get(table_name, {}).get("status") == "SUCCESS":
            try:
                # Run OPTIMIZE with Z-ORDER
                z_order_clause = ", ".join(z_order_cols)
                spark.sql(f"OPTIMIZE gold_{table_name} ZORDER BY ({z_order_clause})")
                
                # Run VACUUM (keep 7 days)
                spark.sql(f"VACUUM gold_{table_name} RETAIN 168 HOURS")
                
                print(f"✅ Optimized gold_{table_name}")
                
            except Exception as e:
                print(f"⚠️ Failed to optimize gold_{table_name}: {str(e)}")
    
    log_step("Gold Optimization", "SUCCESS", "Gold optimization completed")

# Run optimization
optimize_gold_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 70)
print("🏆 GOLD LAYER ANALYTICS SUMMARY")
print("=" * 70)

# Count successful vs failed analytics
successful_analytics = [name for name, result in analytics_results.items() 
                       if result.get("status") == "SUCCESS"]
failed_analytics = [name for name, result in analytics_results.items() 
                   if result.get("status") != "SUCCESS"]

print(f"📊 Overall Statistics:")
print(f"   • Total Analytics Tables Created: {len(analytics_results)}")
print(f"   • Successful Creations: {len(successful_analytics)}")
print(f"   • Failed Creations: {len(failed_analytics)}")

total_gold_records = sum([result.get("count", 0) for result in analytics_results.values() 
                         if result.get("status") == "SUCCESS"])
print(f"   • Total Gold Records: {total_gold_records:,}")

if successful_analytics:
    print(f"\n✅ Successfully Created Analytics:")
    for table in successful_analytics:
        count = analytics_results[table]["count"]
        quality = gold_quality_results.get(table, {}).get("quality_score", "N/A")
        print(f"   • {table.replace('_', ' ').title()}: {count:,} records (Quality: {quality}%)")

if failed_analytics:
    print(f"\n❌ Failed Analytics:")
    for table in failed_analytics:
        print(f"   • {table.replace('_', ' ').title()}: Creation failed")

# Display overall quality summary
avg_quality_score = sum([result.get("quality_score", 0) 
                        for result in gold_quality_results.values() 
                        if isinstance(result, dict) and "quality_score" in result]) / len(successful_analytics) if successful_analytics else 0

print(f"\n📈 Quality Summary:")
print(f"   • Average Quality Score: {avg_quality_score:.1f}%")
print(f"   • Business KPIs Generated: {'✅ Yes' if business_kpis else '❌ No'}")

# Display key business insights
if business_kpis:
    print(f"\n💼 Key Business Insights:")
    kpi_dict = {row['metric_name']: row['metric_value'] for row in business_kpis.collect()}
    print(f"   • Total Revenue: ${kpi_dict.get('Total Revenue', 'N/A')}")
    print(f"   • Total Customers: {kpi_dict.get('Total Customers', 'N/A')}")
    print(f"   • Average Order Value: ${kpi_dict.get('Average Order Value', 'N/A')}")
    print(f"   • Top Category: {kpi_dict.get('Top Category', 'N/A')}")

print(f"\n🎯 Next Steps:")
if len(successful_analytics) == 6 and avg_quality_score >= 90:
    print("   1. ✅ All analytics created with excellent quality scores")
    print("   2. 📊 Run 05_data_analysis.py to create visualizations")
    print("   3. 🤖 Continue with machine learning modeling")
    print("   4. 📈 Set up automated reporting and dashboards")
elif len(successful_analytics) == 6:
    print("   1. ⚠️ All analytics created but some quality issues detected")
    print("   2. 🔧 Review and address data quality concerns")
    print("   3. 📊 Proceed with data visualization")
else:
    print("   1. ❌ Fix any failed analytics creation")
    print("   2. 🔄 Re-run this notebook for failed tables")
    print("   3. 📊 Proceed to visualization once all analytics are successful")

print(f"\n📋 Available Gold Tables for Analysis:")
for table in successful_analytics:
    print(f"   • gold_{table}")

print("=" * 70)

# Create final monitoring entry
create_monitoring_entry(
    "Gold Layer Analytics Complete",
    "SUCCESS" if not failed_analytics else "PARTIAL_SUCCESS",
    total_gold_records,
    f"Analytics: {len(successful_analytics)}/{len(analytics_results)}, Avg Quality: {avg_quality_score:.1f}%"
)# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Analytics and Business Intelligence
# MAGIC 
# MAGIC **Purpose**: Create business-ready analytics tables and KPIs from silver layer data
# MAGIC **Author**: Your Name
# MAGIC **Date**: 2025-01-XX
# MAGIC **Dependencies**: Silver layer tables, business requirements

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
from datetime import datetime, timedelta
import json

print("Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Analytics Functions

# COMMAND ----------

def create_customer_analytics():
    """
    Create comprehensive customer analytics table
    
    Returns:
        DataFrame: Customer analytics with KPIs and segments
    """
    log_step("Gold Analytics - Customer", "STARTED", "Creating customer analytics")
    
    try:
        # Read silver layer data
        customers = spark.table("silver_customers")
        orders = spark.table("silver_orders")
        order_items = spark.table("silver_order_items")
        
        # Calculate customer order metrics
        customer_orders = orders.filter(col("status") == "completed") \
            .groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                sum("order_total").alias("total_spent"),
                avg("order_total").alias("avg_order_value"),
                max("order_date").alias("last_order_date"),
                min("order_date").alias("first_order_date"),
                sum("tax_amount").alias("total_tax_paid"),
                sum("shipping_cost").alias("total_shipping_paid"),
                countDistinct("order_year").alias("active_years"),
                countDistinct("order_month").alias("active_months")
            )
        
        # Calculate customer product preferences
        customer_products = orders.filter(col("status") == "completed") \
            .join(order_items, "order_id") \
            .join(spark.table("silver_products").select("product_id", "category"), "product_id") \
            .groupBy("customer_id") \
            .agg(
                countDistinct("product_id").alias("unique_products_bought"),
                countDistinct("category").alias("categories_shopped"),
                sum("quantity").alias("total_items_bought")
            )
        
        # Find most frequent category per customer
        customer_fav_category = orders.filter(col("status") == "completed") \
            .join(order_items, "order_id") \
            .join(spark.table("silver_products").select("product_id", "category"), "product_id") \
            .groupBy("customer_id", "category") \
            .agg(count("*").alias("category_orders")) \
            .withColumn("rank", row_number().over(
                Window.partitionBy("customer_id").orderBy(desc("category_orders"))
            )) \
            .filter(col("rank") == 1) \
            .select("customer_id", col("category").alias("favorite_category"))
        
        # Join all customer metrics
        customer_analytics = customers.alias("c") \
            .join(customer_orders.alias("co"), col("c.customer_id") == col("co.customer_id"), "left") \
            .join(customer_products.alias("cp"), col("c.customer_id") == col("cp.customer_id"), "left") \
            .join(customer_fav_category.alias("cf"), col("c.customer_id") == col("cf.customer_id"), "left")
        
        # Calculate derived metrics and segments
        customer_analytics = customer_analytics.select(
            col("c.customer_id"),
            col("c.full_name"),
            col("c.email"),
            col("c.country"),
            col("c.city"),
            col("c.customer_age_group"),
            col("c.customer_segment"),
            col("c.registration_date"),
            col("c.days_since_registration"),
            col("c.customer_lifetime_category"),
            
            # Order metrics
            coalesce(col("co.total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("co.total_spent"), lit(0.0)).alias("total_spent"),
            coalesce(col("co.avg_order_value"), lit(0.0)).alias("avg_order_value"),
            col("co.last_order_date"),
            col("co.first_order_date"),
            coalesce(col("co.total_tax_paid"), lit(0.0)).alias("total_tax_paid"),
            coalesce(col("co.total_shipping_paid"), lit(0.0)).alias("total_shipping_paid"),
            coalesce(col("co.active_years"), lit(0)).alias("active_years"),
            coalesce(col("co.active_months"), lit(0)).alias("active_months"),
            
            # Product metrics
            coalesce(col("cp.unique_products_bought"), lit(0)).alias("unique_products_bought"),
            coalesce(col("cp.categories_shopped"), lit(0)).alias("categories_shopped"),
            coalesce(col("cp.total_items_bought"), lit(0)).alias("total_items_bought"),
            col("cf.favorite_category"),
            
            # Calculated KPIs
            when(col("co.last_order_date").isNotNull(), 
                 datediff(current_date(), col("co.last_order_date")))
            .otherwise(lit(null)).alias("days_since_last_order"),
            
            when(col("co.first_order_date").isNotNull() & col("co.last_order_date").isNotNull(),
                 datediff(col("co.last_order_date"), col("co.first_order_date")))
            .otherwise(lit(0)).alias("customer_lifespan_days"),
            
            when(col("co.total_orders") > 0,
                 col("co.total_spent") / col("co.total_orders"))
            .otherwise(lit(0.0)).alias("calculated_aov"),
            
            # Customer value segments
            when(col("co.total_spent") >= 1000, "High Value")
            .when(col("co.total_spent") >= 500, "Medium Value")  
            .when(col("co.total_spent") >= 100, "Low Value")
            .when(col("co.total_spent") > 0, "Minimal Value")
            .otherwise("No Purchase").alias("value_segment"),
            
            # Recency segments
            when(col("co.last_order_date").isNull(), "Never Purchased")
            .when(datediff(current_date(), col("co.last_order_date")) <= 30, "Recent")
            .when(datediff(current_date(), col("co.last_order_date")) <= 90, "Active") 
            .when(datediff(current_date(), col("co.last_order_date")) <= 365, "At Risk")
            .otherwise("Churned").alias("recency_segment"),
            
            # Frequency segments
            when(col("co.total_orders") >= 10, "Frequent")
            .when(col("co.total_orders") >= 5, "Regular")
            .when(col("co.total_orders") >= 2, "Occasional")
            .when(col("co.total_orders") == 1, "One-time")
            .otherwise("No Purchase").alias("frequency_segment")
        )
        
        # Add gold layer metadata
        customer_analytics = customer_analytics \
            .withColumn("gold_created_timestamp", current_timestamp()) \
            .withColumn("gold_created_date", current_date()) \
            .withColumn("analytics_version", lit("1.0")) \
            .withColumn("last_updated", current_timestamp())
        
        record_count = customer_analytics.count()
        log_step("Gold Analytics - Customer", "SUCCESS", f"Created {record_count:,} customer analytics records")
        
        return customer_analytics
        
    except Exception as e:
        error_msg = f"Failed to create customer analytics: {str(e)}"
        log_step("Gold Analytics - Customer", "ERROR", error_msg)
        raise e