# Complete Beginner's Guide: Real-World Databricks + Azure Project

## 🎯 Project Overview: E-commerce Sales Analytics Pipeline

We'll build a complete data pipeline that:
- Ingests e-commerce sales data from multiple sources
- Processes and cleans the data
- Creates analytics dashboards
- Implements basic machine learning for sales prediction

**Time to Complete**: 2-3 weeks (working part-time)
**Difficulty**: Beginner to Intermediate

---

## 📋 Prerequisites & Setup

### What You Need
- Azure subscription (free tier works!)
- Basic SQL knowledge
- Basic Python knowledge (we'll guide you through)
- Willingness to learn!

### Cost Estimate
- **Free Tier**: $0-50/month for learning
- **Small Project**: $100-200/month
- **Production**: $500+/month

---

## 🚀 Phase 1: Azure Environment Setup (Week 1, Days 1-2)

### Step 1: Create Azure Account
1. Go to [portal.azure.com](https://portal.azure.com)
2. Sign up for free account ($200 credit)
3. Verify your account with phone/credit card

### Step 2: Create Resource Group
```bash
# In Azure Cloud Shell or Azure CLI
az group create --name "ecommerce-analytics-rg" --location "East US"
```

### Step 3: Set Up Azure Data Lake Storage Gen2
1. **Create Storage Account**:
   - Name: `ecommercedata[yourname]`
   - Performance: Standard
   - Redundancy: LRS (cheapest for learning)
   - Enable hierarchical namespace ✅

2. **Create Containers**:
   - `raw-data` (for incoming data)
   - `processed-data` (for cleaned data)
   - `analytics-data` (for final datasets)

### Step 4: Create Azure Databricks Workspace
1. **In Azure Portal**:
   - Search "Azure Databricks"
   - Create new workspace
   - Name: `ecommerce-analytics-workspace`
   - Pricing: Standard (for learning)
   - Location: Same as storage account

2. **Launch Workspace**:
   - Click "Launch Workspace"
   - This opens Databricks interface

---

## 🔧 Phase 2: Databricks Setup (Week 1, Days 3-4)

### Step 1: Create Your First Cluster
```python
# Cluster Configuration (in Databricks UI)
Cluster Name: "learning-cluster"
Databricks Runtime: 11.3 LTS (or latest LTS)
Node Type: Standard_DS3_v2 (good for learning)
Workers: 2-3 (minimum for learning)
Auto-scaling: Enable
Auto-termination: 60 minutes (saves money!)
```

### Step 2: Connect to Azure Storage
```python
# In Databricks notebook
# Set up Azure Data Lake connection
storage_account_name = "ecommercedata[yourname]"
storage_account_key = "your-storage-key"  # Get from Azure portal

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Test connection
dbutils.fs.ls(f"abfss://raw-data@{storage_account_name}.dfs.core.windows.net/")
```

### Step 3: Install Required Libraries
```python
# Install on cluster
%pip install faker pandas matplotlib seaborn plotly
```

---

## 📊 Phase 3: Generate Sample Data (Week 1, Days 5-7)

### Step 1: Create Sample E-commerce Data
```python
# Databricks Notebook: Data Generation
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

fake = Faker()

def generate_customers(n=1000):
    customers = []
    for i in range(n):
        customers.append({
            'customer_id': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'country': fake.country(),
            'city': fake.city(),
            'age': random.randint(18, 70)
        })
    return pd.DataFrame(customers)

def generate_products(n=200):
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty']
    products = []
    for i in range(n):
        products.append({
            'product_id': str(uuid.uuid4()),
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(10, 500), 2),
            'cost': round(random.uniform(5, 250), 2)
        })
    return pd.DataFrame(products)

def generate_orders(customers_df, products_df, n=5000):
    orders = []
    for i in range(n):
        customer = customers_df.sample(1).iloc[0]
        num_items = random.randint(1, 5)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        
        order = {
            'order_id': str(uuid.uuid4()),
            'customer_id': customer['customer_id'],
            'order_date': order_date,
            'status': random.choice(['completed', 'pending', 'cancelled'])
        }
        orders.append(order)
    return pd.DataFrame(orders)

# Generate data
print("Generating sample data...")
customers_df = generate_customers(1000)
products_df = generate_products(200)
orders_df = generate_orders(customers_df, products_df, 5000)

print(f"Generated {len(customers_df)} customers")
print(f"Generated {len(products_df)} products") 
print(f"Generated {len(orders_df)} orders")
```

### Step 2: Save Data to Azure Data Lake
```python
# Save to Azure Data Lake Storage
storage_path = f"abfss://raw-data@{storage_account_name}.dfs.core.windows.net"

# Convert to Spark DataFrames and save as Delta tables
customers_spark_df = spark.createDataFrame(customers_df)
products_spark_df = spark.createDataFrame(products_df)
orders_spark_df = spark.createDataFrame(orders_df)

# Save as Delta format (better than Parquet for analytics)
customers_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/customers")
products_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/products")
orders_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/orders")

print("Data saved successfully!")
```

---

## 🔄 Phase 4: Build ETL Pipeline (Week 2, Days 1-4)

### Step 1: Create Bronze Layer (Raw Data Ingestion)
```python
# Notebook: 01_Bronze_Data_Ingestion
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read raw data
bronze_path = f"abfss://processed-data@{storage_account_name}.dfs.core.windows.net/bronze"

def ingest_to_bronze(source_path, target_path, table_name):
    # Read source data
    df = spark.read.format("delta").load(source_path)
    
    # Add metadata columns
    df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp()) \
                        .withColumn("source_file", lit(table_name))
    
    # Write to bronze layer
    df_with_metadata.write.format("delta").mode("overwrite").save(target_path)
    print(f"Ingested {table_name} to bronze layer")

# Ingest all tables
ingest_to_bronze(f"{storage_path}/customers", f"{bronze_path}/customers", "customers")
ingest_to_bronze(f"{storage_path}/products", f"{bronze_path}/products", "products")
ingest_to_bronze(f"{storage_path}/orders", f"{bronze_path}/orders", "orders")
```

### Step 2: Create Silver Layer (Data Cleaning)
```python
# Notebook: 02_Silver_Data_Cleaning
silver_path = f"abfss://processed-data@{storage_account_name}.dfs.core.windows.net/silver"

def clean_customers():
    # Read bronze data
    df = spark.read.format("delta").load(f"{bronze_path}/customers")
    
    # Data cleaning operations
    cleaned_df = df.filter(col("email").isNotNull()) \
                   .filter(col("age").between(18, 100)) \
                   .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
                   .withColumn("customer_age_group", 
                              when(col("age") < 25, "18-24")
                              .when(col("age") < 35, "25-34")
                              .when(col("age") < 45, "35-44")
                              .when(col("age") < 55, "45-54")
                              .otherwise("55+"))
    
    # Save to silver layer
    cleaned_df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
    print("Customers data cleaned and saved to silver layer")

def clean_products():
    df = spark.read.format("delta").load(f"{bronze_path}/products")
    
    cleaned_df = df.filter(col("price") > 0) \
                   .filter(col("cost") > 0) \
                   .withColumn("profit_margin", 
                              round(((col("price") - col("cost")) / col("price")) * 100, 2))
    
    cleaned_df.write.format("delta").mode("overwrite").save(f"{silver_path}/products")
    print("Products data cleaned and saved to silver layer")

def clean_orders():
    df = spark.read.format("delta").load(f"{bronze_path}/orders")
    
    cleaned_df = df.filter(col("status").isin(["completed", "pending", "cancelled"])) \
                   .withColumn("order_year", year(col("order_date"))) \
                   .withColumn("order_month", month(col("order_date"))) \
                   .withColumn("order_quarter", quarter(col("order_date")))
    
    cleaned_df.write.format("delta").mode("overwrite").save(f"{silver_path}/orders")
    print("Orders data cleaned and saved to silver layer")

# Execute cleaning functions
clean_customers()
clean_products()
clean_orders()
```

### Step 3: Create Gold Layer (Business Logic)
```python
# Notebook: 03_Gold_Analytics_Ready
gold_path = f"abfss://analytics-data@{storage_account_name}.dfs.core.windows.net/gold"

def create_customer_analytics():
    # Read silver layer data
    customers = spark.read.format("delta").load(f"{silver_path}/customers")
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    
    # Create customer analytics
    customer_analytics = customers.alias("c") \
        .join(orders.alias("o"), col("c.customer_id") == col("o.customer_id"), "left") \
        .groupBy("c.customer_id", "c.full_name", "c.country", "c.customer_age_group") \
        .agg(
            count("o.order_id").alias("total_orders"),
            countDistinct("o.order_id").alias("unique_orders"),
            max("o.order_date").alias("last_order_date"),
            min("o.order_date").alias("first_order_date")
        )
    
    customer_analytics.write.format("delta").mode("overwrite").save(f"{gold_path}/customer_analytics")
    print("Customer analytics created")

def create_sales_summary():
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    
    # Monthly sales summary
    monthly_sales = orders.filter(col("status") == "completed") \
        .groupBy("order_year", "order_month") \
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("order_year", "order_month")
    
    monthly_sales.write.format("delta").mode("overwrite").save(f"{gold_path}/monthly_sales")
    print("Sales summary created")

# Create gold layer tables
create_customer_analytics()
create_sales_summary()
```

---

## 📈 Phase 5: Analytics & Visualization (Week 2, Days 5-7)

### Step 1: Create Analysis Notebook
```python
# Notebook: 04_Data_Analysis
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go

# Read gold layer data
customer_analytics = spark.read.format("delta").load(f"{gold_path}/customer_analytics").toPandas()
monthly_sales = spark.read.format("delta").load(f"{gold_path}/monthly_sales").toPandas()

# Analysis 1: Customer Distribution by Age Group
plt.figure(figsize=(10, 6))
customer_analytics['customer_age_group'].value_counts().plot(kind='bar')
plt.title('Customer Distribution by Age Group')
plt.xlabel('Age Group')
plt.ylabel('Number of Customers')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Analysis 2: Monthly Sales Trend
fig = px.line(monthly_sales, x='order_month', y='total_orders', 
              title='Monthly Sales Trend')
fig.show()

# Analysis 3: Top Countries by Customer Count
top_countries = customer_analytics['country'].value_counts().head(10)
fig = px.bar(x=top_countries.index, y=top_countries.values,
             title='Top 10 Countries by Customer Count')
fig.update_xaxes(title='Country')
fig.update_yaxes(title='Number of Customers')
fig.show()
```

### Step 2: Create Dashboard Tables
```sql
-- Create SQL tables for easier querying
CREATE TABLE customer_analytics
USING DELTA
LOCATION 'abfss://analytics-data@ecommercedatayourname.dfs.core.windows.net/gold/customer_analytics';

CREATE TABLE monthly_sales
USING DELTA  
LOCATION 'abfss://analytics-data@ecommercedatayourname.dfs.core.windows.net/gold/monthly_sales';

-- Sample queries
SELECT customer_age_group, COUNT(*) as customer_count
FROM customer_analytics
GROUP BY customer_age_group
ORDER BY customer_count DESC;

SELECT order_year, order_month, total_orders
FROM monthly_sales
ORDER BY order_year DESC, order_month DESC
LIMIT 12;
```

---

## 🤖 Phase 6: Basic Machine Learning (Week 3, Days 1-3)

### Step 1: Simple Prediction Model
```python
# Notebook: 05_Basic_ML
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Prepare data for ML
monthly_sales_spark = spark.read.format("delta").load(f"{gold_path}/monthly_sales")

# Feature engineering
ml_data = monthly_sales_spark.withColumn("month_numeric", 
                                        col("order_year") * 12 + col("order_month"))

# Prepare features
assembler = VectorAssembler(inputCols=["month_numeric"], outputCols="features")
ml_data_assembled = assembler.transform(ml_data)

# Split data
train_data, test_data = ml_data_assembled.randomSplit([0.8, 0.2], seed=42)

# Train model
lr = LinearRegression(featuresCol="features", labelCol="total_orders")
model = lr.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate model
evaluator = RegressionEvaluator(labelCol="total_orders", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Square Error: {rmse}")

# Show predictions
predictions.select("order_year", "order_month", "total_orders", "prediction").show()
```

---

## 🔄 Phase 7: Automation & Scheduling (Week 3, Days 4-5)

### Step 1: Create Databricks Jobs
1. **In Databricks UI**:
   - Go to "Workflows" → "Jobs"
   - Click "Create Job"
   - Name: "Daily ETL Pipeline"

2. **Configure Tasks**:
   - Task 1: Bronze Data Ingestion
   - Task 2: Silver Data Cleaning  
   - Task 3: Gold Analytics Creation
   - Task 4: ML Model Training

3. **Set Schedule**:
   - Trigger: Scheduled
   - Schedule: Daily at 2 AM
   - Time zone: Your local time zone

### Step 2: Add Monitoring
```python
# Add to each notebook for monitoring
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_pipeline_step(step_name, status, record_count=None):
    message = f"Pipeline Step: {step_name} - Status: {status}"
    if record_count:
        message += f" - Records: {record_count}"
    logger.info(message)
    
    # You can also write to a monitoring table
    monitoring_data = [(step_name, status, record_count, current_timestamp())]
    monitoring_df = spark.createDataFrame(monitoring_data, 
                                         ["step_name", "status", "record_count", "timestamp"])
    
    monitoring_df.write.format("delta").mode("append").save(f"{gold_path}/pipeline_monitoring")

# Use in your pipeline
log_pipeline_step("Bronze Ingestion", "SUCCESS", customers_df.count())
```

---

## 🎯 Phase 8: Testing & Documentation (Week 3, Days 6-7)

### Step 1: Data Quality Tests
```python
# Notebook: 06_Data_Quality_Tests
def test_data_quality():
    # Test 1: Check for null values in critical fields
    customers = spark.read.format("delta").load(f"{silver_path}/customers")
    null_emails = customers.filter(col("email").isNull()).count()
    assert null_emails == 0, f"Found {null_emails} customers with null emails"
    
    # Test 2: Check data ranges
    invalid_ages = customers.filter((col("age") < 18) | (col("age") > 100)).count()
    assert invalid_ages == 0, f"Found {invalid_ages} customers with invalid ages"
    
    # Test 3: Check for duplicate records
    total_customers = customers.count()
    unique_customers = customers.select("customer_id").distinct().count()
    assert total_customers == unique_customers, "Found duplicate customer records"
    
    print("All data quality tests passed!")

test_data_quality()
```

### Step 2: Create Documentation
```markdown
# E-commerce Analytics Project Documentation

## Project Overview
This project demonstrates a complete data pipeline using Azure Databricks and Data Lake Storage.

## Architecture
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Data cleaning and validation  
- **Gold Layer**: Business-ready analytics tables

## Data Flow
1. Sample data generation
2. Bronze layer ingestion
3. Silver layer cleaning
4. Gold layer analytics
5. ML model training
6. Dashboard creation

## Key Metrics
- Customer analytics by age group and geography
- Monthly sales trends
- Basic sales forecasting

## Maintenance
- Pipeline runs daily at 2 AM
- Data quality tests included
- Monitoring and alerting configured
```

---

## 🚀 Next Steps & Best Practices

### Production Readiness Checklist
- [ ] Implement proper error handling
- [ ] Add comprehensive logging
- [ ] Set up alerts and monitoring  
- [ ] Implement data backup strategy
- [ ] Add security and access controls
- [ ] Optimize for cost (auto-scaling, spot instances)
- [ ] Create disaster recovery plan

### Learning Path Recommendations
1. **Complete Azure Databricks Certification**
2. **Learn advanced Spark optimization**
3. **Explore MLOps with MLflow**
4. **Study real-time streaming with Event Hubs**
5. **Practice with larger datasets**

### Common Beginner Mistakes to Avoid
- Not using Delta format (stick with Delta!)
- Forgetting to terminate clusters (expensive!)
- Not implementing proper error handling
- Ignoring data quality validation
- Not documenting your work

---

## 💰 Cost Optimization Tips

### For Learning (Keep costs under $50/month)
- Use auto-terminating clusters (60 minutes)
- Choose smaller node types (Standard_DS3_v2)
- Use minimal workers (2-3 nodes)
- Delete unused resources regularly
- Use Azure free tier credits

### Monitoring Costs
```python
# Check your cluster costs regularly
cluster_usage = dbutils.notebook.run("/path/to/cost-monitoring-notebook", 0)
```

Remember: **Start small, learn the concepts, then scale up!** This project gives you real hands-on experience with all the key components of modern data engineering.