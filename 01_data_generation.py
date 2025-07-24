# Databricks notebook source
# MAGIC %md
# MAGIC # Sample E-commerce Data Generation
# MAGIC 
# MAGIC **Purpose**: Generate realistic sample e-commerce data for the analytics pipeline
# MAGIC **Author**: ajaykan47
# MAGIC **Date**: 2025-07-25
# MAGIC **Dependencies**: Faker library, pandas, Azure Data Lake Storage

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import uuid
from pyspark.sql.types import *
import numpy as np

# Initialize Faker
fake = Faker()
fake.seed_instance(42)  # For reproducible data
random.seed(42)
np.random.seed(42)

print("Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Functions

# COMMAND ----------

def generate_customers(n=1000):
    """
    Generate realistic customer data
    
    Args:
        n (int): Number of customers to generate
        
    Returns:
        pd.DataFrame: Customer data
    """
    log_step("Customer Generation", "STARTED", f"Generating {n} customers")
    
    customers = []
    countries = ['United States', 'Canada', 'United Kingdom', 'Germany', 'France', 
                'Australia', 'Japan', 'India', 'Brazil', 'Mexico']
    
    for i in range(n):
        # Generate realistic age distribution
        age = np.random.choice(
            range(18, 71), 
            p=[0.15, 0.25, 0.25, 0.20, 0.15]  # Weight towards middle ages
        )
        country = np.random.choice(countries, p=[0.3, 0.1, 0.1, 0.1, 0.08, 0.08, 0.06, 0.06, 0.06, 0.06])
        
        customer = {
            'customer_id': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'country': country,
            'city': fake.city(),
            'address': fake.address().replace('\n', ', '),
            'postal_code': fake.postcode(),
            'age': int(age),
            'gender': random.choice(['Male', 'Female', 'Other']),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic'])
        }
        customers.append(customer)
    
    customers_df = pd.DataFrame(customers)
    log_step("Customer Generation", "SUCCESS", f"Generated {len(customers_df)} customers")
    return customers_df

# COMMAND ----------

def generate_products(n=200):
    """
    Generate realistic product data
    
    Args:
        n (int): Number of products to generate
        
    Returns:
        pd.DataFrame: Product data
    """
    log_step("Product Generation", "STARTED", f"Generating {n} products")
    
    categories = {
        'Electronics': {'min_price': 50, 'max_price': 2000, 'min_cost': 25, 'max_cost': 1000},
        'Clothing': {'min_price': 15, 'max_price': 300, 'min_cost': 8, 'max_cost': 150},
        'Books': {'min_price': 10, 'max_price': 100, 'min_cost': 5, 'max_cost': 50},
        'Home & Garden': {'min_price': 20, 'max_price': 500, 'min_cost': 10, 'max_cost': 250},
        'Sports': {'min_price': 25, 'max_price': 800, 'min_cost': 12, 'max_cost': 400},
        'Beauty': {'min_price': 10, 'max_price': 200, 'min_cost': 5, 'max_cost': 100}
    }
    
    products = []
    
    for i in range(n):
        category = random.choice(list(categories.keys()))
        price_info = categories[category]
        
        # Generate cost first, then price with realistic margin
        cost = round(random.uniform(price_info['min_cost'], price_info['max_cost']), 2)
        margin_multiplier = random.uniform(1.5, 3.0)  # 50% to 200% margin
        price = round(cost * margin_multiplier, 2)
        
        # Ensure price doesn't exceed category max
        price = min(price, price_info['max_price'])
        
        product = {
            'product_id': str(uuid.uuid4()),
            'product_name': fake.catch_phrase(),
            'category': category,
            'sub_category': fake.word().title(),
            'brand': fake.company(),
            'price': price,
            'cost': cost,
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'dimensions': f"{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(2,30)} cm",
            'in_stock': random.choice([True, False]),
            'stock_quantity': random.randint(0, 1000),
            'supplier': fake.company(),
            'created_date': fake.date_between(start_date='-1y', end_date='today')
        }
        products.append(product)
    
    products_df = pd.DataFrame(products)
    log_step("Product Generation", "SUCCESS", f"Generated {len(products_df)} products")
    return products_df

# COMMAND ----------

def generate_orders(customers_df, products_df, n=5000):
    """
    Generate realistic order data with order items
    
    Args:
        customers_df (pd.DataFrame): Customer data
        products_df (pd.DataFrame): Product data
        n (int): Number of orders to generate
        
    Returns:
        tuple: (orders_df, order_items_df)
    """
    log_step("Order Generation", "STARTED", f"Generating {n} orders")
    
    orders = []
    order_items = []
    
    # Define seasonal patterns
    seasonal_weights = {
        1: 0.08, 2: 0.07, 3: 0.08, 4: 0.08, 5: 0.09, 6: 0.09,
        7: 0.08, 8: 0.08, 9: 0.08, 10: 0.09, 11: 0.12, 12: 0.14  # Higher in Nov/Dec
    }
    
    for i in range(n):
        # Select random customer with preference for active customers
        customer = customers_df.sample(1).iloc[0]
        
        # Generate order date with seasonal pattern
        month = np.random.choice(list(seasonal_weights.keys()), p=list(seasonal_weights.values()))
        year = random.choice([2023, 2024])
        day = random.randint(1, 28)
        order_date = datetime(year, month, day).date()
        
        # Order status with realistic distribution
        status_weights = [0.85, 0.10, 0.05]  # Most orders completed
        status = np.random.choice(['completed', 'pending', 'cancelled'], p=status_weights)
        
        # Number of items per order (realistic distribution)
        num_items = np.random.choice([1, 2, 3, 4, 5, 6], p=[0.4, 0.25, 0.15, 0.1, 0.05, 0.05])
        
        order = {
            'order_id': str(uuid.uuid4()),
            'customer_id': customer['customer_id'],
            'order_date': order_date,
            'status': status,
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
            'shipping_method': random.choice(['Standard', 'Express', 'Overnight']),
            'order_total': 0,  # Will calculate after items
            'tax_amount': 0,
            'shipping_cost': round(random.uniform(5, 25), 2) if status == 'completed' else 0
        }
        
        # Generate order items
        selected_products = products_df.sample(num_items, replace=True)
        order_total = 0
        
        for _, product in selected_products.iterrows():
            quantity = random.randint(1, 3)
            unit_price = product['price']
            line_total = quantity * unit_price
            order_total += line_total
            
            order_item = {
                'order_item_id': str(uuid.uuid4()),
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': line_total,
                'discount_amount': round(random.uniform(0, line_total * 0.1), 2) if random.random() < 0.2 else 0
            }
            order_items.append(order_item)
        
        # Update order totals
        order['order_total'] = round(order_total, 2)
        order['tax_amount'] = round(order_total * 0.08, 2)  # 8% tax
        
        orders.append(order)
    
    orders_df = pd.DataFrame(orders)
    order_items_df = pd.DataFrame(order_items)
    
    log_step("Order Generation", "SUCCESS", f"Generated {len(orders_df)} orders with {len(order_items_df)} items")
    return orders_df, order_items_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate All Sample Data

# COMMAND ----------

print("Starting data generation process...")
print("=" * 50)

# Generate customers
customers_df = generate_customers(1000)
print(f"✅ Customers: {len(customers_df)} records")

# Generate products  
products_df = generate_products(200)
print(f"✅ Products: {len(products_df)} records")

# Generate orders and order items
orders_df, order_items_df = generate_orders(customers_df, products_df, 5000)
print(f"✅ Orders: {len(orders_df)} records")
print(f"✅ Order Items: {len(order_items_df)} records")

print("=" * 50)
print("Data generation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def perform_data_quality_checks():
    """Perform basic data quality checks on generated data"""
    log_step("Data Quality Check", "STARTED", "Performing quality checks")
    
    issues = []
    
    # Check customers
    if customers_df['customer_id'].duplicated().any():
        issues.append("Duplicate customer IDs found")
    if customers_df['email'].duplicated().any():
        issues.append("Duplicate customer emails found")
    if customers_df.isnull().sum().sum() > 0:
        issues.append("Null values found in customers data")
    
    # Check products
    if products_df['product_id'].duplicated().any():
        issues.append("Duplicate product IDs found")
    if (products_df['price'] <= products_df['cost']).any():
        issues.append("Products with price <= cost found")
    
    # Check orders
    if orders_df['order_id'].duplicated().any():
        issues.append("Duplicate order IDs found")
    
    # Check order items
    if order_items_df['order_item_id'].duplicated().any():
        issues.append("Duplicate order item IDs found")
    
    if issues:
        log_step("Data Quality Check", "WARNING", f"Issues found: {', '.join(issues)}")
        for issue in issues:
            print(f"⚠️ {issue}")
    else:
        log_step("Data Quality Check", "SUCCESS", "No data quality issues found")
        print("✅ All data quality checks passed")

perform_data_quality_checks()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------

print("=== Sample Data Preview ===")
print("\nCustomers (first 5 rows):")
print(customers_df.head())

print("\nProducts (first 5 rows):")
print(products_df.head())

print("\nOrders (first 5 rows):")
print(orders_df.head())

print("\nOrder Items (first 5 rows):")
print(order_items_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Data to Azure Data Lake

# COMMAND ----------

def save_to_azure_storage():
    """Save generated data to Azure Data Lake Storage"""
    log_step("Data Saving", "STARTED", "Saving data to Azure Data Lake")
    
    try:
        storage_path = get_storage_path("raw")
        
        # Convert pandas DataFrames to Spark DataFrames
        customers_spark_df = spark.createDataFrame(customers_df)
        products_spark_df = spark.createDataFrame(products_df)
        orders_spark_df = spark.createDataFrame(orders_df)
        order_items_spark_df = spark.createDataFrame(order_items_df)
        
        # Save as Delta format
        customers_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/customers")
        log_step("Customer Data", "SAVED", f"{len(customers_df)} records")
        
        products_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/products")
        log_step("Product Data", "SAVED", f"{len(products_df)} records")
        
        orders_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/orders")
        log_step("Order Data", "SAVED", f"{len(orders_df)} records")
        
        order_items_spark_df.write.format("delta").mode("overwrite").save(f"{storage_path}/order_items")
        log_step("Order Items Data", "SAVED", f"{len(order_items_df)} records")
        
        log_step("Data Saving", "SUCCESS", "All data saved successfully")
        
        # Create monitoring entry
        create_monitoring_entry(
            "Data Generation Complete", 
            "SUCCESS", 
            len(customers_df) + len(products_df) + len(orders_df) + len(order_items_df)
        )
        
        return True
        
    except Exception as e:
        error_msg = f"Failed to save data: {str(e)}"
        log_step("Data Saving", "ERROR", error_msg)
        create_monitoring_entry("Data Generation", "ERROR", error_message=error_msg)
        return False

# Save the data
if save_to_azure_storage():
    print("🎉 Data generation and saving completed successfully!")
    print("\nNext Steps:")
    print("1. Run 02_bronze_ingestion.py to ingest data to bronze layer")
    print("2. Continue with the data pipeline")
else:
    print("❌ Failed to save data. Please check the error messages above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=== Final Data Summary ===")
print(f"📊 Dataset Statistics:")
print(f"   • Customers: {len(customers_df):,} records")
print(f"   • Products: {len(products_df):,} records") 
print(f"   • Orders: {len(orders_df):,} records")
print(f"   • Order Items: {len(order_items_df):,} records")
print(f"   • Total Records: {len(customers_df) + len(products_df) + len(orders_df) + len(order_items_df):,}")

print(f"\n💰 Business Metrics:")
total_revenue = orders_df[orders_df['status'] == 'completed']['order_total'].sum()
avg_order_value = orders_df[orders_df['status'] == 'completed']['order_total'].mean()
completion_rate = (orders_df['status'] == 'completed').mean() * 100

print(f"   • Total Revenue: ${total_revenue:,.2f}")
print(f"   • Average Order Value: ${avg_order_value:.2f}")
print(f"   • Order Completion Rate: {completion_rate:.1f}%")

print(f"\n🌍 Geographic Distribution:")
country_counts = customers_df['country'].value_counts().head()
for country, count in country_counts.items():
    print(f"   • {country}: {count} customers")

print(f"\n📈 Date Range:")
print(f"   • Customer Registration: {customers_df['registration_date'].min()} to {customers_df['registration_date'].max()}")
print(f"   • Order Dates: {orders_df['order_date'].min()} to {orders_df['order_date'].max()}")