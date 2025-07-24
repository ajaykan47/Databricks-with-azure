# Databricks notebook source
# MAGIC %md
# MAGIC # Data Analysis and Visualization
# MAGIC 
# MAGIC **Purpose**: Create comprehensive data analysis and visualizations from gold layer
# MAGIC **Author**: ajaykan47
# MAGIC **Date**: 2025-07-2025
# MAGIC **Dependencies**: Gold layer analytics tables, visualization libraries

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")

print("Visualization libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Functions

# COMMAND ----------

def load_analytics_data():
    """Load all gold layer analytics tables into pandas DataFrames for visualization"""
    log_step("Data Loading", "STARTED", "Loading analytics data for visualization")
    
    try:
        # Load all analytics tables
        analytics_data = {}
        
        table_names = ["customer_analytics", "product_analytics", "monthly_sales", 
                      "daily_sales", "category_sales", "cohort_analysis"]
        
        for table_name in table_names:
            try:
                df = spark.table(f"gold_{table_name}").toPandas()
                analytics_data[table_name] = df
                print(f"✅ Loaded {table_name}: {len(df):,} records")
            except Exception as e:
                print(f"⚠️ Failed to load {table_name}: {str(e)}")
                analytics_data[table_name] = pd.DataFrame()
        
        log_step("Data Loading", "SUCCESS", f"Loaded {len([k for k, v in analytics_data.items() if not v.empty])} tables")
        return analytics_data
        
    except Exception as e:
        error_msg = f"Failed to load analytics data: {str(e)}"
        log_step("Data Loading", "ERROR", error_msg)
        raise e

# Load data
analytics_data = load_analytics_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Analytics Visualizations

# COMMAND ----------

def create_customer_visualizations():
    """Create comprehensive customer analytics visualizations"""
    log_step("Customer Visualizations", "STARTED", "Creating customer analytics charts")
    
    try:
        customer_df = analytics_data["customer_analytics"]
        
        if customer_df.empty:
            print("⚠️ No customer data available for visualization")
            return
        
        # 1. Customer Distribution by Age Group
        plt.figure(figsize=(12, 8))
        
        plt.subplot(2, 2, 1)
        age_counts = customer_df['customer_age_group'].value_counts()
        plt.pie(age_counts.values, labels=age_counts.index, autopct='%1.1f%%', startangle=90)
        plt.title('Customer Distribution by Age Group')
        
        # 2. Customer Value Segments
        plt.subplot(2, 2, 2)
        value_counts = customer_df['value_segment'].value_counts()
        bars = plt.bar(value_counts.index, value_counts.values, color=['gold', 'silver', 'bronze', 'lightgray', 'red'])
        plt.title('Customers by Value Segment')
        plt.xlabel('Value Segment')
        plt.ylabel('Number of Customers')
        plt.xticks(rotation=45)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}', ha='center', va='bottom')
        
        # 3. Customer Recency Distribution
        plt.subplot(2, 2, 3)
        recency_counts = customer_df['recency_segment'].value_counts()
        colors = ['green', 'blue', 'orange', 'red', 'gray']
        plt.bar(recency_counts.index, recency_counts.values, color=colors[:len(recency_counts)])
        plt.title('Customer Recency Segments')
        plt.xlabel('Recency Segment')
        plt.ylabel('Number of Customers')
        plt.xticks(rotation=45)
        
        # 4. Top Countries by Customer Count
        plt.subplot(2, 2, 4)
        top_countries = customer_df['country'].value_counts().head(10)
        plt.barh(range(len(top_countries)), top_countries.values)
        plt.yticks(range(len(top_countries)), top_countries.index)
        plt.title('Top 10 Countries by Customer Count')
        plt.xlabel('Number of Customers')
        
        plt.tight_layout()
        plt.show()
        
        # Interactive Plotly visualizations
        print("\n📊 Interactive Customer Analytics:")
        
        # Customer Value vs Recency Scatter
        fig = px.scatter(customer_df, 
                        x='total_spent', 
                        y='total_orders',
                        color='value_segment',
                        size='days_since_last_order',
                        hover_data=['full_name', 'country', 'recency_segment'],
                        title='Customer Value Analysis: Spending vs Order Frequency',
                        labels={'total_spent': 'Total Spent ($)', 'total_orders': 'Total Orders'})
        fig.show()
        
        # Geographic distribution
        country_summary = customer_df.groupby('country').agg({
            'customer_id': 'count',
            'total_spent': 'sum',
            'avg_order_value': 'mean'
        }).reset_index()
        country_summary.columns = ['country', 'customer_count', 'total_revenue', 'avg_order_value']
        
        fig = px.bar(country_summary.head(15), 
                    x='country', 
                    y='customer_count',
                    color='total_revenue',
                    title='Customer Distribution by Country',
                    labels={'customer_count': 'Number of Customers', 'total_revenue': 'Total Revenue'})
        fig.update_xaxes(tickangle=45)
        fig.show()
        
        log_step("Customer Visualizations", "SUCCESS", "Customer charts created")
        
    except Exception as e:
        error_msg = f"Failed to create customer visualizations: {str(e)}"
        log_step("Customer Visualizations", "ERROR", error_msg)
        print(f"❌ {error_msg}")

create_customer_visualizations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Analytics Visualizations

# COMMAND ----------

def create_product_visualizations():
    """Create comprehensive product analytics visualizations"""
    log_step("Product Visualizations", "STARTED", "Creating product analytics charts")
    
    try:
        product_df = analytics_data["product_analytics"]
        
        if product_df.empty:
            print("⚠️ No product data available for visualization")
            return
        
        # 1. Product Performance Analysis
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Sales performance distribution
        sales_perf_counts = product_df['sales_performance'].value_counts()
        axes[0, 0].pie(sales_perf_counts.values, labels=sales_perf_counts.index, autopct='%1.1f%%')
        axes[0, 0].set_title('Product Sales Performance Distribution')
        
        # Revenue by category
        category_revenue = product_df.groupby('category')['total_revenue'].sum().sort_values(ascending=True)
        axes[0, 1].barh(category_revenue.index, category_revenue.values)
        axes[0, 1].set_title('Total Revenue by Category')
        axes[0, 1].set_xlabel('Total Revenue ($)')
        
        # Price vs Revenue scatter
        axes[1, 0].scatter(product_df['price'], product_df['total_revenue'], 
                          c=product_df['profit_margin_pct'], cmap='viridis', alpha=0.6)
        axes[1, 0].set_xlabel('Product Price ($)')
        axes[1, 0].set_ylabel('Total Revenue ($)')
        axes[1, 0].set_title('Price vs Revenue (colored by Profit Margin %)')
        
        # Top products by quantity sold
        top_products = product_df.nlargest(10, 'total_quantity_sold')
        axes[1, 1].bar(range(len(top_products)), top_products['total_quantity_sold'])
        axes[1, 1].set_xticks(range(len(top_products)))
        axes[1, 1].set_xticklabels(top_products['product_name'], rotation=45, ha='right')
        axes[1, 1].set_title('Top 10 Products by Quantity Sold')
        axes[1, 1].set_ylabel('Quantity Sold')
        
        plt.tight_layout()
        plt.show()
        
        # Interactive Plotly visualizations
        print("\n📊 Interactive Product Analytics:")
        
        # Product performance bubble chart
        fig = px.scatter(product_df, 
                        x='total_quantity_sold', 
                        y='total_revenue',
                        size='profit_margin_pct',
                        color='category',
                        hover_data=['product_name', 'price', 'sales_performance'],
                        title='Product Performance: Quantity vs Revenue (bubble size = profit margin)',
                        labels={'total_quantity_sold': 'Total Quantity Sold', 'total_revenue': 'Total Revenue ($)'})
        fig.show()
        
        # Category performance comparison
        category_stats = product_df.groupby('category').agg({
            'total_revenue': 'sum',
            'total_quantity_sold': 'sum',
            'product_id': 'count',
            'profit_margin_pct': 'mean'
        }).reset_index()
        category_stats.columns = ['category', 'total_revenue', 'total_quantity', 'product_count', 'avg_profit_margin']
        
        fig = make_subplots(rows=2, cols=2,
                           subplot_titles=('Revenue by Category', 'Quantity by Category', 
                                         'Product Count by Category', 'Avg Profit Margin by Category'))
        
        fig.add_trace(go.Bar(x=category_stats['category'], y=category_stats['total_revenue'], name='Revenue'), row=1, col=1)
        fig.add_trace(go.Bar(x=category_stats['category'], y=category_stats['total_quantity'], name='Quantity'), row=1, col=2)
        fig.add_trace(go.Bar(x=category_stats['category'], y=category_stats['product_count'], name='Products'), row=2, col=1)
        fig.add_trace(go.Bar(x=category_stats['category'], y=category_stats['avg_profit_margin'], name='Avg Margin %'), row=2, col=2)
        
        fig.update_layout(height=800, showlegend=False, title_text="Category Performance Dashboard")
        fig.show()
        
        log_step("Product Visualizations", "SUCCESS", "Product charts created")
        
    except Exception as e:
        error_msg = f"Failed to create product visualizations: {str(e)}"
        log_step("Product Visualizations", "ERROR", error_msg)
        print(f"❌ {error_msg}")

create_product_visualizations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Trend Analysis

# COMMAND ----------

def create_sales_trend_analysis():
    """Create comprehensive sales trend analysis"""
    log_step("Sales Trend Analysis", "STARTED", "Creating sales trend visualizations")
    
    try:
        monthly_df = analytics_data["monthly_sales"]
        daily_df = analytics_data["daily_sales"]
        category_df = analytics_data["category_sales"]
        
        if monthly_df.empty:
            print("⚠️ No sales data available for visualization")
            return
        
        # 1. Monthly Sales Trends
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Monthly revenue trend
        monthly_df_sorted = monthly_df.sort_values(['order_year', 'order_month'])
        axes[0, 0].plot(range(len(monthly_df_sorted)), monthly_df_sorted['total_revenue'], marker='o', linewidth=2)
        axes[0, 0].set_title('Monthly Revenue Trend')
        axes[0, 0].set_xlabel('Month')
        axes[0, 0].set_ylabel('Total Revenue ($)')
        axes[0, 0].grid(True, alpha=0.3)
        
        # Monthly order count trend
        axes[0, 1].plot(range(len(monthly_df_sorted)), monthly_df_sorted['total_orders'], marker='s', color='orange', linewidth=2)
        axes[0, 1].set_title('Monthly Order Count Trend')
        axes[0, 1].set_xlabel('Month')
        axes[0, 1].set_ylabel('Total Orders')
        axes[0, 1].grid(True, alpha=0.3)
        
        # Average order value trend
        axes[1, 0].plot(range(len(monthly_df_sorted)), monthly_df_sorted['avg_order_value'], marker='^', color='green', linewidth=2)
        axes[1, 0].set_title('Average Order Value Trend')
        axes[1, 0].set_xlabel('Month')
        axes[1, 0].set_ylabel('Average Order Value ($)')
        axes[1, 0].grid(True, alpha=0.3)
        
        # Customer acquisition trend
        axes[1, 1].plot(range(len(monthly_df_sorted)), monthly_df_sorted['unique_customers'], marker='d', color='red', linewidth=2)
        axes[1, 1].set_title('Monthly Unique Customers Trend')
        axes[1, 1].set_xlabel('Month')
        axes[1, 1].set_ylabel('Unique Customers')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        
        # 2. Daily Sales Analysis (if available)
        if not daily_df.empty:
            print("\n📅 Daily Sales Analysis:")
            
            # Convert order_date to datetime
            daily_df['order_date'] = pd.to_datetime(daily_df['order_date'])
            daily_df_sorted = daily_df.sort_values('order_date')
            
            # Daily revenue trend
            fig = px.line(daily_df_sorted, 
                         x='order_date', 
                         y='total_revenue',
                         title='Daily Revenue Trend',
                         labels={'order_date': 'Date', 'total_revenue': 'Total Revenue ($)'})
            fig.show()
            
            # Weekday vs Weekend performance
            weekday_performance = daily_df.groupby('weekday_performance').agg({
                'total_revenue': 'mean',
                'total_orders': 'mean',
                'unique_customers': 'mean'
            }).reset_index()
            
            fig = px.bar(weekday_performance, 
                        x='weekday_performance', 
                        y='total_revenue',
                        title='Average Daily Revenue: Weekday vs Weekend',
                        labels={'total_revenue': 'Average Revenue ($)'})
            fig.show()
        
        # 3. Interactive monthly trends
        print("\n📈 Interactive Monthly Trends:")
        
        fig = make_subplots(rows=2, cols=2,
                           subplot_titles=('Monthly Revenue', 'Monthly Orders', 
                                         'Average Order Value', 'Unique Customers'),
                           specs=[[{"secondary_y": False}, {"secondary_y": False}],
                                 [{"secondary_y": False}, {"secondary_y": False}]])
        
        fig.add_trace(go.Scatter(x=monthly_df_sorted['year_month'], y=monthly_df_sorted['total_revenue'],
                                mode='lines+markers', name='Revenue'), row=1, col=1)
        fig.add_trace(go.Scatter(x=monthly_df_sorted['year_month'], y=monthly_df_sorted['total_orders'],
                                mode='lines+markers', name='Orders'), row=1, col=2)
        fig.add_trace(go.Scatter(x=monthly_df_sorted['year_month'], y=monthly_df_sorted['avg_order_value'],
                                mode='lines+markers', name='AOV'), row=2, col=1)
        fig.add_trace(go.Scatter(x=monthly_df_sorted['year_month'], y=monthly_df_sorted['unique_customers'],
                                mode='lines+markers', name='Customers'), row=2, col=2)
        
        fig.update_layout(height=800, showlegend=False, title_text="Monthly Sales Performance Dashboard")
        fig.update_xaxes(tickangle=45)
        fig.show()
        
        log_step("Sales Trend Analysis", "SUCCESS", "Sales trend charts created")
        
    except Exception as e:
        error_msg = f"Failed to create sales trend analysis: {str(e)}"
        log_step("Sales Trend Analysis", "ERROR", error_msg)
        print(f"❌ {error_msg}")

create_sales_trend_analysis()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort Analysis Visualization

# COMMAND ----------

def create_cohort_analysis_visualization():
    """Create cohort retention analysis visualization"""
    log_step("Cohort Visualization", "STARTED", "Creating cohort analysis charts")
    
    try:
        cohort_df = analytics_data["cohort_analysis"]
        
        if cohort_df.empty:
            print("⚠️ No cohort data available for visualization")
            return
        
        # Create cohort retention heatmap
        cohort_pivot = cohort_df.pivot(index='cohort_month', 
                                      columns='months_since_first_purchase', 
                                      values='retention_rate')
        
        plt.figure(figsize=(15, 8))
        sns.heatmap(cohort_pivot, 
                   annot=True, 
                   fmt='.1f', 
                   cmap='YlOrRd',
                   cbar_kws={'label': 'Retention Rate (%)'})
        plt.title('Customer Cohort Retention Analysis')
        plt.xlabel('Months Since First Purchase')
        plt.ylabel('Cohort Month')
        plt.tight_layout()
        plt.show()
        
        # Interactive cohort analysis
        print("\n🔄 Interactive Cohort Analysis:")
        
        fig = px.imshow(cohort_pivot,
                       labels=dict(x="Months Since First Purchase", y="Cohort Month", color="Retention Rate (%)"),
                       x=cohort_pivot.columns,
                       y=cohort_pivot.index,
                       color_continuous_scale="YlOrRd",
                       title="Customer Cohort Retention Heatmap")
        fig.show()
        
        # Cohort size analysis
        cohort_sizes = cohort_df[cohort_df['months_since_first_purchase'] == 0]
        if not cohort_sizes.empty:
            fig = px.bar(cohort_sizes, 
                        x='cohort_month', 
                        y='cohort_size',
                        title='Cohort Sizes by Month',
                        labels={'cohort_size': 'Number of Customers', 'cohort_month': 'Cohort Month'})
            fig.update_xaxes(tickangle=45)
            fig.show()
        
        log_step("Cohort Visualization", "SUCCESS", "Cohort analysis charts created")
        
    except Exception as e:
        error_msg = f"Failed to create cohort visualization: {str(e)}"
        log_step("Cohort Visualization", "ERROR", error_msg)
        print(f"❌ {error_msg}")

create_cohort_analysis_visualization()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary Dashboard

# COMMAND ----------

def create_executive_dashboard():
    """Create executive summary dashboard with key metrics"""
    log_step("Executive Dashboard", "STARTED", "Creating executive summary dashboard")
    
    try:
        # Calculate key metrics
        customer_df = analytics_data["customer_analytics"]
        product_df = analytics_data["product_analytics"]
        monthly_df = analytics_data["monthly_sales"]
        category_df = analytics_data["category_sales"]
        
        # Key business metrics
        total_customers = len(customer_df)
        total_revenue = monthly_df['total_revenue'].sum() if not monthly_df.empty else 0
        total_orders = monthly_df['total_orders'].sum() if not monthly_df.empty else 0
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        
        active_customers = len(customer_df[customer_df['recency_segment'].isin(['Recent', 'Active'])]) if not customer_df.empty else 0
        high_value_customers = len(customer_df[customer_df['value_segment'] == 'High Value']) if not customer_df.empty else 0
        
        # Display metrics
        print("=" * 60)
        print("📊 EXECUTIVE SUMMARY DASHBOARD")
        print("=" * 60)
        
        print(f"\n💰 REVENUE METRICS:")
        print(f"   • Total Revenue: ${total_revenue:,.2f}")
        print(f"   • Total Orders: {total_orders:,}")
        print(f"   • Average Order Value: ${avg_order_value:.2f}")
        
        print(f"\n👥 CUSTOMER METRICS:")
        print(f"   • Total Customers: {total_customers:,}")
        print(f"   • Active Customers: {active_customers:,} ({active_customers/total_customers*100:.1f}%)")
        print(f"   • High Value Customers: {high_value_customers:,} ({high_value_customers/total_customers*100:.1f}%)")
        
        print(f"\n🛍️ PRODUCT METRICS:")
        if not product_df.empty:
            total_products = len(product_df)
            products_with_sales = len(product_df[product_df['total_quantity_sold'] > 0])
            print(f"   • Total Products: {total_products:,}")
            print(f"   • Products with Sales: {products_with_sales:,} ({products_with_sales/total_products*100:.1f}%)")
        
        print(f"\n📈 CATEGORY PERFORMANCE:")
        if not category_df.empty:
            top_category = category_df.loc[category_df['total_revenue'].idxmax()]
            print(f"   • Top Category: {top_category['category']}")
            print(f"   • Top Category Revenue: ${top_category['total_revenue']:,.2f}")
        
        # Create summary visualization
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=('Revenue Trend', 'Customer Segments', 'Category Performance',
                           'Order Value Distribution', 'Customer Retention', 'Product Performance'),
            specs=[[{"type": "scatter"}, {"type": "pie"}, {"type": "bar"}],
                   [{"type": "histogram"}, {"type": "bar"}, {"type": "scatter"}]]
        )
        
        # Revenue trend
        if not monthly_df.empty:
            monthly_sorted = monthly_df.sort_values(['order_year', 'order_month'])
            fig.add_trace(go.Scatter(x=monthly_sorted['year_month'], y=monthly_sorted['total_revenue'],
                                    mode='lines+markers', name='Revenue'), row=1, col=1)
        
        # Customer segments
        if not customer_df.empty:
            value_counts = customer_df['value_segment'].value_counts()
            fig.add_trace(go.Pie(labels=value_counts.index, values=value_counts.values), row=1, col=2)
        
        # Category performance
        if not category_df.empty:
            fig.add_trace(go.Bar(x=category_df['category'], y=category_df['total_revenue']), row=1, col=3)
        
        # Order value distribution
        if not customer_df.empty:
            fig.add_trace(go.Histogram(x=customer_df['avg_order_value'], nbinsx=20), row=2, col=1)
        
        # Customer recency
        if not customer_df.empty:
            recency_counts = customer_df['recency_segment'].value_counts()
            fig.add_trace(go.Bar(x=recency_counts.index, y=recency_counts.values), row=2, col=2)
        
        # Product performance
        if not product_df.empty:
            fig.add_trace(go.Scatter(x=product_df['total_quantity_sold'], y=product_df['total_revenue'],
                                    mode='markers', name='Products'), row=2, col=3)
        
        fig.update_layout(height=1000, showlegend=False, title_text="Executive Summary Dashboard")
        fig.show()
        
        log_step("Executive Dashboard", "SUCCESS", "Executive dashboard created")
        
    except Exception as e:
        error_msg = f"Failed to create executive dashboard: {str(e)}"
        log_step("Executive Dashboard", "ERROR", error_msg)
        print(f"❌ {error_msg}")

create_executive_dashboard()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Analytics Insights

# COMMAND ----------

def generate_business_insights():
    """Generate key business insights from the data analysis"""
    log_step("Business Insights", "STARTED", "Generating business insights")
    
    try:
        customer_df = analytics_data["customer_analytics"]
        product_df = analytics_data["product_analytics"]
        monthly_df = analytics_data["monthly_sales"]
        category_df = analytics_data["category_sales"]
        
        insights = []
        
        # Customer insights
        if not customer_df.empty:
            # Top spending age group
            age_spending = customer_df.groupby('customer_age_group')['total_spent'].mean().sort_values(ascending=False)
            top_age_group = age_spending.index[0]
            avg_spending = age_spending.iloc[0]
            insights.append(f"🎯 The {top_age_group} age group has the highest average spending at ${avg_spending:.2f}")
            
            # Customer retention insight
            churned_customers = len(customer_df[customer_df['recency_segment'] == 'Churned'])
            churn_rate = churned_customers / len(customer_df) * 100
            insights.append(f"⚠️ Customer churn rate is {churn_rate:.1f}% - consider retention strategies")
            
            # Geographic insight
            country_revenue = customer_df.groupby('country')['total_spent'].sum().sort_values(ascending=False)
            top_country = country