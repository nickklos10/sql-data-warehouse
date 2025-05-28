# SQL Data Warehouse

A complete SQL-based data warehouse implementation featuring a medallion architecture (Bronze, Silver, Gold layers) for processing and analyzing customer and sales data from multiple source systems.

## 📋 Overview

This project demonstrates a modern data warehouse design pattern using a medallion architecture to transform raw business data into analytics-ready datasets. The warehouse integrates data from two primary source systems:

- **CRM System**: Customer information, product catalog, and sales transactions
- **ERP System**: Customer demographics, location data, and product categorization

## 🏗️ Architecture

### Medallion Architecture Layers

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │    │   Silver    │    │    Gold     │
│  (Raw Data) │───▶│ (Cleaned)   │───▶│ (Analytics) │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

#### 🥉 Bronze Layer
- **Purpose**: Raw data ingestion from source systems
- **Data Quality**: As-is from source, minimal transformations
- **Tables**:
  - `crm_cust_info` - Customer master data
  - `crm_prd_info` - Product catalog
  - `crm_sales_details` - Sales transactions
  - `erp_loc_a101` - Customer location data
  - `erp_cust_az12` - Customer demographics
  - `erp_px_cat_g1v2` - Product categories

#### 🥈 Silver Layer
- **Purpose**: Cleaned, validated, and enriched data
- **Data Quality**: Business rules applied, data types standardized
- **Features**:
  - Data validation and quality checks
  - Standardized column naming
  - Data warehouse timestamps (`dwh_create_date`)
  - Consistent data types and formats

#### 🥇 Gold Layer
- **Purpose**: Business-ready analytical views
- **Data Quality**: Fully dimensional model, optimized for analytics
- **Views**:
  - `dim_customers` - Customer dimension with integrated CRM/ERP data
  - `dim_products` - Product dimension with category hierarchies
  - `fact_sales` - Sales fact table with dimensional relationships

## 📁 Project Structure

```
sql-data-warehouse/
├── datasets/                  # Source data files
│   ├── source_crm/           # CRM system exports
│   │   ├── cust_info.csv     # Customer information
│   │   ├── prd_info.csv      # Product catalog
│   │   └── sales_details.csv # Sales transactions
│   └── source_erp/           # ERP system exports
│       ├── CUST_AZ12.csv     # Customer demographics
│       ├── LOC_A101.csv      # Location data
│       └── PX_CAT_G1V2.csv   # Product categories
├── scripts/                  # SQL deployment scripts
│   ├── bronze/               # Bronze layer DDL and ETL
│   │   ├── ddl_bronze.sql    # Table definitions
│   │   └── proc_load_bronze.sql # Data loading procedures
│   ├── silver/               # Silver layer DDL and ETL
│   │   ├── ddl_silver.sql    # Table definitions
│   │   └── proc_load_silver.sql # Data transformation procedures
│   ├── gold/                 # Gold layer DDL
│   │   └── ddl_gold.sql      # Dimensional views
│   ├── create_datawarehouse_db.sql # Database creation
│   └── create_schemas_in_datawarehouse.sql # Schema setup
├── tests/                    # Data quality validation
│   ├── quality_checks_silver.sql # Silver layer validations
│   └── quality_checks_gold.sql   # Gold layer validations
└── README.md                 # This file
```

## 🚀 Getting Started

### Prerequisites

- PostgreSQL 12+ (or compatible SQL database)
- Database client (psql, pgAdmin, DBeaver, etc.)
- CSV import capabilities

### Database Setup

1. **Create the database:**
   ```sql
   -- Run scripts/create_datawarehouse_db.sql
   DROP DATABASE IF EXISTS datawarehouse;
   CREATE DATABASE datawarehouse;
   ```

2. **Create schemas:**
   ```sql
   -- Run scripts/create_schemas_in_datawarehouse.sql
   CREATE SCHEMA bronze;
   CREATE SCHEMA silver;
   CREATE SCHEMA gold;
   ```

3. **Deploy Bronze layer:**
   ```sql
   -- Run scripts/bronze/ddl_bronze.sql
   \i scripts/bronze/ddl_bronze.sql
   ```

4. **Deploy Silver layer:**
   ```sql
   -- Run scripts/silver/ddl_silver.sql
   \i scripts/silver/ddl_silver.sql
   ```

5. **Deploy Gold layer:**
   ```sql
   -- Run scripts/gold/ddl_gold.sql
   \i scripts/gold/ddl_gold.sql
   ```

### Data Loading

1. **Load raw data into Bronze tables:**
   ```sql
   -- Use your preferred method to load CSV files:
   -- Option 1: Using COPY command
   \COPY bronze.crm_cust_info FROM 'datasets/source_crm/cust_info.csv' WITH CSV HEADER;
   
   -- Option 2: Run the ETL procedure
   \i scripts/bronze/proc_load_bronze.sql
   ```

2. **Transform data to Silver layer:**
   ```sql
   \i scripts/silver/proc_load_silver.sql
   ```

3. **Verify data quality:**
   ```sql
   \i tests/quality_checks_silver.sql
   \i tests/quality_checks_gold.sql
   ```

## 🔍 Usage Examples

### Customer Analysis
```sql
-- Top customers by sales volume
SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    c.country,
    SUM(f.sales_amount) AS total_sales,
    COUNT(f.order_number) AS order_count
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, customer_name, c.country
ORDER BY total_sales DESC
LIMIT 10;
```

### Product Performance
```sql
-- Product sales by category
SELECT 
    p.category,
    p.subcategory,
    SUM(f.sales_amount) AS category_sales,
    AVG(f.price) AS avg_price
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY category_sales DESC;
```

### Monthly Sales Trends
```sql
-- Sales trend analysis
SELECT 
    DATE_TRUNC('month', f.order_date) AS month,
    SUM(f.sales_amount) AS monthly_sales,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(f.order_number) AS total_orders
FROM gold.fact_sales f
GROUP BY month
ORDER BY month;
```

## 🧪 Data Quality

The project includes comprehensive data quality checks:

- **Referential Integrity**: Primary key uniqueness and foreign key relationships
- **Data Completeness**: NULL value detection in critical fields
- **Data Consistency**: Cross-table validation and business rule enforcement
- **Data Format**: Standardized formats and trimmed whitespace
- **Logical Validation**: Date ranges, calculated field verification

Run quality checks after each data load:
```sql
\i tests/quality_checks_silver.sql
\i tests/quality_checks_gold.sql
```

## 📊 Business Intelligence

The Gold layer provides analytics-ready data for:

- **Customer Segmentation**: Demographics, geography, purchase behavior
- **Product Analysis**: Category performance, pricing strategies
- **Sales Analytics**: Trends, seasonality, growth metrics
- **Operational Insights**: Order fulfillment, shipping performance

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your changes with appropriate tests
4. Ensure data quality checks pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏷️ Tags

`#datawarehouse` `#sql` `#medallion-architecture` `#etl` `#analytics` `#postgresql` `#dataengineering` `#businessintelligence`

---