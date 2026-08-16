# Databricks-ELT-Pipeline

A production-ready Databricks Lakeflow Spark Declarative Pipeline implementing a medallion architecture (Bronze → Silver → Gold) for retail transaction data processing and analytics.

## 📊 Architecture Overview

This pipeline follows the **Medallion Architecture** pattern for data lakehouse best practices:

```
┌─────────────────────────────────────────────────────────────┐
│  Source: dataengineering_project.default.pofolioproject     │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
            ┌───────────────────────────────┐
            │   🥉 BRONZE LAYER             │
            │   bronze_transactions         │
            │   • Raw data ingestion        │
            │   • Streaming table           │
            │   • No transformations        │
            └──────────────┬────────────────┘
                           │
                           ▼
            ┌───────────────────────────────┐
            │   🥈 SILVER LAYER             │
            │   silver_transactions_cleaned │
            │   • Data quality checks       │
            │   • Deduplication             │
            │   • Data type standardization │
            │   • Derived columns           │
            └──────────────┬────────────────┘
                           │
                           ▼
            ┌───────────────────────────────┐
            │   🥇 GOLD LAYER               │
            │   gold_daily_transactions     │
            │   • Daily aggregations        │
            │   • Business KPIs             │
            │   • Optimized for BI          │
            └───────────────────────────────┘
```

## 🎯 Pipeline Layers

### Bronze Layer: Raw Data Ingestion
**File:** `transformations/bronze/bronze_transactions.py`

**Purpose:** Capture all raw transaction data from the source system with no transformations.

**Implementation:**
* **Type:** Streaming Table
* **Source:** `dataengineering_project.default.pofolioproject`
* **Strategy:** Incremental ingestion using Spark Structured Streaming
* **Data Preservation:** All raw data is preserved exactly as received

**Output Schema:**
```
transaction_id        : string
transaction_date      : string/date
customer_id           : string
product_id            : string
quantity              : numeric
total_amount          : numeric
payment_method        : string
category              : string
store_location        : string
```

### Silver Layer: Data Quality & Transformation
**File:** `transformations/silver/silver_transactions_cleaned.py`

**Purpose:** Apply data quality rules, clean data, and prepare for analytics.

**Data Quality Expectations:**
* ✅ `valid_transaction_id` - Transaction ID must not be null (DROP)
* ✅ `valid_transaction_date` - Transaction date must not be null (DROP)
* ✅ `positive_amount` - Total amount must be greater than 0 (DROP)
* ✅ `positive_quantity` - Quantity must be greater than 0 (DROP)
* ⚠️ `recent_transactions` - Transactions should be within last 365 days (WARN)

**Transformations:**
1. **Deduplication:** Remove duplicate transactions by `transaction_id`
2. **Data Type Standardization:**
   * `transaction_date` → proper date type
   * `total_amount` → double
   * `quantity` → integer
3. **Text Cleaning:** Trim whitespace from categorical fields
4. **Derived Columns:**
   * `unit_price` - Calculated price per item
   * `transaction_year` - Year extracted from date
   * `transaction_month` - Month extracted from date
   * `transaction_day_of_week` - Day of week (1=Sunday, 7=Saturday)
   * `processed_timestamp` - Pipeline processing timestamp

**Output Schema:**
```
transaction_id            : string
transaction_date          : date
customer_id               : string
product_id                : string
quantity                  : int
total_amount              : double
payment_method            : string
category                  : string
store_location            : string
unit_price                : double (derived)
transaction_year          : int (derived)
transaction_month         : int (derived)
transaction_day_of_week   : int (derived)
processed_timestamp       : timestamp (derived)
```

### Gold Layer: Business Aggregations
**File:** `transformations/gold/gold_daily_transactions.py`

**Purpose:** Daily aggregated metrics optimized for business intelligence and reporting.

**Implementation:**
* **Type:** Materialized View
* **Optimization:** Clustered by `transaction_date` for fast time-series queries
* **Refresh Strategy:** Incremental updates from silver layer

**Metrics Calculated:**

**Overall Metrics:**
* `transaction_count` - Total number of transactions
* `total_revenue` - Sum of all transaction amounts
* `avg_transaction_value` - Average transaction size
* `total_items_sold` - Total quantity of items sold
* `unique_customers` - Distinct customer count
* `unique_products` - Distinct product count
* `unique_locations` - Distinct store locations

**Payment Method Breakdown:**
* `credit_card_count`
* `debit_card_count`
* `cash_count`
* `google_pay_count`
* `apple_pay_count`

**Category Revenue Breakdown:**
* `electronics_revenue`
* `apparel_revenue`
* `food_revenue`
* `office_supplies_revenue`

**Output Schema:**
```
transaction_date          : date (clustered)
transaction_count         : bigint
total_revenue             : double
avg_transaction_value     : double
total_items_sold          : bigint
unique_customers          : bigint
unique_products           : bigint
unique_locations          : bigint
credit_card_count         : bigint
debit_card_count          : bigint
cash_count                : bigint
google_pay_count          : bigint
apple_pay_count           : bigint
electronics_revenue       : double
apparel_revenue           : double
food_revenue              : double
office_supplies_revenue   : double
```

## ⚙️ Pipeline Configuration

**Pipeline Name:** END_END_PIPELINE

**Compute:**
* **Serverless:** Enabled (automatic scaling)
* **Photon:** Enabled (optimized query engine)
* **Channel:** CURRENT (latest stable features)

**Storage:**
* **Catalog:** workspace
* **Schema:** default

**Libraries:**
* Auto-discovery: `transformations/**` (all Python files)

## 🚀 Getting Started

### Prerequisites
* Databricks workspace with Unity Catalog enabled
* Access to source table: `dataengineering_project.default.pofolioproject`
* Permissions to create pipelines and tables in `workspace.default` catalog

### Installation & Setup

**1. Clone or Download Repository**
```bash
git clone https://github.com/YOUR_USERNAME/END_END_PIPELINE.git
cd END_END_PIPELINE
```

**2. Import to Databricks Workspace**
```bash
# Using Databricks CLI
databricks workspace import_dir \
  ./END_END_PIPELINE \
  /Users/YOUR_EMAIL@example.com/END_END_PIPELINE
```

Or use the Databricks UI:
* Workspace → Import → Select folder → Upload

**3. Create Pipeline**
* Navigate to **Workflows** → **Pipelines**
* Click **Create Pipeline**
* Configure:
  * **Name:** END_END_PIPELINE
  * **Libraries:** Add `transformations` folder path
  * **Target:** workspace.default
  * **Compute:** Enable Serverless and Photon
* Click **Create**

**4. Run Pipeline**
* Click **Start** to run your first update
* Monitor progress in the pipeline graph view

## 📈 Usage Examples

### Query Gold Layer for Daily Metrics
```sql
SELECT 
  transaction_date,
  transaction_count,
  total_revenue,
  avg_transaction_value,
  unique_customers
FROM workspace.default.gold_daily_transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY transaction_date DESC;
```

### Analyze Payment Method Trends
```sql
SELECT 
  transaction_date,
  credit_card_count,
  debit_card_count,
  cash_count,
  google_pay_count + apple_pay_count AS digital_wallet_count
FROM workspace.default.gold_daily_transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 90)
ORDER BY transaction_date;
```

### Category Performance Comparison
```sql
SELECT 
  SUM(electronics_revenue) AS total_electronics,
  SUM(apparel_revenue) AS total_apparel,
  SUM(food_revenue) AS total_food,
  SUM(office_supplies_revenue) AS total_office
FROM workspace.default.gold_daily_transactions
WHERE transaction_date BETWEEN '2026-07-01' AND '2026-07-31';
```

## 🔍 Monitoring & Observability

### Data Quality Monitoring
The pipeline includes built-in data quality checks:
* Navigate to Pipeline → **Data Quality** tab
* View expectations pass/fail rates
* Set up alerts for quality degradation

### Pipeline Health
* **Latest Updates:** View in Pipeline monitoring page
* **Event Logs:** Detailed execution logs available
* **Lineage:** Auto-generated data lineage graph

## 🛠️ Development

### Local Development
```bash
# Install Databricks Connect for local testing
pip install databricks-connect

# Edit transformation files
vim transformations/silver/silver_transactions_cleaned.py

# Test locally (requires Databricks Connect setup)
python transformations/bronze/bronze_transactions.py
```

### Adding New Transformations
1. Create new Python file in appropriate layer folder
2. Use `@dp.table()` or `@dp.materialized_view()` decorators
3. Pipeline auto-discovers new files
4. Run update to apply changes

## 📊 Performance Optimization

**Current Optimizations:**
* ✅ Photon engine enabled for faster queries
* ✅ Serverless compute for automatic scaling
* ✅ Gold layer clustered by `transaction_date`
* ✅ Streaming ingestion for low latency
* ✅ Materialized views for pre-aggregated data

**Future Enhancements:**
* [ ] Add Liquid Clustering to silver layer
* [ ] Implement Z-Ordering on high-cardinality columns
* [ ] Add partition pruning strategies
* [ ] Enable Auto Optimize for maintenance

## 🔐 Security & Governance

* **Unity Catalog Integration:** All tables registered in Unity Catalog
* **Access Control:** Managed through UC permissions
* **Data Lineage:** Automatic tracking through Unity Catalog
* **Audit Logs:** All pipeline operations logged

## 🐛 Troubleshooting

### Common Issues

**Pipeline fails on first run:**
* Verify source table exists and is accessible
* Check catalog/schema permissions
* Review Event Logs for specific error messages

**Data quality expectations failing:**
* Query bronze layer to inspect raw data quality
* Adjust expectations or improve source data
* Use `@dp.expect()` instead of `@dp.expect_or_drop()` for warnings

**Slow performance:**
* Ensure Photon is enabled
* Check cluster size if not using serverless
* Review query patterns and add appropriate clustering

## 📝 License

[Add your license here]

## 👥 Contributors

* **Your Name** - Initial development and architecture

## 📧 Contact

For questions or support, please contact:
* **Email:** zulusanele626@gmail.com
* **Phone:** 069 116 22 55

