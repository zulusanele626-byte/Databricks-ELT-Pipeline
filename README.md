# Databricks ELT Pipeline

A production-ready Databricks ELT pipeline implementing a **Medallion Architecture (Bronze → Silver → Gold)** for retail transaction data processing and analytics.

The pipeline uses **Lakeflow Spark Declarative Pipelines** to ingest, clean, validate, transform, and aggregate transaction data for business intelligence and reporting.

## 📊 Architecture Overview

The pipeline follows the Medallion Architecture pattern:

**Source → Bronze → Silver → Gold**

* **Bronze:** Raw transaction data ingestion
* **Silver:** Data cleaning, validation, deduplication, and transformation
* **Gold:** Business aggregations and analytics-ready metrics

## 🛠️ Technologies

* Databricks
* Lakeflow Spark Declarative Pipelines
* PySpark
* SQL
* Unity Catalog
* Spark Structured Streaming
* Git/GitHub
* AWS

## 🎯 Pipeline Layers

### 🥉 Bronze Layer — Raw Data Ingestion

**File:** `transformations/bronze/bronze_transactions.py`

The Bronze layer captures raw transaction data from the source system with minimal transformation.

**Key features:**

* Incremental ingestion using Spark Structured Streaming
* Raw data preservation
* Streaming table implementation

### 🥈 Silver Layer — Data Quality & Transformation

**File:** `transformations/silver/silver_transactions_cleaned.py`

The Silver layer cleans and prepares the data for analytics.

**Data quality checks:**

* Transaction ID must not be null
* Transaction date must not be null
* Total amount must be greater than 0
* Quantity must be greater than 0
* Recent transaction validation

**Transformations:**

* Deduplication using `transaction_id`
* Data type standardization
* Text cleaning
* Unit price calculation
* Transaction year/month extraction
* Day-of-week extraction
* Processing timestamp

### 🥇 Gold Layer — Business Aggregations

**File:** `transformations/gold/gold_daily_transactions.py`

The Gold layer produces analytics-ready daily metrics optimized for business intelligence and reporting.

**Key metrics include:**

* Transaction count
* Total revenue
* Average transaction value
* Total items sold
* Unique customers
* Unique products
* Store locations
* Payment method breakdown
* Category revenue breakdown

## 📈 Business Insights

The Gold layer enables analysis of:

* Daily revenue performance
* Transaction trends
* Customer activity
* Product performance
* Payment method usage
* Category revenue performance
* Average transaction value

## ⚙️ Pipeline Configuration

The pipeline is configured with:

* **Serverless compute** for automatic scaling
* **Photon** for optimized query execution
* **Unity Catalog** for data governance
* **Incremental processing** for efficient data ingestion
* Automatic discovery of transformation files

## 🚀 Project Structure

```text
Databricks-ELT-Pipeline/
│
├── transformations/
│   ├── bronze/
│   │   └── bronze_transactions.py
│   │
│   ├── silver/
│   │   └── silver_transactions_cleaned.py
│   │
│   └── gold/
│       └── gold_daily_transactions.py
│
├── README.md
└── LICENSE
```

## 🔍 Data Quality & Governance

The pipeline implements data quality checks throughout the transformation process.

Unity Catalog provides:

* Table governance
* Access control
* Data lineage
* Auditability

## ⚡ Performance Optimization

The pipeline uses several optimization techniques:

* Photon engine
* Serverless compute
* Streaming ingestion
* Incremental processing
* Clustering on `transaction_date`
* Materialized views for aggregated data

## 🔮 Future Improvements

Potential future enhancements include:

* Liquid Clustering for the Silver layer
* Additional partition pruning strategies
* Auto Optimize for maintenance
* Additional high-cardinality optimization strategies

## ▶️ How to Run

1. Clone the repository.
2. Import the project into your Databricks workspace.
3. Configure the Lakeflow pipeline.
4. Add the `transformations` directory as the pipeline source.
5. Configure the required Unity Catalog catalog and schema.
6. Start the pipeline and monitor the pipeline graph.

## 📄 License

MIT License

## 👤 Author

**Sanele Zulu**

Data Analyst | Aspiring Data Engineer

GitHub: [Add your GitHub profile link]

Email: [zulusalema626@gmail.com](mailto:zulusalema626@gmail.com)
