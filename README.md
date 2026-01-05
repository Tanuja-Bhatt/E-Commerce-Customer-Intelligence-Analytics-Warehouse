# E-Commerce-Customer-Intelligence-Analytics-Warehouse

> Designed a customer-centric analytics warehouse using PostgreSQL to analyze customer lifecycle, retention, churn, and lifetime value from raw transactional data using SQL.



---

##  GITHUB FOLDER STRUCTURE



```
ecommerce-customer-intelligence/
│
├── README.md
│
├── sql/
│   ├── 01_raw_ingestion.sql
│   ├── 02_staging_clean.sql
│   ├── 03_entity_tables.sql
│   ├── 04_fact_customer_orders.sql
│   ├── 05_dim_customers.sql
│   ├── 06_customer_behavior_summary.sql
│   └── 07_business_queries.sql
│

```


---



####  Project Overview

While traditional sales dashboards report revenue and profit, they often fail to explain *customer behavior*.
This project builds a **customer-centric analytics warehouse** using PostgreSQL to analyze customer lifecycle, retention, churn, and lifetime value using SQL.

---

####  Dataset

* Historical e-commerce transactional dataset (Superstore-style)
* Contains orders, customers, products, and sales metrics
* Data represents a historical snapshot (not real-time)

---

####  Data Modeling Approach

The project follows a layered analytics architecture:

* **Raw Layer** – Source data ingested as-is (no assumptions)
* **Staging Layer** – Cleaned and type-casted data
* **Analytics Layer** – Fact and dimension tables
* **Insight Layer** – Customer behavior summary

Key modeling choices:

* Star-schema inspired design
* `fact_customer_orders` built at **one row per customer per order**
* Time-based behavior derived using SQL window functions

---

####  Business Questions Answered

* Who are the top customers by lifetime value (LTV)?
* What percentage of customers are one-time buyers?
* How many customers are active vs churned?
* Do high-value customers purchase more frequently?
* Which customer segments generate the highest LTV?
* Which high-value customers are at risk of churn?

---

####  Key Insights 

* A small percentage of customers contributes a disproportionately high share of total revenue.
* A large segment of customers churns after their first purchase, indicating retention opportunities.
* High-value customers tend to have shorter purchase gaps, showing stronger loyalty.
* Corporate segment customers exhibit higher average lifetime value.

---

#### Technical Skills Demonstrated

* PostgreSQL
* Advanced SQL (CTEs, window functions, aggregations)
* Analytics data modeling (fact & dimension tables)
* Customer lifecycle, churn & LTV logic
* SQL-only business analysis

---
# Assumptions and Limitations

## Assumptions
- Customer churn is defined based on inactivity relative to the dataset’s latest transaction date.
- Customer lifecycle starts at the first recorded purchase.
- Discounts are assumed to be proportional at the order-item level.
- All transactions are treated as completed (no payment failure data available).

## Limitations
- Dataset represents historical data and does not reflect real-time behavior.
- No product return or refund information is available.
- Customer acquisition channels are not included.
- Cost price and margin details are not available for products.


# Warehouse Design & Data Modeling

## Overview
This project follows a layered analytics warehouse design to transform raw transactional data into customer-focused business insights using SQL.

The primary goal of the warehouse is to support **customer lifecycle, retention, churn, and lifetime value analysis**, rather than simple sales reporting.

---

## Data Architecture

The warehouse is organized into four logical layers:

### 1. Raw Source Layer
**Table:** `raw_superstore`

- Represents the original CSV data ingested as-is
- All columns stored as TEXT to avoid ingestion failures
- No transformations or assumptions applied

Purpose:
- Preserve source truth
- Enable reproducible transformations
- Avoid data loss during ingestion

---

### 2. Clean Staging Layer
**Table:** `stg_superstore_clean`

- Data types explicitly cast (dates, numerics)
- Invalid or malformed values handled safely
- Still retains row-level transactional granularity

Purpose:
- Prepare reliable data for downstream modeling
- Separate data quality handling from analytics logic

---

### 3. Entity Staging Layer
**Tables:**
- `raw_orders`
- `raw_customers`
- `raw_products`
- `raw_order_items`

Each table represents a single business entity.

| Table | Grain |
|------|------|
| raw_orders | One row per order |
| raw_customers | One row per customer |
| raw_products | One row per product |
| raw_order_items | One row per order-product |

Purpose:
- Normalize transactional data
- Reduce duplication
- Enable clean joins for analytics modeling

---

### 4. Analytics Layer
**Core Fact Table:** `fact_customer_orders`

**Grain:** One row per customer per order

This table is the backbone of the warehouse and enables:
- Purchase sequence analysis
- Time gaps between orders
- Customer-level aggregation (LTV, retention)

Derived using:
- Aggregations
- Window functions (`ROW_NUMBER`, `LAG`)
- Explicit grain control

---

### 5. Dimension Tables

#### `dim_customers`
Captures customer lifecycle attributes:
- First and last purchase dates
- Total orders
- Customer age
- Active vs churned status

Churn is defined **relative to the dataset’s latest transaction date** to ensure correct classification for historical data.

---

### 6. Insight Layer
**Table:** `customer_behavior_summary`

One row per customer summarizing:
- Lifetime value (LTV)
- Average order value
- Purchase frequency
- Average days between purchases
- Value segmentation (High / Mid / Low)

Purpose:
- Provide business-ready metrics
- Enable stakeholder-friendly querying
- Serve as the primary insight consumption layer

---

## Key Design Decisions

- **Customer-first modeling:** Fact table designed around customer behavior, not product sales.
- **Layer separation:** Raw, staging, and analytics logic intentionally isolated.
- **No hard-coded assumptions:** All derived fields documented and reproducible.
- **SQL-only approach:** Entire pipeline implemented using PostgreSQL SQL.

---

## Why This Design Works

This warehouse design allows analysts to:
- Answer behavioral questions without rewriting complex SQL
- Extend the model with new dimensions or metrics
- Maintain transparency and auditability of transformations

The design mirrors real-world analytics engineering practices used in modern data teams.

####  Repository Structure

```
sql/    → all transformation & analytics queries
```

---

