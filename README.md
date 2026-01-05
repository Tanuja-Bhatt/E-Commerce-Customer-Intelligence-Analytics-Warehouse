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
├── docs/
│   ├── assumptions_and_limitations.md
│   └── warehouse_design.md
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


####  Repository Structure

```
sql/    → all transformation & analytics queries
docs/   → design notes and assumptions
```

---

