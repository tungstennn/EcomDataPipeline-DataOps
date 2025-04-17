# End-to-End DataOps Batch ELT Project

## 1. Project Overview
This project builds an **ELT pipeline** that loads raw sales transaction data from **AWS S3** into an **Amazon Redshift** data warehouse, where it is then transformed for **business intelligence (BI) and reporting**. The pipeline follows a **star schema model** and is orchestrated using **Apache Airflow (local setup)**.

The project uses S3 as a data lake to store structured historical data from an OLTP system. Because OLTP data is highly normalized and lacks business logic, it is not immediately useful for analysis. To address this, raw data is first **loaded into Redshift staging tables**, and then transformed into analytical **fact and dimension tables** using **SQL-based transformations** aligned with dimensional modeling standards. The final objective is to produce a **Single Customer View (SCV)** that enhances decision-making and insight generation across the business.

## 2. Problem Statement
E-commerce businesses often store transactional data in **AWS S3**, but it remains **semi-structured and inefficient** for analysis. This project automates the daily batch **loading** of new data into **Amazon Redshift**, where it is then **transformed** into a clean, denormalized schema for sales, customer, and product analytics.

## 3. Project Scope

### **Infrastructure (Terraform-Managed AWS Resources)**
✅ **Amazon S3** – Historical structured transactional data (Data Lake)  
✅ **Amazon Redshift (Serverless)** – Data warehouse for ELT operations  
✅ **IAM Roles & Policies** – Secure S3-Redshift access  
✅ **Apache Airflow (Local Setup)** – Orchestrates the ELT workflow  

```mermaid
graph TD
    A[Raw CSVs (Olist Dataset)] --> B[S3 Data Lake (Parquet Format)]
    B --> C[Amazon Redshift - Staging Tables]
    C --> D[Amazon Redshift - SQL Transformations]
    D --> E[Amazon Redshift - Star Schema (Fact & Dimension Tables)]
    E --> F[BI & Reporting Layer (Tableau / QuickSight / Streamlit)]

    subgraph Terraform-Provisioned Infrastructure
        B
        C
        D
        E
    end

    subgraph Orchestration
        G[Apache Airflow (Local)]
    end

    G --> B
    G --> C
    G --> D



## 4. Tech Stack
- **Infrastructure as Code**: Terraform
- **Cloud Services**: AWS S3 (Data Lake), Redshift (Data Warehouse), IAM
- **ELT Framework**: Python (for orchestration), SQL (for transformations), optionally dbt
- **Orchestration**: Apache Airflow (Local Setup)

---

## 5. Project Milestones & Remaining Work

### ✅ Completed Work

- **Provisioned Infrastructure with Terraform**
  - Set up and configured AWS services using Terraform modules:
    - **Amazon S3** bucket for storing raw data in Parquet format
    - **Amazon Redshift Serverless** cluster for data warehousing
    - **IAM roles and policies** to securely allow Redshift to access data in S3
  - Infrastructure is fully reproducible via code and can be torn down/recreated reliably

- **Exploratory Data Analysis (EDA)**
  - Performed EDA using python pandas on raw CSVs (from the Olist dataset) to understand:
    - Schema structure and relationships between entities
    - Data types, null values, inconsistencies, and distributions
    - Business-relevant metrics (e.g. sales volume, delivery performance, review scores)
  - Used insights from EDA to inform star schema design and transformation logic

- **Data Ingestion into Redshift**
  - Uploaded raw CSVs into the S3 bucket in preparation for batch processing
  - Created Redshift **staging tables** for each dataset to mirror the raw structure
  - Loaded data from S3 into Redshift staging using `COPY` commands

- **SQL Transformations & Business Logic**
  - Designed a **star schema** with appropriate fact and dimension tables
  - Wrote SQL scripts to:
    - Join, clean, and reshape staging data
    - Apply core business rules (e.g. aggregations, derived metrics, handling duplicates)
    - Populate fact tables (e.g. orders, order items) and dimension tables (e.g. customers, products)
      
---

### 🔧 Remaining Work

- [ ] **Develop Airflow DAGs**
  - Automate ELT pipeline to run batch loads on a schedule
  - Modularize tasks: file detection → staging load → transformation execution

- [ ] **Build dashboard for insights**
  - Connect Redshift to Tableau, QuickSight, or Streamlit
  - Visualize KPIs like revenue trends, top customers, product performance
