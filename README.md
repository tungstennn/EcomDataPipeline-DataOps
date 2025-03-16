# End-to-End DataOps Batch ETL Project

## 1. Project Overview
This project builds an **ETL pipeline** that transforms raw sales transaction data from **AWS S3** into an **Amazon Redshift** data warehouse for **business intelligence (BI) and reporting**. The pipeline follows a **star schema model** and is orchestrated using **Apache Airflow (local setup)**.

This project leverages S3 as a data lake for structured historical data from an OLTP database, skipping the ingestion layer. The highly normalized OLTP data lacks business logic, making analysis difficult. To address this, I apply dimensional star schema modeling and business logic to enhance clarity and align with data warehousing standards. The ultimate goal is to create a Single Customer View (SCV), providing a unified dataset that improves insights, decision-making, and customer interactions across the data ecosystem.

## 2. Problem Statement
E-commerce businesses store transactional data in **AWS S3**, but it remains **semi-structured and inefficient** for analysis. This project automates the process of extracting, transforming, and loading **new batch data** daily, enabling efficient sales, customer, and product analytics in **Redshift**.

## 3. Project Scope
### **Infrastructure (Terraform-Managed AWS Resources)**
✅ **Amazon S3** – Historical structued transanctional data (Data Lake).  
✅ **Amazon Redshift (Serverless)** – Data warehouse for analytics.  
✅ **IAM Roles & Policies** – Secure S3-Redshift access.  
✅ **Apache Airflow (Local Setup)** – Orchestrates the ETL workflow.  

### **ETL Process**
**1. Extract**: Copy new batch data from **S3** (Parquet format).  
**2. Transform**: Clean and structure data into **fact and dimension tables** using star schema approach and apply business logic.  
**3. Load**: Store transformed data in **Amazon Redshift** for analytical purposes.  
**4. Orchestrate**: Automate ETL execution using **Airflow DAGs (local setup)**.

## 4. Tech Stack
- **Infrastructure as Code**: Terraform
- **Cloud Services**: AWS S3 (Data lake - Parquet format), Redshift (Data Warehouse), IAM
- **ETL Framework**: Python (Pandas/PySpark)
- **Orchestration**: Apache Airflow (Local Setup)

---
### 📌 **Status:** In Progress ⏳  
A detailed breakdown of the ETL process and schema will be added upon completion

---
