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

### **ELT Process**
**1. Extract**: Detect and prepare new batch files stored in **S3** (Parquet format)  
**2. Load**: Ingest raw data into **Redshift staging tables**  
**3. Transform**: Use **SQL and/or dbt** to create **fact and dimension tables** within Redshift, applying business logic and dimensional modeling  
**4. Orchestrate**: Automate the ELT pipeline with **Airflow DAGs (local setup)**  

## 4. Tech Stack
- **Infrastructure as Code**: Terraform
- **Cloud Services**: AWS S3 (Data Lake), Redshift (Data Warehouse), IAM
- **ELT Framework**: Python (for orchestration), SQL (for transformations), optionally dbt
- **Orchestration**: Apache Airflow (Local Setup)

---
### 📌 **Status:** In Progress ⏳  
A detailed breakdown of the ELT pipeline and schema will be added upon completion

---
