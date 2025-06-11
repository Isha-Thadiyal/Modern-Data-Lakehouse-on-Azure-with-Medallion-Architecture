# Modern-Data-Lakehouse-on-Azure-with-Medallion-Architecture


This project walks through the implementation of a **modern data lakehouse architecture** using Microsoft Azure services such as Data Factory, Databricks, Data Lake Gen2, and Power BI. The pipeline is structured using the **Medallion architecture (Bronze, Silver, Gold)** and includes **Delta Lake**, **Unity Catalog**, and **Dimensional Modeling (Star Schema)**.

![Architecture Diagram](./Screenshot%20(18).png)

---

## 📌 Project Highlights

- ⛓️ Built a scalable **ETL pipeline** using **Azure Data Factory** and **Databricks**. Also ensure the incremental data loading.
- 🧽 Implemented **data transformation** and **cleaning** using **PySpark**
- 🪙 Applied **Bronze → Silver → Gold** layer architecture with **Delta Lake**
- 🧠 Included **Slowly Changing Dimensions (SCD)** and **Dimensional Modeling**
- 🔐 Managed data governance using **Unity Catalog**
- 📊 Delivered final outputs using **Power BI**

---

## 🔧 Tech Stack

| Category         | Tools/Tech Used                          |
|------------------|-------------------------------------------|
| Cloud Platform   | Microsoft Azure                           |
| Orchestration    | Azure Data Factory                        |
| Processing       | Databricks, PySpark                       |
| Storage          | Azure SQL Database, Azure Data Lake Gen2, Delta Lake          |
| Governance       | Unity Catalog                             |
| Data Modeling    | Star Schema, SCD                          |
| Reporting        | Power BI                                  |
| Version Control  | GitHub                                    |

---

## 🧱 Medallion Architecture Overview

| Layer   | Description                                            | Format    |
|---------|--------------------------------------------------------|-----------|
| Bronze  | Raw data ingested from SQL source via Data Factory     | Parquet   |
| Silver  | Cleaned and transformed data using PySpark             | Parquet   |
| Gold    | Final aggregated tables modeled for analytics/reporting| Delta     |

---

## 🔄 Dataflow

1. **External Source to Azure SQL Database**  
   The external source in this project is my git repo in which I'm having 2 csv files (raw_data and incremental_data). In this step I have created one pipeline (source_prep) to dump the external source data into the Azure SQL Database. 


2. **Transformation**  
   - Tool: Databricks using PySpark  
   - Process: Filtering, Cleaning, Joins, SCD  
   - Output: Silver and Gold tables in Delta Lake

3. **Serving**  
   - Model: Star Schema  
   - Tool: Power BI  
   - Output: Visual Reports for business insights

---

## 📂 Project Structure

```bash
azure-data-engineering/
│
├── ingestion/                # ADF pipelines JSON
├── transformations/          # PySpark notebooks/scripts
├── data_modeling/            # Star schema explanation
├── dashboard/                # Power BI screenshots or PBIX file
├── Screenshot (18).png       # Architecture diagram
├── README.md                 # Project documentation
└── requirements.txt          # Dependencies (if applicable)

