# End-to-End Big Data ETL & Analytics Pipeline

## Project Overview
This project is an end-to-end Big Data pipeline that demonstrates the extraction, transformation, and analytical processing of large-scale datasets. By leveraging the Hadoop ecosystem, the project efficiently handles data that exceeds standard memory constraints, transforming unstructured raw data into clean, aggregated metrics. The pipeline culminates in an interactive Streamlit dashboard that visualizes the final business insights.

##  System Architecture & Workflow
The pipeline follows a strict modular architecture, separated into four distinct stages:

### 1. Data Storage (HDFS)
* **Ingestion:** Raw datasets are ingested into the Hadoop Distributed File System (HDFS).
* **Organization:** Data is structured into logical directories (`/raw`, `/processed`, and `/summary`) to separate raw inputs from transformed outputs.

### 2. Data Preprocessing (Apache PySpark)
* **Extraction:** PySpark reads the massive raw datasets directly from HDFS.
* **Cleaning & Transformation:** The data undergoes rigorous cleaning, including:
  * Handling missing and null values.
  * Removing duplicate records.
  * Correcting inconsistent data formats and casting correct data types.
  * Engineering new features and filtering out unnecessary columns.
* **Optimization:** The cleaned DataFrame is written back to HDFS in **Parquet** format. Parquet's columnar storage heavily optimizes the performance of downstream SQL queries.

### 3. Data Warehousing & Analysis (Apache Hive)
* **Warehousing:** Hive External Tables are created on top of the Spark Parquet outputs, allowing SQL querying without duplicating data.
* **Analysis:** Complex SQL queries are executed to extract meaningful business insights, KPIs, and aggregations.
* **Export:** The results of these heavy analytical queries are saved as lightweight aggregated tables for the presentation layer.

### 4. Data Visualization (Streamlit)
* **Dashboarding:** A Streamlit web application connects to the aggregated outputs to render an interactive analytics dashboard.
* **Features:** The dashboard includes time-series analysis, bar/line/pie charts, dynamic filters, and top-level KPI metrics to make the data easily digestible.

## 🛠️ Technologies Used
* **Distributed Storage:** Hadoop HDFS
* **Data Processing:** Apache Spark (PySpark)
* **Data Warehousing:** Apache Hive & SQL
* **Frontend Visualization:** Python & Streamlit
* **Environment:** Linux 

##  Repository Structure

├── README.md                  # Project documentation
├── requirements.txt           # Python dependencies (Streamlit, Pandas, etc.)
├── src/                       
│   ├── spark/                 # PySpark scripts for data cleaning and transformation
│   └── hive/                  # Hive SQL scripts for creating tables and aggregations
├── data/                      
│   └── summary/               # Lightweight, aggregated CSVs for the dashboard
├── dashboard/                 
│   └── app.py                 # Streamlit dashboard application
└── report/
    └── Project_Report.pdf     # Comprehensive documentation of findings and architecture


##  How to Run the Pipeline

**1. HDFS Storage Setup**
Ensure Hadoop is running, then create the directory structure and upload the raw data:
```bash
hdfs dfs -mkdir -p /project/data/raw
hdfs dfs -mkdir -p /project/data/processed
hdfs dfs -mkdir -p /project/data/summary
hdfs dfs -put local_dataset.csv /project/data/raw/