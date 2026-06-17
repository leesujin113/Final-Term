# Semiconductor Stock BI Pipeline

### MADSC301 — Business Intelligence Final Assignment

This project was developed for the MADSC301 Business Intelligence final assignment and builds a complete BI pipeline from data collection to analysis and visualization.
The project uses the Twelve Data API, Apache Airflow, PostgreSQL, and Streamlit to create an end-to-end workflow for semiconductor stock analysis.

---

## Project Objective

The main objective of this project is to monitor and analyze short-term trends in major U.S. semiconductor stocks through an automated ETL pipeline and interactive dashboard.
This project follows the assignment requirement to build a complete ETL/ELT pipeline including data collection, cleaning, storage, orchestration, and visualization.

---

## Business Case

Semiconductor companies are among the most important players in the global technology market, and their stock performance is closely linked to innovation, AI demand, and supply chain conditions.  
This project helps track price trends across major semiconductor companies and provides a simple business intelligence solution for comparing stock performance over time.

The selected companies are:

- Intel (`INTC`)
- NVIDIA (`NVDA`)
- Qualcomm (`QCOM`)
- Micron (`MU`)
- Broadcom (`AVGO`)
- AMD (`AMD`)

---

## Project Architecture

```text
Twelve Data API
      ↓
Airflow ETL Pipeline
(collect → clean → store → alert)
      ↓
PostgreSQL Database
(stockdata table)
      ↓
Streamlit Dashboard
(KPIs, charts, raw data, regression model)
```

This architecture reflects the required pipeline structure of data source → storage → visualization.

---

## Project Files

| File | Description |
|------|-------------|
| `airflow_etl.py` | Apache Airflow DAG that automates data collection, cleaning, storage, and email notification. |
| `dashboard.py` | Streamlit dashboard for stock analysis, filtering, visualization, and prediction.|
---

## ETL Pipeline

The ETL workflow is implemented in Apache Airflow and consists of four tasks connected in sequence: `collect >> clean >> store >> alert`.

### 1. Collect
- Connects to the Twelve Data API using `TDClient`.
- Retrieves daily stock data for six semiconductor companies.
- Downloads the latest 90 days of data.
- Saves the raw data as a CSV file.

### 2. Clean
- Reads the raw CSV file using pandas.
- Removes missing values.
- Filters rows where the closing price is less than or equal to 0.
- Creates engineered columns such as `closenormalized` and `return`.
- Saves the cleaned data to a second CSV file.

### 3. Store
- Connects to PostgreSQL using SQL Alchemy.
- Loads the cleaned dataset into the `stockdata` table.

### 4. Alert
- Sends an email after the ETL pipeline finishes successfully.

This workflow satisfies the assignment requirement for orchestration and scheduled automation.

---

## Database

The cleaned data is stored in a PostgreSQL database named `semiconductor`, which matches the assignment requirement to use an SQL or NoSQL database other than SQLite.

### Main table
- `stockdata`

### Main columns
- `datetime`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `closenormalized`
- `return`

---

## Dashboard Features

The Streamlit dashboard reads data directly from PostgreSQL and provides interactive business intelligence features for analysis and visualization.

### Main features
- Company filter using sidebar multiselect.
- Date range filter.
- KPI cards showing number of rows, number of companies, and average close price.
- Bar chart of average close price by company.
- Line chart showing closing price trends over time.
- Raw data table for detailed inspection.

These features satisfy the assignment requirement for post-storage data analysis and visualization.<img width="1217" height="671" alt="Screenshot 2026-06-17 at 16 27 01" src="https://github.com/user-attachments/assets/f6a4b993-1f60-4fac-8cc4-40e74c184ddb" />

---

## Machine Learning Extension

As an optional bonus feature, the dashboard also includes a simple linear regression model to predict the next-day closing price.

### Model details
- Features: `closenormalized`, `return`.
- Target: `nextclose`.
- Algorithm: Linear Regression from scikit-learn.

### Model outputs
- MSE (Mean Squared Error).
- R² score.
- Actual vs predicted scatter plot.
- Latest prediction table by company.
- Comparison chart of current close vs predicted next close.
---

## Technologies Used

- **Python** — core programming language for ETL and dashboard logic.
- **pandas** — data cleaning and transformation.
- **NumPy** — numerical processing.
- **PostgreSQL** — structured data storage.
- **SQLAlchemy** — database connection and table loading.
- **Apache Airflow** — workflow orchestration and scheduling.
- **Streamlit** — dashboard interface.
- **Matplotlib** — chart generation.
- **scikit-learn** — regression model.

---

## How to Run the Project

### 1. Install dependencies

```bash
pip install pandas numpy sqlalchemy psycopg2-binary twelvedata apache-airflow streamlit matplotlib scikit-learn
```

### 2. Create the PostgreSQL database

Create a PostgreSQL database named `semiconductor` and update the connection string if needed.

### 3. Configure credentials

Before running the project, update the following credentials:

- Twelve Data API key.
- PostgreSQL username and password.
- Email sender credentials for SMTP alert.


### 4. Run the ETL pipeline

If using Airflow, place `airflow_etl.py` inside the Airflow DAGs folder and trigger the DAG from the Airflow UI.

### 5. Run the dashboard

```bash
streamlit run dashboard.py
```

---

## Assignment Requirement Mapping

| Assignment Requirement | Implementation |
|------------------------|----------------|
| Data Collection | Twelve Data API is used to collect stock market data. |
| Data Cleaning & Preparation | Missing values are removed, invalid rows are filtered, and new analytical columns are created. |
| Data Storage | Cleaned data is stored in PostgreSQL.|
| Workflow Orchestration | Airflow DAG automates the ETL process and supports alerts.|
| Data Analysis & Visualization | Streamlit dashboard provides filtering, KPIs, charts, and raw data view.|
| Bonus Component | Linear regression model predicts next-day close price.|

---

## Conclusion

This project demonstrates a complete business intelligence pipeline using financial market data from a public API to dashboard visualization.
It includes data collection, transformation, SQL storage, Airflow orchestration, and interactive dashboard analysis, which aligns well with the MADSC301 final assignment requirements.
