# Semiconductor Stock ETL Pipeline

## Overview

This project implements an automated ETL pipeline for U.S. semiconductor stocks using the Twelve Data API, Python, PostgreSQL, Apache Airflow, and Streamlit.  
It collects daily market data, cleans and transforms it, loads the data into PostgreSQL, automates recurring ETL with Airflow (including email alerts), and exposes an interactive Streamlit dashboard with basic next‑day price prediction.

## Architecture

- **Twelve Data API** → source of daily OHLCV data for INTC, NVDA, QCOM, MU, AVGO, AMD  
- **Python ETL (`Final_term.py`)** → extract, clean, feature engineer, load to DB  
- **PostgreSQL (`stockdata` table)** → central storage for processed records  
- **Airflow DAG (`airflow_etl.py`)** → `collect → clean → store → alert` scheduled at 07:00 every day  
- **Streamlit app (`dashboard.py`)** → filters, charts, KPIs, regression model, latest predictions

## Features

### ETL & Storage

- Fetches up to 90 days of daily data per ticker from the Twelve Data API  
- Removes missing values and invalid prices (`close <= 0`)  
- Creates normalized close (`closenormalized`) and daily return (`return`) features  
- Writes cleaned records into PostgreSQL (`semiconductor` DB, `stockdata` table)

### Automation (Airflow)

- DAG ID: `airflow_etl`  
- Schedule: `0 7 * * *` (every day at 07:00)  
- Tasks:
  - `collect` – download raw CSV from Twelve Data
  - `clean` – clean and transform raw data
  - `store` – append cleaned data into PostgreSQL
  - `alert` – send completion email via Gmail SMTP

### Dashboard (Streamlit)

- Sidebar filters for ticker selection and date range  
- KPIs: row count, number of companies, average close price  
- Charts:
  - Average close price by company (bar)
  - Closing price trend over time (line)  
- Raw data table for the filtered subset  
- Linear regression model:
  - Features: `closenormalized`, `return`
  - Target: next‑day close (`nextclose`)
  - Shows MSE, R², coefficients, intercept, and actual vs predicted scatter plot  
- 
