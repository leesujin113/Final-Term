# Semiconductor ETL Project

# Semiconductor Stock ETL Pipeline

## Overview
This project builds an automated ETL pipeline for U.S. semiconductor stock data using the Twelve Data API, Python, PostgreSQL, and Apache Airflow.  
The pipeline collects market data, cleans and transforms it, stores it in a PostgreSQL database, schedules recurring runs with Airflow, sends completion alerts by email, and supports downstream analysis and predictive modeling.

## Pipeline
1. **Extract**: Collect daily stock data for selected semiconductor companies (INTC, NVDA, QCOM, MU, AVGO, AMD) from the Twelve Data API.
2. **Transform**: Clean the raw dataset by removing missing values, filtering invalid prices, normalizing close prices, calculating returns, standardizing returns, and encoding ticker symbols.
3. **Load**: Store the processed data in a PostgreSQL table named `stock_data`.
4. **Orchestrate**: Run the workflow with an Airflow DAG scheduled for daily execution at 7:00 AM.
5. **Notify**: Send a completion email with `EmailOperator` after the ETL job finishes successfully.
6. **Analyze**: Explore the cleaned data with Python and visualization libraries.
7. **Model**: Build a regression model with scikit-learn to support stock price prediction.

## Tech Stack
- **Python** – Core language for ETL, analysis, and modeling.
- **Pandas** – Data cleaning, preprocessing, and feature engineering.
- **PostgreSQL** – Relational database for storing processed stock data.
- **Apache Airflow** – Workflow orchestration, scheduling, and email notification.
- **Matplotlib** – Data visualization and exploratory analysis.
- **scikit-learn** – Regression modeling and evaluation.
- **Twelve Data API** – Source of historical stock market data.

## Project Structure
```bash
.
├── Final-term.ipynb
└── README.md
```

## How to Run

### 1. Clone the repository
```bash
git clone https://github.com/leesujin113/Final-Term.git
cd Final-Term
```

### 2. Install dependencies
```bash
pip install pandas twelvedata sqlalchemy psycopg2-binary matplotlib scikit-learn apache-airflow
```

### 3. Configure the Twelve Data API key
Replace `YOUR_API_KEY` in the code with your valid Twelve Data API key before running the data collection step.

### 4. Set up PostgreSQL
Create a PostgreSQL database named `semiconductor` and make sure the connection string matches your local setup:

```python
postgresql://postgres:0241@localhost:5432/semiconductor
```

### 5. Run the notebook or script
You can run the workflow in either of these ways:
- Execute `Final-term.ipynb` step by step in Jupyter Notebook.
- Place `Final-term-1.py` (the Airflow DAG) in your Airflow `dags/` directory.

### 6. Start Airflow locally
```bash
airflow standalone
```

Then open the Airflow UI, turn on the `semiconductor_etl` DAG, and let it run on schedule.

### 7. Schedule
The DAG is configured to run every day at 7:00 AM:

```python
schedule = "0 7 * * *"
```

### 8. Email alert
When the ETL pipeline finishes successfully, Airflow sends a notification email using `EmailOperator` to the configured recipient address.

### 9. Check the Streamlit Dashboard
```streamlit run yourscript.py
```

## Output
- Raw semiconductor stock dataset collected from the API.
- Cleaned and transformed dataset with engineered features.
- PostgreSQL table containing processed stock records.
- Airflow-managed scheduled ETL workflow.
- Visual analysis plots and a regression model for prediction tasks.

## Future Improvements
- Move API keys and database credentials to environment variables.
- Add logging, retry policies, and monitoring/alerting.
- Deploy Airflow on a dedicated server or cloud VM for always-on scheduling.
- Extend the modeling pipeline with more features and model types.
