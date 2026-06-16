#!/usr/bin/env python
# coding: utf-8

# In[1]:


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import smtplib
from email.mime.text import MIMEText
import os

DATA_DIR = "/Users/sujin/airflow/data"
RAW_FILE = f"{DATA_DIR}/semiconductor_stock_data.csv"
CLEAN_FILE = f"{DATA_DIR}/semiconductor_cleaned.csv"


def collect_data(**context):
    from twelvedata import TDClient
    import pandas as pd

    os.makedirs(DATA_DIR, exist_ok=True)

    td = TDClient(apikey="YOUR_TWELVEDATA_API_KEY")

    symbols = ["INTC", "NVDA", "QCOM", "MU", "AVGO", "AMD"]
    all_data = []

    for symbol in symbols:
        ts = td.time_series(symbol=symbol, interval="1day", outputsize=90)
        df = ts.as_pandas()
        df["symbol"] = symbol
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.to_csv(RAW_FILE, index=False)


def clean_data(**context):
    import pandas as pd

    df = pd.read_csv(RAW_FILE)
    df = df.dropna()
    df = df[df["close"] > 0]

    df["close_normalized"] = (
        (df["close"] - df["close"].min())
        / (df["close"].max() - df["close"].min())
    )

    df["return"] = df["close"].pct_change()

    df.to_csv(CLEAN_FILE, index=False)


def store_data(**context):
    from sqlalchemy import create_engine
    import pandas as pd

    engine = create_engine(
        "postgresql://postgres:0241@localhost:5432/semiconductor"
    )

    df = pd.read_csv(CLEAN_FILE)
    df.to_sql(
        "stock_data",
        engine,
        if_exists="append",
        index=False
    )


def send_mail(**context):
    msg = MIMEText(
        "The semiconductor stock data pipeline finished successfully."
    )

    msg["Subject"] = "ETL Pipeline Completed"
    msg["From"] = "suzunultube@gmail.com"
    msg["To"] = "sujin.lee@euruni.edu"

    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)

    server.login(
        "suzunultube@gmail.com",
        "tjwogxyalzcgobxm"
    )

    server.send_message(msg)
    server.quit()


dag = DAG(
    dag_id="airflow_etl",
    default_args={"retries": 1},
    schedule="0 7 * * *",
    start_date=datetime(2026, 6, 9),
    catchup=False,
)

collect = PythonOperator(
    task_id="collect",
    python_callable=collect_data,
    dag=dag,
)

clean = PythonOperator(
    task_id="clean",
    python_callable=clean_data,
    dag=dag,
)

store = PythonOperator(
    task_id="store",
    python_callable=store_data,
    dag=dag,
)

alert = PythonOperator(
    task_id="alert",
    python_callable=send_mail,
    dag=dag,
)

collect >> clean >> store >> alert


# In[ ]:




