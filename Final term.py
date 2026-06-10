# 1. Data Collection
    ## Collect stock data from Twelve Data

import sys
sys.module_version = None

_ipython().system('pip install twelvedata pandas')

from twelvedata import TDClient
import pandas as pd

td = TDClient(apikey="c247c7bbc1f9434bb04c60ea0fa0fef4")

print(td)
dir(td) 



# Top 6 US semiconductor company tickers (Basic plan compatible)
symbols = [
    "INTC",       # Intel
    "NVDA",       # NVIDIA
    "QCOM",       # Qualcomm
    "MU",         # Micron
    "AVGO",       # Broadcom
    "AMD"         # AMD
]

all_data = []
for symbol in symbols:
    ts = td.time_series(
        symbol=symbol,
        interval="1day",
        outputsize=90,
        timezone="Europe/Berlin"
    )
    df = ts.as_pandas()
    df = df.reset_index()  
    df = df.rename(columns={"index": "timestamp"})
    df['symbol'] = symbol
    all_data.append(df)
    

combined_df = pd.concat(all_data, ignore_index=True)
combined_df.to_csv("semiconductor_stock_data_us_6.csv", index=False)
print("Data collection complete:", combined_df.shape)


# In[119]:


import pandas as pd

df = pd.read_csv("semiconductor_stock_data_us_6.csv")


# In[120]:


df.head()


# In[121]:


# 2. Data Cleaning & Preparation
    ## Handle missing or inconsistent data
    ## Normalize, standardize, or encode fields if needed
    ## Filter or transform based on your business case


# In[148]:


import numpy as np

# 2.1 Handle missing data
df = df.dropna()  # or: df['close'].fillna(df['close'].mean())

# 2.2 Handle inconsistent data (e.g., remove negative prices)
df = df[df['close'] > 0]

# 2.3 Normalization: scale close prices to the range 0–1
df['close_normalized'] = (df['close'] - df['close'].min()) / (df['close'].max() - df['close'].min())

# 2.4 Standardization: add return column and standardize it
df['return'] = df['close'].pct_change()
df['return_standardized'] = (df['return'] - df['return'].mean()) / df['return'].std()

# 2.5 Encoding: convert symbol to categorical codes
df['symbol_encoded'] = df['symbol'].astype('category').cat.codes


# In[136]:


df.head()


# In[177]:


# 3. Data Storage
    ## SQL database (PostgreSQL)


# In[178]:


pip install psycopg2-binary


# In[179]:


from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:0241@localhost:5432/semiconductor")

df = pd.read_csv("semiconductor_stock_data_us_6.csv")

df = df.dropna()
df = df[df["close"] > 0]

df["close_normalized"] = (df["close"] - df["close"].min()) / (
    df["close"].max() - df["close"].min()
)
df["return"] = df["close"].pct_change()
df["return_standardized"] = (df["return"] - df["return"].mean()) / df["return"].std()
df["symbol_encoded"] = df["symbol"].astype("category").cat.codes


# In[180]:


df.to_sql("stock_data", engine, if_exists="append", index=False)


# In[181]:


# 4. Workflow Orchestration
    ## Airflow, Airflow EmailOperator


# In[182]:


pip install apache-airflow[EXTRAS]==AIRFLOW_VERSION --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-AIRFLOW_VERSION/constraints-PYTHON_VERSION.txt"


# In[183]:


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime


def collect_data(**context):
    # Step 1: Collect stock market data from Twelve Data API
    from twelvedata import TDClient
    import pandas as pd

    td = TDClient(apikey="YOUR_API_KEY")

    symbols = [
        "INTC",   # Intel
        "NVDA",   # NVIDIA
        "QCOM",   # Qualcomm
        "MU",     # Micron
        "AVGO",   # Broadcom
        "AMD"     # AMD
    ]

    all_data = []

    for symbol in symbols:
        ts = td.time_series(symbol=symbol, interval="1day", outputsize=90)
        df = ts.as_pandas()
        df["symbol"] = symbol
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.to_csv("/data/semiconductor_stock_data.csv", index=False)


def clean_data(**context):
    # Step 2: Clean and preprocess the collected data
    import pandas as pd
    import numpy as np

    df = pd.read_csv("/data/semiconductor_stock_data.csv")
    df = df.dropna()
    df = df[df["close"] > 0]

    df["close_normalized"] = (df["close"] - df["close"].min()) / (
        df["close"].max() - df["close"].min()
    )
    df["return"] = df["close"].pct_change()

    df.to_csv("/data/semiconductor_cleaned.csv", index=False)


def store_data(**context):
    # Step 3: Store the cleaned data in PostgreSQL
    from sqlalchemy import create_engine
    import pandas as pd

    engine = create_engine(
        "postgresql://postgres:0241@localhost:5432/semiconductor"
    )

    df = pd.read_csv("/data/semiconductor_cleaned.csv")
    df.to_sql("stock_data", engine, if_exists="append", index=False)


dag = DAG(
    dag_id="semiconductor_etl",
    default_args={"retries": 1},
    schedule="@daily",
    start_date=datetime(2026, 6, 9),
    catchup=False
)

collect = PythonOperator(
    task_id="collect",
    python_callable=collect_data,
    dag=dag
)

clean = PythonOperator(
    task_id="clean",
    python_callable=clean_data,
    dag=dag
)

store = PythonOperator(
    task_id="store",
    python_callable=store_data,
    dag=dag
)

alert = EmailOperator(
    task_id="alert",
    to="sujin.lee@euruni.edu",
    subject="ETL Pipeline Completed",
    html_content="The semiconductor stock data pipeline finished successfully.",
    dag=dag
)

collect >> clean >> store >> alert


# In[184]:


# 5. Data Analysis & Visualization


# In[199]:


import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# 1. Connect to PostgreSQL
engine = create_engine(
    "postgresql://postgres:0241@localhost:5432/semiconductor"
)

# 2. Load data from the stock_data table (with timestamp)
query = """
    SELECT datetime, symbol, open, high, low, close, volume, close_normalized
    FROM stock_data
"""

df = pd.read_sql(query, engine, parse_dates=["datetime"])

# 3. Bar chart: Average close price by company
avg_close = df.groupby("symbol")["close"].mean()

# normal prices: blue, max&min: red
max_symbol = avg_close.idxmax()
min_symbol = avg_close.idxmin()

colors = ["tab:blue"] * len(avg_close)
for i, sym in enumerate(avg_close.index):
    if sym == max_symbol or sym == min_symbol:
        colors[i] = "tab:red"

# 전체 평균값 (모든 종목 평균)
overall_mean = avg_close.mean()

plt.figure(figsize=(10, 5))
avg_close.plot(
    kind="bar",
    color=colors,
    title="Average Close Price by Company"
)
plt.ylabel("Close Price")

# 평균 추세선 (수평 점선)
plt.axhline(
    y=overall_mean,
    color="gray",
    linestyle="--",
    linewidth=1,
    label=f"Overall mean ({overall_mean:.2f})"
)

plt.legend()
plt.tight_layout()
plt.show()

# 4. Line chart: Closing price trend using timestamp as x-axis
plt.figure(figsize=(12, 6))

for symbol in df["symbol"].unique():
    temp = df[df["symbol"] == symbol].sort_values("datetime")
    plt.plot(temp["datetime"], temp["close"], label=symbol)
    
plt.legend()
plt.title("Closing Price Trend by Timestamp")
plt.xlabel("Timestamp")
plt.ylabel("Close Price")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# In[186]:


# 6. Optional Components
    ## Machine Learning: Build a simple model


# In[187]:


from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# 1. Connect to PostgreSQL
engine = create_engine("postgresql://postgres:0241@localhost:5432/semiconductor")

# 2. Load data
query = """
    SELECT datetime, symbol, close, close_normalized, return
    FROM stock_data
"""
df = pd.read_sql(query, engine, parse_dates=["datetime"])

# 3. Sort by symbol and datetime
df = df.sort_values(["symbol", "datetime"]).copy()

# 4. Create next day's close as target
df["next_close"] = df.groupby("symbol")["close"].shift(-1)

# 5. Remove rows with missing values
df = df.dropna(subset=["close_normalized", "return", "next_close"])

# 6. Features and target
features = ["close_normalized", "return"]
target = "next_close"

X = df[features]
y = df[target]

# 7. Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# 8. Train linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# 9. Predict
pred = model.predict(X_test)

# 10. Evaluation
print("MSE:", mean_squared_error(y_test, pred))
print("R2:", r2_score(y_test, pred))

# 11. Coefficients
coef_df = pd.DataFrame({
    "feature": features,
    "coefficient": model.coef_
})
print(coef_df)
print("intercept:", model.intercept_)


# In[188]:


plt.figure(figsize=(7, 7))
plt.scatter(y_test, pred, alpha=0.7)
min_val = min(y_test.min(), pred.min())
max_val = max(y_test.max(), pred.max())
plt.plot([min_val, max_val], [min_val, max_val], "r--")  # 완전한 예측 기준선
plt.xlabel("Actual next_close")
plt.ylabel("Predicted next_close")
plt.title("Actual vs Predicted Next-Day Close")
plt.tight_layout()
plt.show()


# In[194]:


rows = []

for symbol in df["symbol"].unique():
    # The most recent date row for each category
    latest = df[df["symbol"] == symbol].sort_values("datetime").iloc[-1]

    X_new = pd.DataFrame(
    [[latest["close_normalized"], latest["return"]]],
    columns=["close_normalized", "return"]
)
    pred_next_close = model.predict(X_new)[0]

    rows.append({
        "symbol": symbol,
        "today_datetime": latest["datetime"],
        "today_close": latest["close"],
        "predicted_next_close": pred_next_close
    })

pred_all = pd.DataFrame(rows)

x = np.arange(len(pred_all))
width = 0.35

plt.figure(figsize=(10, 5))
plt.bar(x - width/2, pred_all["today_close"], width, label="Today close")
plt.bar(x + width/2, pred_all["predicted_next_close"], width, label="Predicted next close")

plt.xticks(x, pred_all["symbol"])
plt.ylabel("Price")
plt.title("Today vs Predicted Next-Day Close")
plt.legend()
plt.tight_layout()
plt.show()
