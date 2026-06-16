#!/usr/bin/env python
# coding: utf-8

# In[1]:


import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

st.set_page_config(page_title="Semiconductor Dashboard", layout="wide")

st.title("Semiconductor Stock Dashboard")

@st.cache_data
def load_data():
    engine = create_engine(
        "postgresql://postgres:0241@localhost:5432/semiconductor"
    )

    query = """
        SELECT datetime, symbol, open, high, low, close, volume,
               close_normalized, return
        FROM stock_data
    """

    df = pd.read_sql(query, engine, parse_dates=["datetime"])
    return df

df = load_data()

st.sidebar.header("Filters")

symbols = sorted(df["symbol"].unique())
selected_symbols = st.sidebar.multiselect(
    "Select companies",
    options=symbols,
    default=symbols
)

min_date = df["datetime"].min().date()
max_date = df["datetime"].max().date()

date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

filtered_df = df[df["symbol"].isin(selected_symbols)].copy()

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df["datetime"].dt.date >= start_date) &
        (filtered_df["datetime"].dt.date <= end_date)
    ]

if filtered_df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

filtered_df = filtered_df.sort_values(["symbol", "datetime"])

# KPI
col1, col2, col3 = st.columns(3)
col1.metric("Rows", f"{len(filtered_df):,}")
col2.metric("Companies", f"{filtered_df['symbol'].nunique()}")
col3.metric("Average Close", f"{filtered_df['close'].mean():.2f}")

# Chart 1
st.subheader("Average Close Price by Company")

avg_close = filtered_df.groupby("symbol")["close"].mean().sort_values()

max_symbol = avg_close.idxmax()
min_symbol = avg_close.idxmin()

colors = ["tab:blue"] * len(avg_close)
for i, sym in enumerate(avg_close.index):
    if sym == max_symbol or sym == min_symbol:
        colors[i] = "tab:red"

overall_mean = avg_close.mean()

fig1, ax1 = plt.subplots(figsize=(10, 5))
avg_close.plot(kind="bar", color=colors, ax=ax1)

ax1.set_title("Average Close Price by Company")
ax1.set_ylabel("Close Price")
ax1.axhline(
    y=overall_mean,
    color="gray",
    linestyle="--",
    linewidth=1,
    label=f"Overall mean ({overall_mean:.2f})"
)
ax1.legend()
plt.xticks(rotation=45)
plt.tight_layout()

st.pyplot(fig1, width="stretch")

# Chart 2
st.subheader("Closing Price Trend by Timestamp")

fig2, ax2 = plt.subplots(figsize=(12, 6))

for symbol in filtered_df["symbol"].unique():
    temp = filtered_df[filtered_df["symbol"] == symbol].sort_values("datetime")
    ax2.plot(temp["datetime"], temp["close"], label=symbol)

ax2.set_title("Closing Price Trend by Timestamp")
ax2.set_xlabel("Timestamp")
ax2.set_ylabel("Close Price")
ax2.legend()
plt.xticks(rotation=45)
plt.tight_layout()

st.pyplot(fig2, width="stretch")

# Raw data
st.subheader("Raw Data")
st.dataframe(filtered_df, width="stretch")

# =========================
# Linear Regression Section
# =========================
st.subheader("Linear Regression: Predict Next-Day Close")

ml_df = filtered_df.copy()

ml_df["next_close"] = ml_df.groupby("symbol")["close"].shift(-1)
ml_df = ml_df.dropna(subset=["close_normalized", "return", "next_close"])

if len(ml_df) < 10:
    st.warning("Not enough data to train the regression model.")
else:
    features = ["close_normalized", "return"]
    target = "next_close"

    X = ml_df[features]
    y = ml_df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = LinearRegression()
    model.fit(X_train, y_train)

    pred = model.predict(X_test)

    mse = mean_squared_error(y_test, pred)
    r2 = r2_score(y_test, pred)

    col4, col5 = st.columns(2)
    col4.metric("MSE", f"{mse:.4f}")
    col5.metric("R²", f"{r2:.4f}")

    coef_df = pd.DataFrame({
        "feature": features,
        "coefficient": model.coef_
    })

    st.markdown("### Model Coefficients")
    st.dataframe(coef_df, width="stretch")
    st.write(f"Intercept: {model.intercept_:.4f}")

    fig3, ax3 = plt.subplots(figsize=(7, 7))
    ax3.scatter(y_test, pred, alpha=0.7)

    min_val = min(y_test.min(), pred.min())
    max_val = max(y_test.max(), pred.max())
    ax3.plot([min_val, max_val], [min_val, max_val], "r--")

    ax3.set_xlabel("Actual next_close")
    ax3.set_ylabel("Predicted next_close")
    ax3.set_title("Actual vs Predicted Next-Day Close")
    plt.tight_layout()

    st.pyplot(fig3, width="stretch")

    # Latest prediction by symbol
    rows = []

    for symbol in filtered_df["symbol"].unique():
        latest = filtered_df[filtered_df["symbol"] == symbol].sort_values("datetime").iloc[-1]

        if pd.notna(latest["close_normalized"]) and pd.notna(latest["return"]):
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

    if not pred_all.empty:
        st.markdown("### Latest Prediction by Company")
        st.dataframe(pred_all, width="stretch")

        x = np.arange(len(pred_all))
        width = 0.35

        fig4, ax4 = plt.subplots(figsize=(10, 5))
        ax4.bar(x - width/2, pred_all["today_close"], width, label="Today close")
        ax4.bar(x + width/2, pred_all["predicted_next_close"], width, label="Predicted next close")

        ax4.set_xticks(x)
        ax4.set_xticklabels(pred_all["symbol"])
        ax4.set_ylabel("Price")
        ax4.set_title("Today vs Predicted Next-Day Close")
        ax4.legend()
        plt.tight_layout()

        st.pyplot(fig4, width="stretch")


# In[ ]:




