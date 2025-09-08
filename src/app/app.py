import streamlit as st
import polars as pl
import time
from datetime import datetime
import src.shared.database as db

st.set_page_config(layout="wide")

def get_markets_data() -> pl.DataFrame:
    query = """
    SELECT * FROM markets
    WHERE timestamp = (SELECT MAX(timestamp) FROM markets)
    """

    df = pl.read_database_uri(
        query=query,
        uri=db.get_database_uri()
    )

    return df

data = (
    get_markets_data()
    .select(
        "timestamp",
        "ticker",
        "title",
        "expiration_time",
        "yes_ask",
        "no_ask"
    )
    .filter(
        pl.col('yes_ask').is_between(50, 100)
    )
    .sort("expiration_time", "yes_ask")
)

st.title("Kalshi Markets Dashbaord")

# Refresh controls
col1, col2 = st.columns([3, 1])
with col1:
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.text(f"Last refreshed: {last_updated}")
with col2:
    if st.button("🔄 Refresh Now"):
        st.rerun()

# Auto-refresh every 60 seconds
placeholder = st.empty()

with placeholder.container():
    st.dataframe(data)

# Auto-refresh at the top of each minute (like cron)
current_time = datetime.now().replace(second=5)
seconds_until_next_minute = 65 - current_time.second
time.sleep(seconds_until_next_minute)
st.rerun()