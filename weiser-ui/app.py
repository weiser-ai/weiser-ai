import os
import datetime
from typing import Any
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


from sqlalchemy import create_engine
import altair as alt
import pandas as pd

conn = create_engine(
    "postgresql+psycopg2://{user}:{pssw}@{host}/{dbnm}".format(
        user=os.getenv("CUBEJS_SQL_USER"),
        pssw=os.getenv("CUBEJS_SQL_PASSWORD"),
        host=os.getenv("CUBEJS_SQL_HOST"),
        dbnm=os.getenv("CUBEJS_SQL_DB_NAME"),
    )
)


@st.cache_data
def load_runs_by_status(last_n_days: int = 90) -> pd.DataFrame:
    return pd.read_sql_query(
        f"""
        SELECT
            status,
            DATE_TRUNC('day', metrics.run_time) AS day,
            MEASURE(metrics.count) AS count
        FROM metrics
        WHERE
            metrics.run_time > CURRENT_TIMESTAMP() - interval '{last_n_days} days'
            AND metrics.run_time < CURRENT_TIMESTAMP()
        GROUP BY 1,2
        LIMIT 5000
        """,
        con=conn,
    )


st.title("Data Quality Checks")

data_load_state = st.text("Loading data...")

last_n_days = 90
data = load_runs_by_status(last_n_days)

data_load_state.text("Loading data...done!")

st.subheader(f"Checks by Status last {last_n_days} days")

chart = (
    alt.Chart(data)
    .mark_bar(size=10)
    .encode(
        x=alt.X(
            "day:T",
            title="Execution Date",
            axis=alt.Axis(format="%b %d %Y"),
            scale=alt.Scale(
                domain=[
                    (
                        datetime.datetime.now()
                        - datetime.timedelta(days=last_n_days + 1)
                    ).strftime("%Y-%m-%dT%H:%M:%S"),
                    (datetime.datetime.now()).strftime("%Y-%m-%dT%H:%M:%S"),
                ]
            ),
        ),
        y=alt.Y("count:Q", title="Checks Count"),
        color=alt.Color("status").scale(
            domain=["success", "fail"], range=["#2dcf43", "#e63e44"]
        ),
        tooltip=["status", "count"],
    )
    # .properties(width="container")
    # .configure_axis(grid=False)
    # .configure_view(stroke=None)
    # .interactive()
)

st.altair_chart(chart)
