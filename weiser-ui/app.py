import os
import datetime
from typing import Any
import streamlit as st
from dotenv import load_dotenv
from rich import print

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

st.set_page_config(layout="wide")


@st.cache_data(ttl=3600)
def load_runs_by_status(last_n_days: int, all_checks_label: str) -> pd.DataFrame:
    limit = 5000
    today = datetime.date.today().strftime("%Y-%m-%d")
    last_row = pd.read_sql_query(
        f"""
        SELECT
            status,
            DATE_TRUNC('day', metrics.run_time) AS day,
            MEASURE(metrics.count) AS count
        FROM metrics
        WHERE
            metrics.run_time >= '{today} 00:00:00'::timestamp - interval '{last_n_days} days'
            AND metrics.run_time <= '{today} 23:59:59'::timestamp
        GROUP BY 1,2
        ORDER BY 2 DESC
        LIMIT 2
        """,
        con=conn,
    )
    last_row = last_row[last_row["day"] == last_row["day"].max()]

    data = pd.read_sql_query(
        f"""
        SELECT
            check_name,
            status,
            DATE_TRUNC('day', metrics.run_time) AS day,
            MEASURE(metrics.count) AS count
        FROM metrics
        WHERE
            metrics.run_time >= '{today} 00:00:00'::timestamp - interval '{last_n_days} days'
            AND metrics.run_time <= '{today} 23:59:59'::timestamp
        GROUP BY 1,2,3
        ORDER BY 3 ASC
        LIMIT {limit}
        """,
        con=conn,
    )
    all_checks_data = data.groupby(["status", "day"])["count"].sum().reset_index()
    all_checks_data["check_name"] = all_checks_label
    all_checks_data = all_checks_data[data.columns]
    data = pd.concat([all_checks_data, data])

    checks_details = pd.read_sql_query(
        f"""
        SELECT
            id,
            check_name,
            check_type,
            dataset,
            datasource,
            last_run_time,
            threshold,
            threshold_list
        FROM metrics
        GROUP BY 1,2,3,4,5,6,7,8
        LIMIT {limit}
        """,
        con=conn,
    )

    failed_checks_data_day = pd.read_sql_query(
        f"""
        SELECT
            id,
            DATE_TRUNC('day', metrics.run_time) AS day,
            MEASURE(actual_value) AS actual_value,
            MEASURE(count_fail) AS count_fail,
            MEASURE(last_value) AS last_value
        FROM metrics
        WHERE
            status = 'fail'
            AND run_time >= '{today} 00:00:00'::timestamp - interval '{last_n_days} days'
            AND run_time <= '{today} 23:59:59'::timestamp
        GROUP BY 1,2
        HAVING MEASURE(count_fail) > '0'
        LIMIT {limit}
        """,
        con=conn,
    )
    failed_checks_data_day = pd.merge(failed_checks_data_day, checks_details, on="id")

    failed_checks_data = (
        failed_checks_data_day.groupby(
            ["id", "check_name", "check_type", "dataset", "datasource", "last_run_time"]
        )
        .agg({"count_fail": "sum", "last_value": "max"})
        .reset_index()
    )

    # Create a complete date range for the last n days
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=last_n_days)
    date_range = pd.date_range(start=start_date, end=end_date)

    # Create a DataFrame for the unique id values
    unique_ids = failed_checks_data_day[["id"]].drop_duplicates()

    # Create a DataFrame for the date range
    date_df = pd.DataFrame(date_range, columns=["day"])

    # Perform a cross join between unique_ids and the date range
    unique_ids["key"] = 1
    date_df["key"] = 1
    cross_joined = pd.merge(unique_ids, date_df, on="key").drop("key", axis=1)

    # Perform an outer join between failed_checks_data_day and the cross-joined date_df
    failed_checks_count_day = pd.merge(
        cross_joined,
        failed_checks_data_day[["id", "day", "count_fail"]],
        on=["id", "day"],
        how="left",
    )

    # Coalesce the values to zero where they do not exist
    failed_checks_count_day["count_fail"].fillna(0, inplace=True)

    # Aggregate fail_history for failed_checks_data
    fail_history = (
        failed_checks_count_day.groupby(["id"])
        .agg({"count_fail": lambda x: list(x)})
        .reset_index()
    )

    # Merge the fail_history with failed_checks_data
    failed_checks_data = pd.merge(
        failed_checks_data,
        fail_history,
        on=[
            "id",
        ],
        how="left",
        suffixes=("", "_history"),
    )

    # Rename the merged column to fail_history and drop the id column
    failed_checks_data.rename(
        columns={"count_fail_history": "fail_history"}, inplace=True
    )
    # set index to id
    failed_checks_data.set_index("id", inplace=True)

    # failed_checks_data.drop(columns=["id"], inplace=True)

    return (
        data,
        last_row,
        failed_checks_data,
        failed_checks_data_day,
        failed_checks_count_day,
    )


st.title("Weiser Data Quality Dashboard")
last_n_days = 90
all_checks_label = "All Checks"
data_load_state = st.markdown("Loading data... :hourglass_flowing_sand:")
data, last_row, failed_checks, failed_checks_day, cross_joined = load_runs_by_status(
    last_n_days, all_checks_label
)
last_run_date = last_row["day"].values[0]
success_count = (
    last_row[last_row["status"] == "success"]["count"].values[0]
    if last_row[last_row["status"] == "success"]["count"].any()
    else 0
)
fail_count = (
    last_row[last_row["status"] == "fail"]["count"].values[0]
    if last_row[last_row["status"] == "fail"]["count"].any()
    else 0
)
check_names = sorted(data["check_name"].unique().tolist())

data_load_state.markdown("Loading data...done! :tada:")

st.markdown(
    """### Last Run Summary
 - :calendar: **Date**: {0}
 - :white_check_mark: **Success Count**: {1}
 - :x: **Fail Count**: {2}""".format(
        last_run_date.astype("datetime64[us]")
        .astype(datetime.datetime)
        .strftime("%Y-%m-%d"),
        success_count,
        fail_count,
    ),
)
st.subheader(f"Checks by Status last {last_n_days} days")

# Define the multiselect widget
if "selected_checks" not in st.session_state:
    st.session_state.selected_checks = [all_checks_label]

selected_checks = st.multiselect(
    "Checks:",
    check_names,
    # default=st.session_state.selected_checks,
    key="selected_checks",
)

chart = (
    alt.Chart(data[data["check_name"].isin(selected_checks)])
    .mark_bar(size=10)
    .encode(
        x=alt.X(
            "day:T",
            title="Execution Date",
            # Streamlit overrides these settings anyways
            # axis=alt.Axis(format="%b %d %Y"),
            # scale=alt.Scale(
            #     domain=[
            #         (
            #             datetime.datetime.now()
            #             - datetime.timedelta(days=last_n_days + 1)
            #         ).strftime("%Y-%m-%dT%H:%M:%S"),
            #         (datetime.datetime.now()).strftime("%Y-%m-%dT%H:%M:%S"),
            #     ]
            # ),
        ),
        y=alt.Y("count:Q", title="Checks Count"),
        color=alt.Color("status").scale(
            domain=["success", "fail"], range=["#2dcf43", "#e63e44"]
        ),
        tooltip=["status", "count"],
    )
    # For streamlit use use_container_width=True
    # .properties(width="container")
    .configure_axis(grid=False)
    # Streamlit overrides these settings anyways
    # .configure_view(stroke=None)
    # .interactive()
)

st.altair_chart(chart, use_container_width=True)

st.subheader(f"Failed Checks last {last_n_days} days")


def select_callback(*args, **kwargs):
    print(st.session_state.failed_checks)
    selection = st.session_state.failed_checks["selection"]
    if selection["rows"]:
        selected_row = selection["rows"][0]
        check_name = failed_checks.iloc[selected_row].check_name
        print(check_name)
        st.session_state.selected_checks = [check_name]
    else:
        st.session_state.selected_checks = [all_checks_label]


# Display the DataFrame for failed_checks_data with the specified column configurations
st.dataframe(
    failed_checks,
    column_config={
        "check_name": "Check Name",
        "count_fail": st.column_config.NumberColumn(
            "Count Fail",
            help="Number of failed checks",
            format="%d",
        ),
        "last_value": st.column_config.NumberColumn(
            "Last Value",
            help="Last recorded value",
            format="%d",
        ),
        "last_run_time": st.column_config.DateColumn("Last Run Time"),
        "fail_history": st.column_config.LineChartColumn(
            f"Fail History (past {last_n_days} days)",  # y_min=0, y_max=1
        ),
    },
    hide_index=True,
    use_container_width=True,
    selection_mode="single-row",
    on_select=select_callback,
    key="failed_checks",
)

selection = st.session_state.failed_checks["selection"]
if selection["rows"]:
    selected_row = selection["rows"][0]
    check_name = failed_checks.iloc[selected_row].check_name
    st.subheader(f"{check_name} by Day last {last_n_days} days")

    # TODO - Add a chart for the selected check

# st.dataframe(failed_checks_day)
# st.dataframe(cross_joined)
