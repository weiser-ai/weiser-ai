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
all_checks_label = "All Checks"


@st.cache_data(ttl=3600)
def load_runs_by_status(
    start_date: datetime.date, end_date: datetime.date
) -> pd.DataFrame:
    limit = 5000
    last_row = pd.read_sql_query(
        f"""
        SELECT
            status,
            DATE_TRUNC('day', metrics.run_time) AS day,
            MEASURE(metrics.count) AS count
        FROM metrics
        WHERE
            metrics.run_time >= '{start_date.strftime("%Y-%m-%d")} 00:00:00'::timestamp
            AND metrics.run_time <= '{end_date.strftime("%Y-%m-%d")} 23:59:59'::timestamp
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
            metrics.run_time >= '{start_date.strftime("%Y-%m-%d")} 00:00:00'::timestamp
            AND metrics.run_time <= '{end_date.strftime("%Y-%m-%d")} 23:59:59'::timestamp
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
            threshold_list,
            condition
        FROM metrics
        GROUP BY 1,2,3,4,5,6,7,8,9
        LIMIT {limit}
        """,
        con=conn,
    )
    # Coalesce the threshold_list and threshold values
    checks_details["threshold"] = checks_details["threshold"].combine_first(
        checks_details["threshold_list"]
    )
    checks_details.drop(columns=["threshold_list"], inplace=True)

    check_runs = pd.read_sql_query(
        f"""
        SELECT
            id,
            status,
            DATE_TRUNC('day', metrics.run_time) AS day,
            MEASURE(actual_value) AS actual_value,
            MEASURE(count_fail) AS count_fail,
            MEASURE(last_value) AS last_value
        FROM metrics
        WHERE
            1=1
            AND run_time >= '{start_date.strftime("%Y-%m-%d")} 00:00:00'::timestamp
            AND run_time <= '{end_date.strftime("%Y-%m-%d")} 23:59:59'::timestamp
        GROUP BY 1,2,3
        LIMIT {limit}
        """,
        con=conn,
    )
    check_runs = pd.merge(check_runs, checks_details, on="id")
    failed_checks_data_day = check_runs[check_runs["status"] == "fail"]

    failed_checks_data = (
        failed_checks_data_day.groupby(
            [
                "id",
                "check_name",
                "check_type",
                "dataset",
                "datasource",
                "last_run_time",
                "threshold",
                "condition",
            ]
        )
        .agg({"last_value": "max", "count_fail": "sum"})
        .reset_index()
    )

    # Create a complete date range for the last n days
    date_range = pd.date_range(start=start_date, end=end_date)

    # Create a DataFrame for the unique id values
    unique_ids = failed_checks_data_day[["id"]].drop_duplicates()

    # Create a DataFrame for the date range
    date_df = pd.DataFrame(date_range, columns=["day"])

    # Perform a cross join between unique_ids and the date range
    unique_ids["key"] = 1
    date_df["key"] = 1
    cross_joined = pd.merge(unique_ids, date_df, on="key").drop("key", axis=1)

    # Ensure the 'day' column in failed_checks_data_day is of type datetime64[ns]
    failed_checks_data_day["day"] = pd.to_datetime(failed_checks_data_day["day"])

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

    # Rename the merged column to fail_history and rename the id column
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
        check_runs,
    )


st.title("Weiser Data Quality Dashboard")

try:
    # Date range selector
    start_date, end_date = st.date_input(
        "Select a date range",
        value=(
            datetime.date.today() - datetime.timedelta(days=90),
            datetime.date.today(),
        ),
    )
except ValueError:
    st.error("Please select a valid date range")
    st.stop()

data_load_state = st.markdown("Loading data... :hourglass_flowing_sand:")
data, last_row, failed_checks, check_runs = load_runs_by_status(start_date, end_date)
if data.empty:
    data_load_state.markdown("Loading data...done! :tada:")
    st.error("No data available for the selected date range")
    st.stop()
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
st.subheader(f"Checks by Status from {start_date} to {end_date}")

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
        ),
        y=alt.Y("count:Q", title="Checks Count"),
        color=alt.Color("status").scale(
            domain=["success", "fail"], range=["#2dcf43", "#e63e44"]
        ),
        tooltip=["day", "status", "count"],
    )
    # For streamlit use use_container_width=True
    # .properties(width="container")
    .configure_axis(grid=False)
    # Streamlit overrides these settings anyways
    # .configure_view(stroke=None)
    # .interactive()
)

st.altair_chart(chart, use_container_width=True)

st.subheader(f"Failed Checks last from {start_date} to {end_date}")


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
        "check_type": "Check Type",
        "dataset": "Dataset",
        "datasource": "Datasource",
        "threshold": "Threshold",
        "condition": "Condition",
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
            "Fail History",  # y_min=0, y_max=1
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
    selected_row = failed_checks.iloc[selection["rows"][0]]
    check_name = selected_row.check_name
    check_id = selected_row.name
    threshold = selected_row.threshold
    st.subheader(f"{check_name} by Day last from {start_date} to {end_date}")
    selected_checks = check_runs[check_runs["id"] == check_id]
    detail_chart = (
        alt.Chart(selected_checks)
        .mark_circle(size=60)
        .encode(
            x=alt.X(
                "day:T",
                title="Execution Date",
            ),
            y=alt.Y("actual_value:Q", title="Actual Value"),
            color=alt.Color("status:N").scale(
                domain=["success", "fail"], range=["#2dcf43", "#e63e44"]
            ),
            tooltip=["day:T", "status:N", "actual_value:Q"],
        )
    )

    # Define the line chart
    line_chart = (
        alt.Chart(selected_checks)
        .mark_line(color="gray")
        .encode(
            x=alt.X(
                "day:T",
                title="Execution Date",
            ),
            y=alt.Y("actual_value:Q", title="Actual Value"),
        )
    )
    charts = [line_chart, detail_chart]
    if "," in str(threshold):
        # TODO: Add threshold line for anomaly detection
        pass
    else:
        threshold_line = (
            alt.Chart(selected_checks)
            .mark_rule(color="red")
            .encode(
                y=alt.datum(float(threshold)),
                tooltip=[alt.Tooltip("threshold:Q", title="Threshold")],
            )
        )
        charts = [threshold_line] + charts
    combined_chart = alt.layer(*charts).configure_axis(grid=False)

    st.altair_chart(combined_chart, use_container_width=True)


# st.dataframe(check_runs)
