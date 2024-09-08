import datetime
import os
from dotenv import load_dotenv

load_dotenv()

from dash import Dash, html, dcc, callback, Output, Input
from sqlalchemy import create_engine
import altair as alt
import dash_vega_components as dvc
import pandas as pd
import plotly.express as px

conn = create_engine(
    "postgresql+psycopg2://{user}:{pssw}@{host}/{dbnm}".format(
        user=os.getenv("CUBEJS_SQL_USER"),
        pssw=os.getenv("CUBEJS_SQL_PASSWORD"),
        host=os.getenv("CUBEJS_SQL_HOST"),
        dbnm=os.getenv("CUBEJS_SQL_DB_NAME"),
    )
)

df = pd.read_sql_query(
    """
    SELECT
        dataset,
        status,
        DATE_TRUNC('day', metrics.run_time) AS day,
        MEASURE(metrics.count) AS count
    FROM metrics
    WHERE
        metrics.run_time > CURRENT_TIMESTAMP() - interval '90 days'
        AND metrics.run_time < CURRENT_TIMESTAMP()
    GROUP BY 1,2,3
    LIMIT 5000
    """,
    con=conn,
)

app = Dash()

all_values = "All Values"


app.layout = [
    html.H1(children="Checks by Dataset", style={"textAlign": "center"}),
    dcc.Dropdown(
        [all_values] + list(df.dataset.unique()),
        all_values,
        id="dropdown-selection",
    ),
    dvc.Vega(id="altair-d-chart", opt={"renderer": "svg", "actions": False}, spec={}),
]


@callback(
    Output(component_id="altair-d-chart", component_property="spec"),
    Input(component_id="dropdown-selection", component_property="value"),
)
def update_graph(value):
    if value == all_values:
        dff = df
    else:
        dff = df[df.dataset == value]

    chart = (
        alt.Chart(dff)
        .mark_bar(size=10)
        .encode(
            x=alt.X(
                "day:T",
                title="Execution Date",
                axis=alt.Axis(format="%b %d %Y"),
                scale=alt.Scale(
                    domain=[
                        (
                            datetime.datetime.now() - datetime.timedelta(days=91)
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
        .properties(width="container")
        .configure_axis(grid=False)
        .configure_view(stroke=None)
        # .interactive()
    )
    return chart.to_dict()


if __name__ == "__main__":
    app.run(debug=True)
