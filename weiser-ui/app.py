import os
from dotenv import load_dotenv

load_dotenv()

from dash import Dash, html, dcc, callback, Output, Input
from sqlalchemy import create_engine
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
        metrics.check_name AS check_name,
        metrics.dataset AS dataset,
        DATE_TRUNC('day', metrics.run_time) AS day,
        MEASURE(metrics.count) AS count
    FROM metrics
    WHERE
        metrics.run_time > CURRENT_TIMESTAMP() - interval '30 days'
        AND metrics.run_time < CURRENT_TIMESTAMP()
    GROUP BY 1,2,3
    LIMIT 5000
    """,
    con=conn,
)

app = Dash()

app.layout = [
    html.H1(children="Checks by Dataset", style={"textAlign": "center"}),
    dcc.Dropdown(df.dataset.unique(), "metrics", id="dropdown-selection"),
    dcc.Graph(id="graph-content"),
]


@callback(Output("graph-content", "figure"), Input("dropdown-selection", "value"))
def update_graph(value):
    dff = df[df.dataset == value]
    return px.bar(dff, x="day", y="count", color="check_name")


if __name__ == "__main__":
    app.run(debug=True)
