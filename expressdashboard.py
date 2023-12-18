#######################################
# Librerías
import pandas as pd
import streamlit as st
from google.cloud import bigquery
import numpy as np
import time
from datetime import datetime
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import random
from annotated_text import annotated_text

# pip install -r requirements.txt

#######################################

#######################################
# WARNING
# Recuerda tener instalado y configurado CLI (GOOGLE CLOUD - BIGQUERY)
# Para esto: 1) Instalar Google Cloud SDK (para m1, mac, o windows)
# 2) Darle permisos en Path
# 3) En terminal, ejecutar gcloud init
# 4) Al final, ejecutar gcloud auth application-default
# 5) Ya podrás usarlo sin problemas
#######################################


#######################################
# PAGE SETUP
#######################################

st.set_page_config(page_title="Sales Dashboard", page_icon=":bar_chart:", layout="wide")


st.header("Author")

"""
```
Esta es la primera versión para el Dashboard de Facturas Express, este actua como un piloto para futuras versiones con Streamlit.
Con ❤️ de BI Team.

_Prototype v0.1
```
"""

st.title("Dashboard Facturas Express")
annotated_text(
    "Este ",
    ("es", "verb"),
    "un ",
    ("dashboard", "adj"),
    ("hecho", "noun"),
    " en ",
    ("Streamlit", "pronoun"),
    ".",
)
#######################################
# QUERY
#######################################
client_bq = bigquery.Client()
project_id = "xepelin-ds-prod"
query = """
WITH Amplitude AS ( 
  SELECT 
    eventType,
    CAST(eventTime AS TIMESTAMP) eventTime,
    EXTRACT(YEAR FROM eventTime) year,
    EXTRACT(MONTH FROM eventTime) month,
    EXTRACT(DAY FROM eventTime) day,
    eventProperties,
  	REPLACE(JSON_EXTRACT(eventProperties, "$.bundle_type"), '"', "") bundleType,
    REPLACE(JSON_EXTRACT(eventProperties, "$.is_express"), '"', "") isExpress,
    REPLACE(JSON_EXTRACT(eventProperties, "$.is_one_click_enabled"), '"', "") isOneClickEnabled,
 	REPLACE(JSON_EXTRACT(eventProperties, "$.is_invoice_intelligence_enabled"), '"', "") isInvoiceIntelligenceEnabled,
    REPLACE(JSON_EXTRACT(eventProperties, "$.order_type"), '"', "") orderType,
    CAST(REPLACE(JSON_EXTRACT(eventProperties, "$.order_id"), '"', "") AS INT64) orderId,
    REPLACE(JSON_EXTRACT(eventProperties, "$.is_personified"), '"', "") isPersonified,
    --REPLACE(JSON_EXTRACT(eventProperties, "$.selected_invoices"), '"', "") selectedInvoices,
    REPLACE(JSON_EXTRACT(eventProperties, "$.business_id"), '"', "") businessId,
    REPLACE(JSON_EXTRACT(eventProperties, "$.business_segment"), '"', "") businessSegment,
    REPLACE(JSON_EXTRACT(eventProperties, "$.country_id"), '"', "") country_amplitude,
    FROM `xepelin-ds-prod.prod_staging_amplitude.Amplitude`
    WHERE 
      TRUE AND  
      REGEXP_CONTAINS(eventType,'^AR Funnel Order Submitted') 
),
OrderStatus AS ( 
  SELECT
    country,
    date, 
    dateTimestamp,
    CAST(orderId AS INT64) orderId, 
    orderStatus,
    --orderInvoiceId,
    orderInvoiceId,
  	isInvoiceFinanced,
    invoiceStatus,
    invoiceAmountFinanced
    FROM `xepelin-ds-prod.prod_int.MasterOrderInvoice`
), 

ss AS (
  SELECT 
  a.*, 
  o.* EXCEPT(orderId),
  CASE 
    WHEN a.isExpress = 'true' AND o.orderInvoiceId IS NOT NULL THEN 1
    ELSE 0
  END AS expressInvoice
  
FROM Amplitude a
  LEFT JOIN OrderStatus o ON a.orderId = o.orderId and o.country=a.country_amplitude 
)

select * FROM ss
WHERE date >= '2023-01-01'
ORDER BY orderId
"""


@st.cache_data(ttl=600)
def run_query(query):
    query_job = client_bq.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows


rows = run_query(query)

df = pd.DataFrame(rows)
# st.write(df.columns)
with st.expander("Data We Working On"):
    st.dataframe(
        df,
        column_config={
            "orderId": st.column_config.NumberColumn(default="int"),
            "year": st.column_config.NumberColumn(format="%d"),
        },
    )

#######################################
# BEGIN DATA VISUALIZATION
#######################################


def plot_metric(
    df,
    column_name,
    label,
    aggregation="sum",
    prefix="",
    suffix="",
    show_graph=False,
    color_graph="",
):
    if aggregation == "sum":
        value = df[column_name].sum()
    elif aggregation == "count":
        value = df[column_name].count()
    elif aggregation == "count_unique":
        value = df[column_name].nunique()
    elif aggregation == "mean":
        value = df[column_name].mean()
    else:
        raise ValueError("Unsupported aggregation method")

    fig = go.Figure()

    fig.add_trace(
        go.Indicator(
            value=value,
            gauge={"axis": {"visible": False}},
            number={
                "prefix": prefix,
                "suffix": suffix,
                "font_color": "green",
                "font.size": 28,
            },
            title={
                "text": label,
                "font_color": "black",
                "font": {"size": 24},
            },
        )
    )

    if show_graph:
        fig.add_trace(
            go.Scatter(
                y=random.sample(range(0, 101), 30),
                hoverinfo="skip",
                fill="tozeroy",
                fillcolor=color_graph,
                line={
                    "color": color_graph,
                },
            )
        )

    fig.update_xaxes(visible=False, fixedrange=True)
    fig.update_yaxes(visible=False, fixedrange=True)
    fig.update_layout(
        # paper_bgcolor="lightgrey",
        margin=dict(t=30, b=0),
        showlegend=False,
        plot_bgcolor="white",
        height=100,
    )

    st.plotly_chart(fig, use_container_width=True)


def plot_gauge(
    df,
    column_name,
    indicator_color,
    indicator_suffix,
    indicator_title,
    goal_value,
    aggregation="sum",
):
    # Calcula el indicador según el método de agregación
    if aggregation == "sum":
        indicator_number = df[column_name].sum()
    elif aggregation == "count":
        indicator_number = df[column_name].count()
    elif aggregation == "count_unique":
        indicator_number = df[column_name].nunique()
    elif aggregation == "mean":
        indicator_number = df[column_name].mean()
    else:
        raise ValueError("Unsupported aggregation method")

    # Calcula el porcentaje respecto a la meta
    percentage = (indicator_number / goal_value) * 100

    # Define los valores y textos de las marcas del eje en términos de porcentaje
    tick_values = [0, 50, 100]  # Porcentajes: 0%, 50%, 100%
    tick_texts = ["0%", "50%", "100%"]

    # Crea el medidor
    fig = go.Figure(
        go.Indicator(
            value=percentage,  # Usa el porcentaje calculado
            mode="gauge+number",
            domain={"x": [0, 1], "y": [0, 1]},
            number={
                "suffix": indicator_suffix,
                "font.size": 26,
            },
            gauge={
                "axis": {
                    "range": [0, 100],
                    "tickvals": tick_values,  # Valores personalizados
                    "ticktext": tick_texts,  # Textos personalizados
                },
                "bar": {"color": indicator_color},
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.75,
                    "value": percentage,  # El valor actual en el medidor
                },
            },
            title={
                "text": indicator_title,
                "font": {"size": 28},
            },
        )
    )
    fig.update_layout(
        height=200,
        margin=dict(l=10, r=10, t=50, b=10, pad=8),
    )
    st.plotly_chart(fig, use_container_width=True)


def count_invoices_plot():
    invoices = duckdb.sql(
        f"""
      WITH invoices AS ( 
        SELECT 
          bundleType, 
          COUNT(orderInvoiceId) AS totalInvoices
        FROM df
        WHERE isPersonified = false
        GROUP BY bundleType
      )
      SELECT * FROM invoices
    """
    ).df()
    st.dataframe(invoices)


def express_plot():
    express_invoices = duckdb.sql(
        f"""
        WITH express_invoices AS ( 
          SELECT 
            bundleType,
            month,
            COUNT(orderInvoiceId) AS totalInvoices
          FROM df
          WHERE isPersonified = false
          AND isExpress = true
          AND isInvoiceIntelligenceEnabled = true
          GROUP BY bundleType, month
        )
        
        SELECT * FROM express_invoices
        ORDER BY month
    """
    ).df()
    st.dataframe(express_invoices)


def express_line_chart():
    express_invoices = duckdb.sql(
        f"""
        WITH express AS ( 
        SELECT 
          bundleType, 
          day,
          COUNT(orderInvoiceId) AS totalInvoices
        FROM df
        WHERE isExpress = true
        AND isInvoiceIntelligenceEnabled = true 
        GROUP BY bundleType, day
        )
        SELECT * FROM express
    """
    ).df()

    st.line_chart(
        express_invoices,
        x="day",
        y="totalInvoices",
        color="bundleType",
        use_container_width=True,
    )


def express_bar_chart():
    express_invoices = duckdb.sql(
        f"""
        WITH express AS ( 
        SELECT 
          bundleType, 
          COUNT(orderInvoiceId) AS totalInvoices
        FROM df
        WHERE isExpress = true
        AND isInvoiceIntelligenceEnabled = true 
        GROUP BY bundleType
        )
        SELECT * FROM express
    """
    ).df()

    fig = px.bar(
        express_invoices,
        x="bundleType",
        y="totalInvoices",
        color="bundleType",
        title="Total EXpress Invoices per Operation Flow",
    )

    fig.update_traces(textposition="inside")
    st.plotly_chart(fig, use_container_width=True)


#######################################
# Layout
#######################################

startDate = pd.to_datetime(df["eventTime"]).min()
endDate = pd.to_datetime(df["eventTime"]).max()

col1, col2 = st.columns((2))

with col1:
    date1 = pd.to_datetime(st.date_input("Start Date", startDate))
with col2:
    date2 = pd.to_datetime(st.date_input("End Date", endDate))

col1, col2, col3 = st.columns((3))

with st.container(border=True):
    with col1:
        options = df["isPersonified"].sort_values().unique().tolist()
        option = st.selectbox("Personified", options)
    with col2:
        options = df["orderType"].sort_values().unique().tolist()
        option = st.selectbox("Order Type", options)
    with col3:
        options = df["country"].sort_values().unique().tolist()
        option = st.selectbox("Country", options)

top_left_column, top_right_column = st.columns((2, 1))
bottom_left_column, bottom_right_column = st.columns(2)

with top_left_column:
    column_1, column_2, column_3, column_4 = st.columns(4)

    with column_1:
        plot_metric(
            df,
            "orderId",  # Replace with your actual column name
            "Orders",
            aggregation="count_unique",
            prefix="",
            suffix="",
            show_graph=True,
            color_graph="rgba(0, 104, 201, 0.2)",
        )
        plot_gauge(
            df,
            "orderId",  # Reemplaza con el nombre real de tu columna
            "#0068C9",
            "%",
            "Current Ratio",
            10000,  # Meta de 10000
            aggregation="count_unique",
        )

    with column_2:
        plot_metric(
            df,
            "orderInvoiceId",  # Replace with your actual column name
            "Invoices",
            aggregation="count_unique",
            prefix="",
            suffix="",
            show_graph=True,
            color_graph="rgba(0, 104, 201, 0.2)",
        )
        plot_gauge(
            df,
            "orderInvoiceId",  # Reemplaza con el nombre real de tu columna
            "#0068C9",
            "%",
            "Current Ratio",
            30000,  # Meta de 30000
            aggregation="count_unique",
        )

    with column_3:
        plot_metric(
            df,
            "expressInvoice",
            "Express Invoices",
            aggregation="sum",
            prefix="",
            suffix="",
            show_graph=True,
            color_graph="rgba(0, 104, 201, 0.2)",
        )

    one_click = duckdb.sql(
        f"""
        WITH oneclick AS ( 
        SELECT 
          orderId
        FROM df
        WHERE bundleType = 'ONE-CLICK'
        )
        SELECT * FROM oneclick
      """
    ).df()

    with column_4:
        plot_metric(
            one_click,
            "orderId",
            "One Click Ops",
            aggregation="count_unique",
            prefix="",
            suffix="",
            show_graph=True,
            color_graph="rgba(0, 104, 201, 0.2)",
        )


with top_right_column:
    express_line_chart()

with bottom_left_column:
    express_bar_chart()

express_plot()
