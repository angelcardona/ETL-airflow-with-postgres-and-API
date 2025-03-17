# app.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.load import engine


st.set_page_config(page_title="SPY Terminal", layout="wide", initial_sidebar_state="expanded")

# Sidebar
st.sidebar.title(" SPY Financial Terminal")
st.sidebar.markdown("SPX")
st.sidebar.divider()
# Datos
with engine.connect() as conn:
    df = pd.read_sql("SELECT * FROM spx_data ORDER BY date", conn)
    df['date'] = pd.to_datetime(df['date'])

# KPIs estilo terminal
col1, col2, col3, col4 = st.columns(4)
last_row = df.iloc[-1]
prev_row = df.iloc[-2]

col1.metric("Último Cierre", f"${last_row['close']:.2f}", f"{last_row['close'] - prev_row['close']:+.2f}")
col2.metric("Máximo", f"${last_row['high']:.2f}")
col3.metric("Mínimo", f"${last_row['low']:.2f}")
col4.metric("Volumen", f"{last_row['volume']:,}")

# Gráfico de velas con volumen estilo Bloomberg
fig = go.Figure()

fig.add_trace(go.Candlestick(
    x=df['date'],
    open=df['open'],
    high=df['high'],
    low=df['low'],
    close=df['close'],
    name='SPY'
))

fig.update_layout(
    title="SPY Candlestick Chart",
    yaxis_title="Price (USD)",
    xaxis_title="Date",
    xaxis_rangeslider_visible=False,
    template="plotly_dark",
    height=600,
    margin=dict(l=10, r=10, t=50, b=10),
)

# Gráfico de volumen
fig.add_trace(go.Bar(
    x=df['date'],
    y=df['volume'],
    name="Volume",
    marker_color='rgba(158,202,225,0.5)',
    yaxis='y2',
    opacity=0.4
))

fig.update_layout(
    yaxis2=dict(
        overlaying='y',
        side='right',
        showgrid=False,
        title="Volumen"
    ),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)

st.plotly_chart(fig, use_container_width=True)

# Tabla
with st.expander("Ver tabla completa"):
    st.dataframe(df.tail(100), use_container_width=True)




