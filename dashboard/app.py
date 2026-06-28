import time

import streamlit as st

from db import load_alerts, db_count, DB_PATH
from ui import (
    RULE_ORDER, build_donut, build_histogram, render_rule_card,
)

#  Page setup

st.set_page_config(page_title="FraudCore Dashboard",
                   page_icon="🛡️", layout="wide")
st.title("FraudCore Real-Time Dashboard")

kpi_placeholder = st.empty()
charts_placeholder = st.empty()
st.divider()
feed_placeholder = st.empty()
st.divider()
footer_placeholder = st.empty()

_render = 0

#  Main render loop

while True:
    _render += 1
    df = load_alerts(5000)
    total = db_count()

    # KPIs
    with kpi_placeholder.container():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total alerts", f"{total:,}")
        c2.metric("Unique users",
                  df["user_id"].nunique() if not df.empty else 0)
        c3.metric("Rule types",   df["rule"].nunique() if not df.empty else 0)
        c4.metric("Worker", "running" if DB_PATH.exists() else "No db")

    # Charts
    with charts_placeholder.container():
        left, right = st.columns(2)

        with left:
            st.subheader("Alerts by rule type (last 5000)")
            if df.empty:
                st.info("Waiting for alerts")
            else:
                st.plotly_chart(
                    build_donut(df),
                    width="stretch",
                    config={"displayModeBar": False},
                    key=f"donut_{_render}",
                )

        with right:
            st.subheader("Alert rate (per minute)")
            if df.empty or "alert_ts" not in df.columns:
                st.info("Waiting for alerts")
            else:
                fig = build_histogram(df)
                if fig is None:
                    st.info("No valid timestamps yet")
                else:
                    st.plotly_chart(fig, width="stretch",
                                    config={"displayModeBar": False},
                                    key=f"hist_{_render}")

    # Live feed
    with feed_placeholder.container():
        st.subheader("Live alert feed by rule")
        if df.empty:
            st.info("No alerts yet, start the worker + producer + Spark.")
        else:
            for row_start in range(0, 6, 2):
                left_card, right_card = st.columns(2)
                for offset, card_col in enumerate([left_card, right_card]):
                    idx = row_start + offset
                    if idx >= 6:
                        break
                    rule_name = RULE_ORDER[idx]
                    with card_col:
                        render_rule_card(
                            rule_name, df[df["rule"] == rule_name])
                        st.markdown("---")

    # Footer
    with footer_placeholder.container():
        st.caption("Powered by FraudCore, disclaimer: synthetic data.")

    time.sleep(2)
