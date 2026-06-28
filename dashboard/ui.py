from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

#  Rule configuration maps

RULE_ORDER = [
    "HIGH_VALUE", "RAPID_ACTIVITY", "GEO_ANOMALY",
    "REPEATED_HIGH_VALUE", "CRYPTO_RISK", "VELOCITY_SPIKE",
]

RULE_COLORS = {
    "HIGH_VALUE": "#ff6b6b",
    "RAPID_ACTIVITY": "#ffa502",
    "GEO_ANOMALY": "#2ed573",
    "REPEATED_HIGH_VALUE": "#1e90ff",
    "CRYPTO_RISK": "#a55eea",
    "VELOCITY_SPIKE": "#ff6348",
}

RULE_LABELS = {
    "HIGH_VALUE": "High Value",
    "RAPID_ACTIVITY": "Rapid Activity",
    "GEO_ANOMALY": "Geo Anomaly",
    "REPEATED_HIGH_VALUE": "Repeated High Value",
    "CRYPTO_RISK": "Crypto Risk",
    "VELOCITY_SPIKE": "Velocity Spike",
}

DETAIL_LABELS = {
    "amount": "Amount",
    "transaction_id": "Transaction",
    "location": "Location",
    "method": "Method",
    "event_time": "Event time",
    "window_start": "Window start",
    "window_end": "Window end",
    "tx_count": "Transaction count",
    "city_count": "Cities",
    "high_count": "High-value tx",
    "total_spent": "Total spent",
}

#  Formatting helpers


def fmt_ts(ts_str: Any) -> str:
    """ISO 8601 to DD/MM/YYYY HH:MM:SS."""
    if not ts_str or pd.isna(ts_str):
        return "?"
    try:
        return pd.Timestamp(str(ts_str).replace("Z", "+00:00")).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(ts_str)[:19]


def fmt_value(v: Any) -> str:
    """Format a detail value: floats to currency, ISO strings to timestamp."""
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, str) and ("T" in v or "Z" in v):
        return fmt_ts(v)
    return str(v)


def fmt_details(details: Any) -> list[str]:
    """Turn a detail dict into a list of '`Label`: **value**' lines."""
    if not isinstance(details, dict):
        return [str(details)]
    lines = []
    for k, v in details.items():
        label = DETAIL_LABELS.get(k, k)
        lines.append(f"`{label}`: **{fmt_value(v)}**")
    return lines


def build_donut(df: pd.DataFrame) -> go.Figure:
    """Return a Plotly donut figure (alerts by rule)."""
    counts = df["rule"].value_counts()
    colors = [RULE_COLORS.get(r, "#999") for r in counts.index]
    labels = [RULE_LABELS.get(r, r) for r in counts.index]

    fig = go.Figure(go.Pie(
        labels=labels, values=counts.values, hole=0.6,
        textinfo="label+value", textfont=dict(size=12),
        marker=dict(colors=colors, line=dict(color="#1a1a2e", width=2)),
    ))
    fig.update_layout(
        annotations=[dict(
            text=f"<b>{len(df)}</b><br><span style='font-size:12px'>total</span>",
            x=0.5, y=0.5, font_size=22, showarrow=False,
        )],
        height=320, margin=dict(t=10, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)", showlegend=False,
    )
    return fig


def build_histogram(df: pd.DataFrame) -> go.Figure | None:
    """Return a stacked bar chart, alerts per minute, coloured by rule."""
    ts = pd.to_datetime(df["alert_ts"], utc=True, errors="coerce").dropna()
    if ts.empty:
        return None
    df = df.copy()
    df["minute"] = ts.dt.floor("1min")

    # Pivot: rows = minute, columns = rule, values = count
    pivot = df.pivot_table(
        index="minute", columns="rule", aggfunc="size", fill_value=0
    )

    fig = go.Figure()
    for rule in RULE_ORDER:
        if rule not in pivot.columns:
            continue
        label = RULE_LABELS.get(rule, rule)
        color = RULE_COLORS.get(rule, "#999")
        fig.add_trace(go.Bar(
            x=pivot.index,
            y=pivot[rule],
            name=label,
            marker_color=color,
            hovertemplate=f"{label}<br>%{{x}}: <b>%{{y}}</b><extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom",
                    y=1.02, xanchor="left", x=0),
        xaxis=dict(title=None, tickformat="%H:%M"),
        yaxis=dict(title=None),
    )
    return fig


def render_rule_card(rule_name: str, rule_df: pd.DataFrame) -> None:
    """Render a single rule card showing the 3 most recent alerts."""
    color = RULE_COLORS.get(rule_name, "#999")
    label = RULE_LABELS.get(rule_name, rule_name)

    st.markdown(
        f"<span style='color:{color};font-weight:bold;font-size:1.1em'>"
        f"{label}</span> - "
        f"<span style='color:#888'>{len(rule_df)} alerts</span>",
        unsafe_allow_html=True,
    )

    if rule_df.empty:
        st.caption("_No alerts yet_")
        return

    for _, row in rule_df.head(3).iterrows():
        for line in fmt_details(row.get("details", {})):
            st.markdown(line)
        st.caption(
            f"🕐 {fmt_ts(row.get('alert_ts'))}  •  "
            f"user `{row.get('user_id', '?')}`"
        )
        st.markdown("<hr style='margin:4px 0;opacity:0.3'>",
                    unsafe_allow_html=True)
