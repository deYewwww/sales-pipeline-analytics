"""
Streamlit dashboard for the SME Sales Pipeline Analytics project.
Reads only from the Gold layer via dashboard/queries.py — no Databricks calls here.
Run from the repo root so .streamlit/secrets.toml is found:
            streamlit run dashboard/app.py
"""
import streamlit as st
from queries import (
    load_conversion_funnel, 
    load_pipeline_velocity, 
    load_rep_performance, 
    load_revenue_forecast
)
from datetime import datetime
import plotly.graph_objects as go

# Business rule, kept at module level so it is easy to find and change.
OPEN_STAGES = ["Enquiry", "Quoted"]

BLUE = "#4c78a8"
LIGHT_BLUE = "#9ecae9"
RED = "#e45756"
ORANGE = "#f0a35e"

st.set_page_config(
    page_title="Sales_Pipeline_Analytics", 
    page_icon="📊", 
    layout="wide"
)

# ---- Header ----
left, right = st.columns([4, 1])
with left:
    st.title("📊 SME Sales Pipeline Analytics")
    st.caption("Malaysian B2B sales pipeline · Kafka -> Spark -> Delta Lake -> dbt -> Airflow")
with right:
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ---- Load ----
funnel = load_conversion_funnel()
velocity = load_pipeline_velocity()
reps = load_rep_performance()
forecast = load_revenue_forecast()

# Show when the numbers were actually fetched (cached results can be up to 10 min old)
st.caption(f"Loaded at {datetime.now():%Y-%m-%d %H:%M} · Gold layer refreshes hourly at :15")

# ---- KPIs ----
total_deals = int(reps["total_deals"].sum())
won_deals = int(reps["won_deals"].sum())

# Win rate from the TOTALs, not reps["win_rate"].mean().
# Averaging percentages gives a rep with 1 deal the same weight as one with 50. 
win_rate = won_deals / total_deals * 100 if total_deals else 0

total_weighted = float(forecast["weighted_value"].sum())
open_weighted = float(forecast.loc[forecast["deal_stage"].isin(OPEN_STAGES), "weighted_value"].sum())
open_value = float(reps["open_value"].sum())

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Open weighted value", f"RM {open_weighted:,.0f}", 
          help="Expected revenue from deals still in Enquiry or Quoted (discounted by win probability)")
k2.metric("Total weighted value", f"RM {total_weighted:,.0f}",
          help="Include closed-won and invoiced deals")
k3.metric("Win rate", f"{win_rate:.1f}%", help=f"{won_deals} won of {total_deals} deals")
k4.metric("Open value", f"RM {open_value:,.0f}",
          help="Full value of open deals (no win-probability discount applied)")
k5.metric("Total deals", f"{total_deals:,}")

# ---- Chart row 1: Funnel + Velocity ----
c1, c2 = st.columns(2)

with c1:
    st.subheader("Conversion funnel")
    st.caption("Deals that **ever reached** each stages (cumulative).")
    fig = go.Figure(go.Funnel(
        y=funnel["stage"].astype(str),      # Categorical -> str; Plotly wants plain text
        x=funnel["deal_entered"],
        textinfo="value+percent initial"    # Count plus % of the first stage 
    ))
    fig.update_layout(height=320, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

    worst = funnel.dropna(subset=["conversion_rate"]).nsmallest(1,"conversion_rate").iloc[0]
    st.caption(f"Biggest drop-off: {worst['stage']} -> next stage at {worst['conversion_rate']:.1f}%")

with c2:
    st.subheader("Days per stage")
    # red only for flagged bottleneck. The flag is threshold, so a healthy pipeline shows not red 
    colors = [RED if b else BLUE for b in velocity["is_bottleneck"]]
    fig = go.Figure()
    fig.add_bar(x=velocity["stage"].astype(str), y=velocity["median_days"],
                name="Median days", marker_color=colors)
    # Average as dots: a dot far above its bar means a few stuck deals skew the mean.
    fig.add_scatter(x=velocity["stage"].astype(str), y=velocity["avg_days"],
                    mode="markers", name="Average", marker=dict(size=10, color=ORANGE))
    fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                      yaxis_title="Days", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)
    flagged = int(velocity["is_bottleneck"].sum())
    st.caption(f"Red = median over 1.5x the pipeline average ({flagged} flagged). "
               "Invoiced is absent because deals never leave it. "
               "Deals still sitting in a stage are not counted until they move."
    )


# ---- Chart row 2: forecast ----
st.subheader("Revenue forecast by stage")
st.caption("Deals **currently sitting in** each stage - a different grain from the funnel above.")

open_only = st.toggle("Open deals only", value=True,
                      help="Hide Won and Invoiced, which are already closed.")
plot_df = forecast[forecast["deal_stage"].isin(OPEN_STAGES)] if open_only else forecast

fig = go.Figure()
# Grouped bars: the GAP between them is the risk being discounted away
fig.add_bar(x=forecast["deal_stage"].astype(str), y=forecast["total_value"],
            name="Total value", marker_color="#9ecae9")
fig.add_bar(x=forecast["deal_stage"].astype(str), y=forecast["weighted_value"],
            name="Weighted value", marker_color="#4c78a8")
fig.update_layout(barmode="group", height=340, margin=dict(l=0, r=0, t=10, b=0),
                  yaxis_title="RM", legend=dict(orientation="h", y=1.15))
st.plotly_chart(fig, use_container_width=True)
st.caption(
    f"Weighted = total × win probability per stage (fixed rates, not learned from history). "
    f"Lost deals are excluded: {int(forecast['deal_count'].sum())} of {total_deals} deals shown."
)

# ---- Chart row 3: Rep leaderboard ----
st.subheader("Sales rep performance")
table = reps.rename(columns={
    "deal_owner": "Rep", 
    "total_deals": "Deals", 
    "won_deals": "Won",
    "win_rate": "Win rate %",
    "won_value": "Won value", 
    "open_value": "Open value",
    "avg_deal_value": "Avg deal",
})[["Rep", "Deals", "Won", "Win rate %", "Won value", "Open value", "Avg deal"]]

st.dataframe(
    table,
    hide_index=True,
    use_container_width=True,
    column_config={
        # format= uses printf-style: %.1f one decimal, %d whole number
        "Win rate %": st.column_config.NumberColumn(format="%.1f%%"),
        "Won value": st.column_config.NumberColumn(format="RM %d"),
        "Open value": st.column_config.NumberColumn(format="RM %d"),
        "Avg deal": st.column_config.NumberColumn(format="RM %d")
    },
)
st.caption("Sorted by won value. Win rate on few deals is noisy — read it beside the deal count.")

# ---- Footer ----
st.divider()
st.caption(
    "Source: Delta Lake Gold layer on Databricks · Kafka -> Spark Structured Streaming -> "
    "Bronze/Silver -> dbt -> Airflow (hourly) · Data is simulated for portfolio purposes."
)