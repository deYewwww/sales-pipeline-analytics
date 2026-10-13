'''
All Databricks access for the dashboard. app.py never talk to Databricks directly.

Two modes:
  live  — credentials in .streamlit/secrets.toml, queries the SQL Warehouse
  demo  — no credentials, reads the frozen Parquet snapshot in data/demo/
'''
from decimal import Decimal 
from pathlib import Path
import pandas as pd
import streamlit as st
from databricks import sql

GOLD = "sales_pipeline_analytics.gold"

# dashboard/queries.py -> repo root -> data/demo
DEMO_DIR = Path(__file__).parent.parent / "data" / "demo"

# Gold refreshes hourly (:15). 
# 10 min = at most ~ 10 min stale after a refresh and at most 6 warehouse queries per hour per table.
CACHE_TTL_S = 600 

# Business order of the pipeline. Alphabetical would give Enquiry, Invoiced, Quoted, Won — wrong.
STAGE_ORDER = ["Enquiry", "Quoted", "Won", "Invoiced"]


def demo_mode() -> bool:
    """True when no Databricks credentials are configured."""
    try:
        return "databricks" not in st.secrets
    except Exception:
        # No secrets.toml at all raises instead of returning empty.
        return True


@st.cache_data(ttl=CACHE_TTL_S, show_spinner="Loading data...")
def run_query(query: str, table: str) -> pd.DataFrame:
    """Run a Gold-layer query, or read its frozen snapshot in demo mode."""
    if demo_mode():
        return pd.read_parquet(DEMO_DIR / f"{table}.parquet")
    
    cfg = st.secrets["databricks"]
    # Fresh Connectiom each time: a long-lived one goes stale when warehouse stops.
    # With closes it even if the query fails.
    with sql.connect(
        server_hostname=cfg["server_hostname"],
        http_path=cfg["http_path"],
        access_token=cfg["access_token"]
    ) as conn, conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetchall_arrow().to_pandas()

    # For decimal column arrive as Python Decimal objects (pandas "object dtype").
    # Convert to float so chart and math work.
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].astype(float)
    return df


def _order_stages(df: pd.DataFrame, col:str) -> pd.DataFrame:
     """Sort by pipeline order. Categorical keeps Plotly from re-sorting alphabetically."""
     missing = set(df[col].dropna().astype(str)) - set(STAGE_ORDER)
     if missing:
        # Fail loudly: Categorical would turn an unknown stage into NaN and silently drop row.
        raise ValueError(f"Unknown stage in {col}: {sorted(missing)}")
     df[col] = pd.Categorical(df[col], categories=STAGE_ORDER, ordered=True)
     return df.sort_values(col).reset_index(drop=True)


# One function per Gold table, never SELECT * 
# if dbt add or rename a column, the error show up here, not broken chart 
def load_conversion_funnel() -> pd.DataFrame:
    df = run_query(f"""
        SELECT 
            stage, 
            deal_entered,
            next_stage_entered,
            conversion_rate
        FROM {GOLD}.gold_conversion_funnel
    """, "gold_conversion_funnel")
    return _order_stages(df, "stage")

def load_pipeline_velocity() -> pd.DataFrame:
    df = run_query(f"""
        SELECT 
            stage, 
            deal_count,
            avg_days,
            median_days,
            min_days,
            max_days,
            is_bottleneck
        FROM {GOLD}.gold_pipeline_velocity
    """, "gold_pipeline_velocity")
    return _order_stages(df, "stage")

def load_rep_performance() -> pd.DataFrame:
    return run_query(f"""
        SELECT 
            deal_owner,
            total_deals,
            won_deals,
            win_rate,
            total_value,
            won_value,
            lost_value,
            open_value,
            avg_deal_value
        FROM {GOLD}.gold_rep_performance
        ORDER BY won_value DESC
    """, "gold_rep_performance")

def load_revenue_forecast() -> pd.DataFrame:
        df = run_query(f"""
            SELECT 
                deal_stage,
                deal_count,
                total_value,
                win_probability,
                weighted_value
            FROM {GOLD}.gold_revenue_forecast
        """, "gold_revenue_forecast")
        return _order_stages(df, "deal_stage")


