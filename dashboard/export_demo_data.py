"""
Freeze the Gold tables to Parquet so the dashboard runs without Databricks.
Run from the repo root, with .streamlit/secrets.toml present:
    python dashboard/export_demo_data.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))   # so `import queries` works

import queries

OUT = Path("data/demo")

LOADERS = {
    "gold_conversion_funnel": queries.load_conversion_funnel,
    "gold_pipeline_velocity": queries.load_pipeline_velocity,
    "gold_rep_performance": queries.load_rep_performance,
    "gold_revenue_forecast": queries.load_revenue_forecast,
}

OUT.mkdir(parents=True, exist_ok=True)
for name, loader in LOADERS.items():
    df = loader()
    # Categorical stage columns don't round-trip cleanly; store as plain text.
    for col in df.select_dtypes("category").columns:
        df[col] = df[col].astype(str)
    path = OUT / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"{path}  {len(df)} rows, {path.stat().st_size / 1024:.1f} KB")