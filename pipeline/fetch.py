import io
import logging
import os
from datetime import datetime, timezone

import boto3
import duckdb
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns

matplotlib.use("Agg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — all values come from environment variables set in the
# CronJob spec. EIA_API_KEY is injected from a Kubernetes Secret; the rest
# are plain env vars you set directly in pipeline-job.yaml.
# ---------------------------------------------------------------------------
API_KEY    = os.environ["EIA_API_KEY"]
RESPONDENT = os.environ.get("EIA_RESPONDENT", "PJM")
S3_BUCKET  = os.environ["S3_BUCKET"]
S3_REGION  = os.environ.get("S3_REGION", "us-east-1")

EIA_URL     = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
PARQUET_KEY = "data.parquet"
PARQUET_S3  = f"s3://{S3_BUCKET}/{PARQUET_KEY}"   # resolved once at startup

# Stacking order for the area chart: base-load sources on the bottom,
# variable renewables on top.
FUEL_ORDER = ["NUC", "COL", "NG", "OIL", "WAT", "WND", "SUN", "GEO", "BIO", "OTH"]
FUEL_LABELS = {
    "NUC": "Nuclear",   "COL": "Coal",       "NG":  "Natural Gas",
    "OIL": "Oil",       "WAT": "Hydro",      "WND": "Wind",
    "SUN": "Solar",     "GEO": "Geothermal", "BIO": "Biomass",
    "OTH": "Other",
}
FUEL_COLORS = {
    "NUC": "#7B2D8B",  "COL": "#444444",  "NG":  "#E8A838",
    "OIL": "#8B4513",  "WAT": "#4682B4",  "WND": "#5CB85C",
    "SUN": "#FFD700",  "GEO": "#CD853F",  "BIO": "#6B8E23",
    "OTH": "#AAAAAA",
}


# ---------------------------------------------------------------------------
# DuckDB setup — called once; returns a connection with httpfs loaded and
# S3 configured to use the EC2 instance profile (no credentials needed).
# ---------------------------------------------------------------------------
def setup_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # CREDENTIAL_CHAIN walks the standard AWS chain: env vars → ~/.aws/credentials
    # → EC2 instance metadata. On EC2 with an IAM role attached this picks up the
    # instance profile automatically — no keys or credentials file needed.
    con.execute(f"""
        CREATE SECRET s3_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            REGION '{S3_REGION}'
        )
    """)
    log.info("DuckDB httpfs ready (region=%s, provider=credential_chain)", S3_REGION)
    return con


# ---------------------------------------------------------------------------
# Step 1 — Fetch from EIA
# ---------------------------------------------------------------------------
def fetch_eia() -> tuple[str, list[dict]]:
    """Return (period_string, list_of_fuel_type_records) for the latest hour."""
    # requests doesn't support repeated bracket-notation keys natively,
    # so we pass params as a list of tuples.
    params = [
        ("api_key",              API_KEY),
        ("frequency",            "hourly"),
        ("data[0]",              "value"),
        ("facets[respondent][]", RESPONDENT),
        ("sort[0][column]",      "period"),
        ("sort[0][direction]",   "desc"),
        ("length",               25),   # more than enough for all fuel types in one hour
    ]
    resp = requests.get(EIA_URL, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json()["response"]["data"]
    if not rows:
        raise ValueError("EIA API returned an empty dataset — check respondent code and API key")

    # The API returns N most-recent records across possibly multiple periods.
    # Isolate only records belonging to the single most-recent period.
    latest_period = rows[0]["period"]
    period_rows = [r for r in rows if r["period"] == latest_period]
    log.info("Fetched %d fuel-type records for period %s (%s)", len(period_rows), latest_period, RESPONDENT)
    return latest_period, period_rows


# ---------------------------------------------------------------------------
# Step 2 — Load history from S3 into DuckDB and append new records
# ---------------------------------------------------------------------------
def load_and_store(con: duckdb.DuckDBPyConnection, period: str, rows: list[dict]) -> None:
    """Read the accumulation Parquet directly from S3 via httpfs (no local
    download), then insert the new hour's records.

    A PRIMARY KEY on (period, fueltype) prevents duplicates if the job ever
    fires twice in the same hour. The Parquet file in S3 is the durable store —
    DuckDB is in-memory because the CronJob pod is ephemeral.
    """
    con.execute("""
        CREATE TABLE readings (
            period     TIMESTAMP,
            respondent VARCHAR,
            fueltype   VARCHAR,
            value_mwh  DOUBLE,
            fetched_at TIMESTAMP,
            PRIMARY KEY (period, fueltype)
        )
    """)

    # Stream the existing Parquet directly from S3 — no temp file needed.
    # On the first ever run the file won't exist; catch that and start fresh.
    try:
        con.execute(f"INSERT INTO readings SELECT * FROM read_parquet('{PARQUET_S3}')")
        existing = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
        log.info("Loaded %d existing records from %s", existing, PARQUET_S3)
    except duckdb.Error:
        log.info("No existing parquet at %s — starting fresh", PARQUET_S3)

    # EIA periods arrive as "2026-03-31T03" (hour only). Parse to datetime so
    # DuckDB doesn't have to handle the non-standard string format.
    fetched_at = datetime.now(timezone.utc)
    new_rows = [
        (
            datetime.strptime(row["period"], "%Y-%m-%dT%H"),
            row["respondent"],
            row["fueltype"],
            row.get("value"),
            fetched_at,
        )
        for row in rows
    ]
    con.executemany(
        "INSERT INTO readings VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        new_rows,
    )

    total   = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    periods = con.execute("SELECT COUNT(DISTINCT period) FROM readings").fetchone()[0]
    log.info("DuckDB: %d records across %d hourly periods", total, periods)


# ---------------------------------------------------------------------------
# Step 3 — Build pivot DataFrame from DuckDB
# ---------------------------------------------------------------------------
def build_dataframe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(
        "SELECT period, fueltype, value_mwh FROM readings ORDER BY period"
    ).df()

    if df.empty:
        raise ValueError("No data in DuckDB — nothing to plot")

    df["period"] = pd.to_datetime(df["period"])
    df = df.pivot_table(index="period", columns="fueltype", values="value_mwh", aggfunc="sum")
    df = df.sort_index()

    # Re-order columns: known fuels in logical stacking order, unknowns appended.
    ordered   = [f for f in FUEL_ORDER if f in df.columns]
    remaining = [f for f in df.columns if f not in ordered]
    df = df[ordered + remaining]

    log.info("DataFrame: %d hourly snapshots x %d fuel types", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Step 4 — Generate stacked area chart
# ---------------------------------------------------------------------------
def generate_plot(df: pd.DataFrame) -> io.BytesIO:
    sns.set_theme(style="whitegrid", context="talk", font_scale=0.9)

    fig, ax = plt.subplots(figsize=(14, 7))

    colors = [FUEL_COLORS.get(f, "#AAAAAA") for f in df.columns]
    labels = [FUEL_LABELS.get(f, f)         for f in df.columns]

    plot_data = df.fillna(0).astype(float)
    ax.stackplot(df.index, plot_data.T.values, labels=labels, colors=colors, alpha=0.88)

    ax.set_title(
        f"Hourly Electricity Generation by Fuel Type — {RESPONDENT}\n"
        f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        fontsize=14, fontweight="bold", pad=14,
    )
    ax.set_xlabel("Time (UTC)", labelpad=8)
    ax.set_ylabel("Generation (MWh)", labelpad=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    handles, leg_labels = ax.get_legend_handles_labels()
    ax.legend(
        handles[::-1], leg_labels[::-1],
        loc="upper left", fontsize=9, ncol=2,
        framealpha=0.85, edgecolor="#cccccc",
    )

    sns.despine(ax=ax, top=True, right=True)
    fig.autofmt_xdate(rotation=25, ha="right")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    log.info("Plot generated (%d bytes)", len(buf.getvalue()))
    return buf


# ---------------------------------------------------------------------------
# Step 5 — Write Parquet back to S3 via httpfs; upload CSV and plot via boto3
# ---------------------------------------------------------------------------
def push_s3(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, plot_buf: io.BytesIO) -> None:
    # Parquet: written directly to S3 by DuckDB — no local file, no boto3.
    con.execute(f"COPY readings TO '{PARQUET_S3}' (FORMAT PARQUET)")
    log.info("Wrote %s via httpfs", PARQUET_S3)

    # CSV and plot are not Parquet so boto3 is the right tool for those.
    s3 = boto3.client("s3", region_name=S3_REGION)

    csv_bytes = df.reset_index().to_csv(index=False).encode()
    s3.put_object(Bucket=S3_BUCKET, Key="data.csv", Body=csv_bytes, ContentType="text/csv")
    log.info("Uploaded data.csv (%d bytes) to s3://%s", len(csv_bytes), S3_BUCKET)

    s3.put_object(Bucket=S3_BUCKET, Key="plot.png", Body=plot_buf.getvalue(), ContentType="image/png")
    log.info("Uploaded plot.png to s3://%s", S3_BUCKET)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    log.info("=== Pipeline starting | respondent=%s | bucket=%s ===", RESPONDENT, S3_BUCKET)
    period, rows = fetch_eia()
    con          = setup_duckdb()
    load_and_store(con, period, rows)
    df           = build_dataframe(con)
    plot_buf     = generate_plot(df)
    push_s3(con, df, plot_buf)
    periods = con.execute("SELECT COUNT(DISTINCT period) FROM readings").fetchone()[0]
    log.info("=== Pipeline complete | %d hourly snapshots accumulated ===", periods)
    con.close()


if __name__ == "__main__":
    main()
