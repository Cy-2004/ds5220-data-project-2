import io
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
from boto3.dynamodb.conditions import Key

matplotlib.use("Agg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ISS_API      = "https://api.wheretheiss.at/v1/satellites/25544"
SATELLITE_ID = "ISS"
TABLE_NAME   = os.environ["DYNAMODB_TABLE"]
S3_BUCKET    = os.environ["S3_BUCKET"]
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")

# Altitude gain at or above this value in a single 15-minute interval is
# flagged as a reboost / orbital burn. ISS reboosts typically raise the
# orbit by 1–3 km; normal orbital decay between burns is ~0.05 km/interval.
BURN_THRESHOLD_KM = Decimal("1.0")


# ---------------------------------------------------------------------------
# Step 1 — Fetch current ISS position from wheretheiss.at
# ---------------------------------------------------------------------------
def fetch_iss() -> dict:
    """Return a DynamoDB-ready item with the current ISS state."""
    resp = requests.get(ISS_API, timeout=10)
    resp.raise_for_status()
    d = resp.json()
    return {
        "satellite_id": SATELLITE_ID,
        "timestamp":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latitude":     Decimal(str(round(d["latitude"],  6))),
        "longitude":    Decimal(str(round(d["longitude"], 6))),
        "altitude_km":  Decimal(str(round(d["altitude"],  3))),
        "velocity_kms": Decimal(str(round(d["velocity"],  3))),
        "visibility":   d.get("visibility", "unknown"),
    }


# ---------------------------------------------------------------------------
# Step 2 — Query DynamoDB for the most recent previous entry
# ---------------------------------------------------------------------------
def get_previous(table) -> dict | None:
    """Return the latest stored item for ISS, or None on first run."""
    resp = table.query(
        KeyConditionExpression=Key("satellite_id").eq(SATELLITE_ID),
        ScanIndexForward=False,   # descending timestamp order
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0] if items else None


# ---------------------------------------------------------------------------
# Step 3 — Compare current altitude to previous entry
# ---------------------------------------------------------------------------
def altitude_analysis(current_km: Decimal, previous: dict | None) -> tuple[str, Decimal]:
    """Return (trend_label, delta_km) comparing current to previous altitude.

    Trend labels:
      FIRST_ENTRY  — no prior data to compare against
      ASCENDING    — small natural gain (solar pressure, atmospheric variation)
      DESCENDING   — normal orbital decay due to atmospheric drag
      STABLE       — negligible change
      ORBITAL_BURN — altitude jumped >= BURN_THRESHOLD_KM; reboost likely
    """
    if previous is None:
        return "FIRST_ENTRY", Decimal("0")

    delta = current_km - Decimal(str(previous["altitude_km"]))

    if delta >= BURN_THRESHOLD_KM:
        trend = "ORBITAL_BURN"
    elif delta > Decimal("0.01"):
        trend = "ASCENDING"
    elif delta < Decimal("-0.01"):
        trend = "DESCENDING"
    else:
        trend = "STABLE"

    return trend, delta


# ---------------------------------------------------------------------------
# Step 4 — Fetch full altitude history from DynamoDB for plotting
# ---------------------------------------------------------------------------
def fetch_history(table) -> pd.DataFrame:
    """Return all stored ISS records as a DataFrame, sorted by timestamp.
    Handles DynamoDB pagination so the full history is always returned.
    """
    items, kwargs = [], dict(
        KeyConditionExpression=Key("satellite_id").eq(SATELLITE_ID),
        ScanIndexForward=True,
    )
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)
    df["timestamp"]   = pd.to_datetime(df["timestamp"])
    df["altitude_km"] = df["altitude_km"].astype(float)
    df["delta_km"]    = df["delta_km"].astype(float)
    return df.sort_values("timestamp").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 5 — Render altitude-over-time plot
# ---------------------------------------------------------------------------
def generate_plot(df: pd.DataFrame) -> io.BytesIO | None:
    """Plot ISS altitude over time with orbital burn annotations."""
    if df.empty or len(df) < 2:
        log.info("Not enough history to plot yet (%d point(s))", len(df))
        return None

    sns.set_theme(style="darkgrid", context="talk", font_scale=0.9)

    fig, ax = plt.subplots(figsize=(14, 6))

    # Altitude line
    sns.lineplot(data=df, x="timestamp", y="altitude_km",
                 ax=ax, color="#4FC3F7", linewidth=2.5, zorder=2)

    # Subtle fill under the line
    ax.fill_between(df["timestamp"], df["altitude_km"],
                    df["altitude_km"].min() - 1,
                    alpha=0.12, color="#4FC3F7")

    # Highlight orbital burns with a scatter point + rocket annotation
    burns = df[df["trend"] == "ORBITAL_BURN"]
    if not burns.empty:
        ax.scatter(burns["timestamp"], burns["altitude_km"],
                   color="#FF6B35", s=140, zorder=4,
                   label=f"Orbital burn ({len(burns)} detected)")
        for _, row in burns.iterrows():
            ax.annotate(
                "🚀",
                xy=(row["timestamp"], row["altitude_km"]),
                xytext=(0, 14),
                textcoords="offset points",
                ha="center", fontsize=16, zorder=5,
            )

    ax.set_title(
        "ISS Orbital Altitude\n"
        f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        fontsize=14, fontweight="bold", pad=14,
    )
    ax.set_xlabel("Time (UTC)", labelpad=8)
    ax.set_ylabel("Altitude (km)", labelpad=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f} km"))

    # Y axis: 300 km floor (well below ISS operational range), top padded 5% above highest reading
    ax.set_ylim(300, df["altitude_km"].max() * 1.05)

    if not burns.empty:
        ax.legend(loc="upper right", fontsize=9, framealpha=0.85, edgecolor="#555555")

    sns.despine(ax=ax, top=True, right=True)
    fig.autofmt_xdate(rotation=25, ha="right")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    log.info("Plot generated (%d bytes, %d points)", len(buf.getvalue()), len(df))
    return buf


# ---------------------------------------------------------------------------
# Step 6 — Upload plot to S3
# ---------------------------------------------------------------------------
def push_plot(buf: io.BytesIO) -> None:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="iss-altitude.png",
        Body=buf.getvalue(),
        ContentType="image/png",
    )
    log.info("Uploaded iss-altitude.png to s3://%s", S3_BUCKET)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table    = dynamodb.Table(TABLE_NAME)

    previous     = get_previous(table)
    entry        = fetch_iss()
    trend, delta = altitude_analysis(entry["altitude_km"], previous)

    entry["trend"]    = trend
    entry["delta_km"] = delta

    table.put_item(Item=entry)

    if trend == "FIRST_ENTRY":
        log.info(
            "ISS | alt=%.3f km | lat=%.4f | lon=%.4f | visibility=%s | FIRST ENTRY",
            entry["altitude_km"], entry["latitude"], entry["longitude"], entry["visibility"],
        )
    else:
        burn_flag = "  *** ORBITAL BURN DETECTED ***" if trend == "ORBITAL_BURN" else ""
        log.info(
            "ISS | alt=%.3f km | delta=%+.3f km | %-12s | lat=%.4f | lon=%.4f | visibility=%s%s",
            entry["altitude_km"], delta, trend,
            entry["latitude"], entry["longitude"], entry["visibility"], burn_flag,
        )

    history  = fetch_history(table)
    plot_buf = generate_plot(history)
    if plot_buf:
        push_plot(plot_buf)


if __name__ == "__main__":
    main()