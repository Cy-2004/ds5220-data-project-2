import io
import logging
import os
import boto3
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime, timezone

TABLE_NAME = os.environ["DYNAMODB_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

URL = "https://waterservices.usgs.gov/nwis/iv/?format=json&sites=02035000&parameterCd=00065"

    
def fetch_water():
    try:
        r = requests.get(URL, timeout=30)
        data = r.json()

        ts = data.get("value", {}).get("timeSeries", [])
        if not ts:
            log.warning("No timeSeries data")
            return None

        values = ts[0].get("values", [])
        if not values or not values[0].get("value"):
            log.warning("No values in response")
            return None

        value = values[0]["value"][0]["value"]

        return {
            "sensor": "usgs",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": Decimal(str(value)),
        }

    except Exception as e:
        log.error("API request failed: %s", str(e))
        return None


def get_history():
    resp = table.query(KeyConditionExpression=Key("sensor").eq("usgs"))
    return resp.get("Items", [])

def generate_plot(df):
    df = df.sort_values("timestamp")
    df["level"] = df["level"].astype(float)

    sns.set_theme(style="darkgrid")

    plt.figure(figsize=(12,6))
    sns.lineplot(x=pd.to_datetime(df["timestamp"]), y=df["level"], marker="o", linewidth=2.5)
    plt.title("River Water Level Over Time")
    plt.xlabel("Time")
    plt.ylabel("Water Level (ft)")
    plt.xticks(rotation=30)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    log.info("Plot generated")
    return buf


def generate_csv(df):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    log.info("CSV generated")
    return buf.getvalue()


def upload_plot(buf):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="plot.png",
        Body=buf.getvalue(),
        ContentType="image/png"
    )
    log.info("Uploaded plot.png to s3://%s", S3_BUCKET)


def upload_csv(csv_data):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="data.csv",
        Body=csv_data,
        ContentType="text/csv"
    )
    log.info("Uploaded data.csv to s3://%s", S3_BUCKET)


def main():
    item = fetch_water()

    if item is None:
        log.warning("Skipping this run due to API failure")
        return

    table.put_item(Item=item)

    log.info("Fetching history")
    items = get_history()

    df = pd.DataFrame(items)
    log.info("History fetched, rows = %d", len(df))

    if len(df) < 2:
        log.info("Not enough data to generate plot")
        return

    log.info("Generating plot")
    plot_buf = generate_plot(df)

    log.info("Uploading plot.png to S3...")
    upload_plot(plot_buf)

    log.info("Generating csv")
    csv_data = generate_csv(df)

    log.info("Uploading data.csv to S3...")
    upload_csv(csv_data)


if __name__ == "__main__":
    main()
