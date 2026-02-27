import json
import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timezone

s3 = boto3.client("s3")

SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver")
GOLD_PREDICTIONS_PREFIX = os.environ.get("GOLD_PREDICTIONS_PREFIX", "gold/predictions")
GOLD_METRICS_PREFIX = os.environ.get("GOLD_METRICS_PREFIX", "gold/metrics")
GOLD_LATEST_PREFIX = os.environ.get("GOLD_LATEST_PREFIX", "gold/latest")


def get_bucket_key_from_event(event: dict) -> tuple[str, str]:
    # S3 notification payload
    if "Records" in event and event["Records"]:
        rec = event["Records"][0]
        return rec["s3"]["bucket"]["name"], rec["s3"]["object"]["key"]

    # EventBridge payload for S3
    if "detail" in event and "bucket" in event["detail"] and "object" in event["detail"]:
        return event["detail"]["bucket"]["name"], event["detail"]["object"]["key"]

    raise KeyError(f"Unsupported event format. Keys: {list(event.keys())}")


def lambda_handler(event, context):
    try:
        bucket, key = get_bucket_key_from_event(event)
        print(f"Received event: bucket={bucket}, key={key}")

        # Only process silver objects
        if not key.startswith(f"{SILVER_PREFIX}/"):
            print(f"Skipping: key does not start with {SILVER_PREFIX}/")
            return {"statusCode": 200, "body": "Skipped non-silver object"}

        # Read CSV from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"])

        # Demo "inference": create a score that works for any dataset
        result_df = df.copy()
        result_df["demo_score"] = result_df.notna().sum(axis=1)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base_name = key.split("/")[-1].replace(".csv", "")

        # Outputs
        pred_key = f"{GOLD_PREDICTIONS_PREFIX}/{base_name}_predictions_{timestamp}.csv"
        metrics_key = f"{GOLD_METRICS_PREFIX}/{base_name}_metrics_{timestamp}.csv"
        latest_key = f"{GOLD_LATEST_PREFIX}/{base_name}_latest.csv"

        # Write predictions
        pred_buf = StringIO()
        result_df.to_csv(pred_buf, index=False)
        s3.put_object(Bucket=bucket, Key=pred_key, Body=pred_buf.getvalue(), ContentType="text/csv")

        # Write metrics summary
        metrics_df = pd.DataFrame([{
            "source_key": key,
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "generated_at_utc": timestamp,
            "demo_score_mean": float(result_df["demo_score"].mean()) if len(result_df) else 0.0
        }])
        metrics_buf = StringIO()
        metrics_df.to_csv(metrics_buf, index=False)
        s3.put_object(Bucket=bucket, Key=metrics_key, Body=metrics_buf.getvalue(), ContentType="text/csv")

        # Write latest snapshot (used by Streamlit later)
        latest_buf = StringIO()
        result_df.to_csv(latest_buf, index=False)
        s3.put_object(Bucket=bucket, Key=latest_key, Body=latest_buf.getvalue(), ContentType="text/csv")

        print(f"Wrote gold outputs: {pred_key}, {metrics_key}, {latest_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Silver → Gold success",
                "input_key": key,
                "prediction_key": pred_key,
                "metrics_key": metrics_key,
                "latest_key": latest_key
            })
        }

    except Exception as e:
        print(f"ERROR in silver_to_gold: {e}")
        raise
