import json
import os
import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client("s3")

BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver")


def get_bucket_key_from_event(event: dict) -> tuple[str, str]:
    """
    Supports:
    1) S3 notification events (event["Records"][0]["s3"]...)
    2) EventBridge S3 events (event["detail"]["bucket"]["name"], event["detail"]["object"]["key"])
    """
    # S3 notification format
    if "Records" in event and event["Records"]:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
        return bucket, key

    # EventBridge S3 format
    if "detail" in event:
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]
        return bucket, key

    raise KeyError("Unsupported event format: missing 'Records' and 'detail'")


def lambda_handler(event, context):
    try:
        bucket, key = get_bucket_key_from_event(event)
        print(f"Received event for bucket={bucket}, key={key}")

        # Only process bronze objects
        if not key.startswith(f"{BRONZE_PREFIX}/"):
            print("Not a bronze key. Skipping.")
            return {"statusCode": 200, "body": "Skipped non-bronze object"}

        # Download CSV from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"])

        # Light cleaning for demo
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df = df.drop_duplicates()

        # Write to silver keeping the same subfolders/filename
        silver_key = key.replace(f"{BRONZE_PREFIX}/", f"{SILVER_PREFIX}/", 1)

        buf = StringIO()
        df.to_csv(buf, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=silver_key,
            Body=buf.getvalue(),
            ContentType="text/csv",
        )

        print(f"Wrote cleaned file to s3://{bucket}/{silver_key}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Bronze to Silver success",
                    "input_key": key,
                    "output_key": silver_key,
                    "rows": int(len(df)),
                }
            ),
        }

    except Exception as e:
        print(f"Error in bronze_to_silver: {e}")
        raise
