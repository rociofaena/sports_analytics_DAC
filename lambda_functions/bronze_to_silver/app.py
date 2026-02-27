import json
import os
import boto3
import pandas as pd
from urllib.parse import unquote_plus

s3 = boto3.client("s3")

BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver")

# If file is large, skip heavy operations like drop_duplicates
BIG_FILE_MB = float(os.environ.get("BIG_FILE_MB", "10"))  # threshold in MB


def get_bucket_key_from_event(event: dict) -> tuple[str, str]:
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

    raise KeyError(f"Unsupported event format. Keys: {list(event.keys())}")


def lambda_handler(event, context):
    bucket, raw_key = get_bucket_key_from_event(event)

    # IMPORTANT: decode URL-encoded keys (fixes NoSuchKey)
    key = unquote_plus(raw_key)

    print(f"Received event: bucket={bucket}, key={key}")

    # Only process bronze
    if not key.startswith(f"{BRONZE_PREFIX}/"):
        print("Skipping: not a bronze key.")
        return {"statusCode": 200, "body": "Skipped non-bronze object"}

    # Head first: size + sanity check
    head = s3.head_object(Bucket=bucket, Key=key)
    size_bytes = int(head.get("ContentLength", 0))
    size_mb = size_bytes / (1024 * 1024)
    print(f"Object size: {size_bytes} bytes ({size_mb:.2f} MB)")

    # Download and read
    print("Reading CSV...")
    obj = s3.get_object(Bucket=bucket, Key=key)

    # Pandas can read file-like directly; keep it simple
    df = pd.read_csv(obj["Body"])

    # Light cleaning
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Avoid expensive dedupe for big files (common reason it “never finishes”)
    if size_mb <= BIG_FILE_MB:
        df = df.drop_duplicates()
        print("drop_duplicates applied")
    else:
        print("Big file detected -> skipping drop_duplicates (to avoid timeout)")

    # Build output key
    silver_key = key.replace(f"{BRONZE_PREFIX}/", f"{SILVER_PREFIX}/", 1)

    # Write to /tmp then upload (more reliable than huge in-memory strings)
    out_path = "/tmp/out.csv"
    df.to_csv(out_path, index=False)

    s3.upload_file(out_path, bucket, silver_key)

    print(f"Wrote cleaned file to s3://{bucket}/{silver_key}")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Bronze → Silver success",
                "input_key": key,
                "output_key": silver_key,
                "rows": int(len(df)),
                "size_mb": round(size_mb, 2),
            }
        ),
    }