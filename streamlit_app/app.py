import os
import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError

st.set_page_config(page_title="Sports Demo – Gold Layer", layout="wide")

# Environment variables (set in Elastic Beanstalk → Configuration → Software → Environment properties)
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET")  # required
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "gold/")  # e.g. "gold/latest/"

st.title("🏟️ Sports Analytics Demo – Gold Outputs")

if not S3_BUCKET:
    st.error(
        "Missing environment variable: S3_BUCKET.\n\n"
        "Set it in Elastic Beanstalk → Configuration → Software → Environment properties."
    )
    st.stop()

# Boto3 will automatically use the EC2 instance profile role (no access keys needed)
s3 = boto3.client("s3", region_name=AWS_REGION)

@st.cache_data(ttl=60)
def list_csv_files() -> list[str]:
    prefix = GOLD_PREFIX.rstrip("/") + "/"
    keys: list[str] = []

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.lower().endswith(".csv"):
                    keys.append(k)
    except (NoCredentialsError, ClientError) as e:
        st.error(f"Could not list objects from s3://{S3_BUCKET}/{prefix}\n\n{e}")
        st.stop()

    return sorted(keys)

@st.cache_data(ttl=60)
def load_csv(key: str) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_csv(obj["Body"])
    except (NoCredentialsError, ClientError) as e:
        st.error(f"Could not read s3://{S3_BUCKET}/{key}\n\n{e}")
        st.stop()

keys = list_csv_files()

if not keys:
    st.warning(f"No CSV files found under `{GOLD_PREFIX}` yet. Trigger the pipeline and refresh.")
    st.stop()

selected_key = st.selectbox("Select an output file", keys)
df = load_csv(selected_key)

c1, c2, c3 = st.columns(3)
c1.metric("Rows", df.shape[0])
c2.metric("Columns", df.shape[1])
c3.write(f"**S3 key:** `{selected_key}`")

st.dataframe(df, use_container_width=True)