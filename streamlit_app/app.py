import os
import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError

st.set_page_config(page_title="Sports Demo – Gold Layer", layout="wide")

# EB env vars
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET = os.getenv("S3_BUCKET")  # required
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "gold/latest/")  # default to latest

st.title("🏟️ Sports Analytics Demo – Gold Outputs")

if not S3_BUCKET:
    st.error(
        "Missing environment variable: S3_BUCKET.\n\n"
        "Set it in Elastic Beanstalk → Configuration → Software → Environment properties."
    )
    st.stop()

# Uses EB instance profile automatically (no keys needed)
s3 = boto3.client("s3", region_name=AWS_REGION)

# Refresh button (clears cached S3 calls)
if st.button("🔄 Refresh (reload S3)"):
    st.cache_data.clear()

def _normalize_prefix(p: str) -> str:
    p = (p or "").strip()
    if p == "":
        return ""
    return p if p.endswith("/") else p + "/"

@st.cache_data(ttl=60)
def list_csv_files(bucket: str, prefix: str):
    """Returns list of dicts: {Key, LastModified, Size} for CSVs under prefix."""
    prefix = _normalize_prefix(prefix)
    items = []

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                # skip "folder" placeholders if any
                if k.endswith("/"):
                    continue
                if k.lower().endswith(".csv"):
                    items.append(
                        {
                            "Key": k,
                            "LastModified": obj.get("LastModified"),
                            "SizeMB": round(obj.get("Size", 0) / 1024 / 1024, 2),
                        }
                    )
    except (NoCredentialsError, ClientError) as e:
        st.error(f"Could not list objects from s3://{bucket}/{prefix}\n\n{e}")
        st.stop()

    # newest first
    items.sort(key=lambda x: (x["LastModified"] is not None, x["LastModified"]), reverse=True)
    return items

@st.cache_data(ttl=60)
def load_csv(bucket: str, key: str) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj["Body"])
    except (NoCredentialsError, ClientError) as e:
        st.error(f"Could not read s3://{bucket}/{key}\n\n{e}")
        st.stop()

st.caption(f"Bucket: **{S3_BUCKET}** | Prefix: **{_normalize_prefix(GOLD_PREFIX)}**")

files = list_csv_files(S3_BUCKET, GOLD_PREFIX)

if not files:
    st.warning(f"No CSV files found under `{_normalize_prefix(GOLD_PREFIX)}` yet.")
    st.info("Check that your pipeline wrote files to `gold/latest/` (recommended) or update GOLD_PREFIX.")
    st.stop()

keys = [f["Key"] for f in files]
selected_key = st.selectbox("Select an output file", keys)

meta = next((f for f in files if f["Key"] == selected_key), None)
df = load_csv(S3_BUCKET, selected_key)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{df.shape[0]:,}")
c2.metric("Columns", f"{df.shape[1]:,}")
c3.metric("Size (MB)", f"{meta['SizeMB'] if meta else '—'}")
c4.write(f"**Last modified:** {meta['LastModified'] if meta else '—'}")

st.write(f"**S3 key:** `{selected_key}`")
st.dataframe(df.head(500), use_container_width=True)

with st.expander("Column summary"):
    col = st.selectbox("Column", df.columns.tolist())
    st.write(df[col].describe(include="all"))