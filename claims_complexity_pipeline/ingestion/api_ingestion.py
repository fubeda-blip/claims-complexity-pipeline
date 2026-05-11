# =========================================================
# Databricks Python API Ingestion Task
# =========================================================
# Purpose:
# 1. Connect to external Medical Activity API
# 2. Retrieve JSON medical activity data
# 3. Save raw JSON response to a Unity Catalog Volume
# 4. Track ingestion metadata in an audit table
# 5. Preserve raw API response for Auto Loader ingestion
# =========================================================


# =========================================================
# Import required libraries
# =========================================================

import json                         # Convert API response to JSON string
import hashlib                      # Generate SHA256 hash of API payload
import requests                     # Make HTTP API requests
from datetime import datetime, timezone
from pyspark.sql import Row         # Create Spark rows for manifest logging


# =========================================================
# Configuration
# =========================================================

# Retrieve API credentials securely from Databricks Secrets
API_URL = dbutils.secrets.get("claims_api", "url")
API_KEY = dbutils.secrets.get("claims_api", "key")

# Unity Catalog Volume location for raw JSON files
LANDING_DIR = "/Volumes/main/raw_files/landing/medical_activity"

# Delta audit table used to track API ingestion history
MANIFEST_TABLE = "main.audit.api_file_manifest"

# Logical source name
SOURCE_SYSTEM = "api_medical_activity"


# =========================================================
# Helper Function:
# Generate SHA256 hash for API payload
# =========================================================
# Used for:
# - Auditability
# - Duplicate detection
# - Data integrity validation
# =========================================================

def calculate_payload_hash(payload_string):

    return hashlib.sha256(
        payload_string.encode("utf-8")
    ).hexdigest()


# =========================================================
# Helper Function:
# Create landing directory if missing
# =========================================================

def ensure_directories():

    dbutils.fs.mkdirs(LANDING_DIR)


# =========================================================
# Helper Function:
# Create audit manifest table if it does not exist
# =========================================================

def create_manifest_table():

    spark.sql("""
        create schema if not exists main.audit
    """)

    spark.sql(f"""
        create table if not exists {MANIFEST_TABLE} (

            source_system string,
            api_url string,
            landing_path string,
            payload_hash string,
            record_count bigint,
            ingested_at timestamp,
            status string,
            error_message string

        )
        using delta
    """)


# =========================================================
# Helper Function:
# Count records in API response
# =========================================================
# Handles common API response patterns:
# - List of records
# - Dictionary with a "data" array
# - Dictionary with a "results" array
# =========================================================

def get_record_count(payload):

    if isinstance(payload, list):
        return len(payload)

    if isinstance(payload, dict):

        if "data" in payload and isinstance(payload["data"], list):
            return len(payload["data"])

        if "results" in payload and isinstance(payload["results"], list):
            return len(payload["results"])

    return 1


# =========================================================
# Initialize pipeline setup
# =========================================================

ensure_directories()
create_manifest_table()

manifest_rows = []


# =========================================================
# Main API ingestion process
# =========================================================

try:

    # Build API request headers
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    # Optional query parameters
    # Example: pull records updated today, paginate, etc.
    params = {
        # "updated_since": "2026-05-01"
    }

    print(f"Calling API: {API_URL}")


    # =====================================================
    # Call external API
    # =====================================================

    response = requests.get(
        API_URL,
        headers=headers,
        params=params,
        timeout=60
    )

    # Raise error if API returns 4xx or 5xx response
    response.raise_for_status()


    # =====================================================
    # Parse JSON response
    # =====================================================

    payload = response.json()

    # Convert JSON payload to formatted string
    payload_string = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2
    )

    # Generate hash of full API response
    payload_hash = calculate_payload_hash(payload_string)

    # Count records returned
    record_count = get_record_count(payload)

    # Create unique output filename
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    file_name = f"medical_activity_{run_timestamp}.json"

    landing_path = f"{LANDING_DIR}/{file_name}"


    # =====================================================
    # Write raw JSON response to landing volume
    # =====================================================

    dbutils.fs.put(
        landing_path,
        payload_string,
        overwrite=False
    )

    print(f"Landed API response: {landing_path}")


    # =====================================================
    # Record successful ingestion metadata
    # =====================================================

    manifest_rows.append(Row(

        source_system=SOURCE_SYSTEM,
        api_url=API_URL,
        landing_path=landing_path,
        payload_hash=payload_hash,
        record_count=record_count,
        ingested_at=datetime.now(timezone.utc),
        status="LANDED",
        error_message=None

    ))


# =========================================================
# Handle API-level failures
# =========================================================

except Exception as api_error:

    manifest_rows.append(Row(

        source_system=SOURCE_SYSTEM,
        api_url=API_URL,
        landing_path=None,
        payload_hash=None,
        record_count=None,
        ingested_at=datetime.now(timezone.utc),
        status="FAILED",
        error_message=str(api_error)

    ))

    print(f"API ingestion failed: {api_error}")


# =========================================================
# Write ingestion audit records into Delta manifest table
# =========================================================

if manifest_rows:

    manifest_df = spark.createDataFrame(manifest_rows)

    manifest_df.write \
        .mode("append") \
        .saveAsTable(MANIFEST_TABLE)


# =========================================================
# Final logging message
# =========================================================

print(f"Completed API ingestion. Records written to manifest: {len(manifest_rows)}")