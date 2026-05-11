# =========================================================
# Databricks Python SFTP Ingestion Task
# =========================================================
# Purpose:
# 1. Connect to SFTP server
# 2. Download new CSV claim files
# 3. Land files into Unity Catalog Volumes
# 4. Track ingestion metadata in an audit table
# 5. Prevent duplicate ingestion
# =========================================================


# =========================================================
# Import required libraries
# =========================================================

import os                    # File and OS operations
import hashlib               # Generate SHA256 file hashes
import paramiko              # Connect to SFTP server
from datetime import datetime, timezone
from pyspark.sql import Row  # Create Spark rows for manifest logging


# =========================================================
# Configuration
# =========================================================
# Define connection settings and storage locations
# =========================================================

# Retrieve SFTP credentials securely from Databricks Secrets
SFTP_HOST = dbutils.secrets.get("sftp_scope", "host")
SFTP_PORT = 22
SFTP_USER = dbutils.secrets.get("sftp_scope", "username")
SFTP_PASSWORD = dbutils.secrets.get("sftp_scope", "password")

# SFTP source folder containing CSV claim files
REMOTE_DIR = "/outbound/claims"

# Unity Catalog Volume locations
LANDING_DIR = "/Volumes/main/raw_files/landing/claims"
ARCHIVE_DIR = "/Volumes/main/raw_files/archive/claims"

# Delta audit table used to track ingestion history
MANIFEST_TABLE = "main.audit.sftp_file_manifest"

# Logical source name
SOURCE_SYSTEM = "sftp_claims"


# =========================================================
# Helper Function:
# Generate SHA256 hash for a file
# =========================================================
# Used for:
# - Duplicate detection
# - Auditability
# - Data integrity validation
# =========================================================

def calculate_sha256(file_path):

    # Create empty SHA256 hashing object
    sha256 = hashlib.sha256()

    # Open file in binary mode
    with open(file_path, "rb") as file:

        # Read file in chunks to avoid memory issues
        for chunk in iter(lambda: file.read(8192), b""):

            # Update hash calculation
            sha256.update(chunk)

    # Return final hash value
    return sha256.hexdigest()


# =========================================================
# Helper Function:
# Create landing/archive directories if missing
# =========================================================

def ensure_directories():

    dbutils.fs.mkdirs(LANDING_DIR)
    dbutils.fs.mkdirs(ARCHIVE_DIR)


# =========================================================
# Helper Function:
# Create audit manifest table if it does not exist
# =========================================================
# This table stores ingestion metadata:
# - file name
# - hash
# - ingestion status
# - timestamps
# =========================================================

def create_manifest_table():

    # Create audit schema if needed
    spark.sql("""
        create schema if not exists main.audit
    """)

    # Create Delta manifest table
    spark.sql(f"""
        create table if not exists {MANIFEST_TABLE} (

            source_system string,
            file_name string,
            remote_path string,
            landing_path string,
            file_size_bytes bigint,
            file_hash string,
            ingested_at timestamp,
            status string,
            error_message string

        )
        using delta
    """)


# =========================================================
# Helper Function:
# Retrieve previously ingested files
# =========================================================
# Prevents re-downloading files already processed
# =========================================================

def get_already_landed_files():

    rows = spark.sql(f"""
        select file_name
        from {MANIFEST_TABLE}
        where source_system = '{SOURCE_SYSTEM}'
          and status = 'LANDED'
    """).collect()

    # Convert query results into Python set
    return {row["file_name"] for row in rows}


# =========================================================
# Initialize pipeline setup
# =========================================================

# Ensure directories exist
ensure_directories()

# Ensure manifest table exists
create_manifest_table()

# Load list of files already ingested
already_landed_files = get_already_landed_files()

# Initialize SFTP/session variables
transport = None
sftp = None

# List to store ingestion audit records
manifest_rows = []


# =========================================================
# Main SFTP ingestion process
# =========================================================

try:

    # Create SFTP transport connection
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))

    # Authenticate connection
    transport.connect(
        username=SFTP_USER,
        password=SFTP_PASSWORD
    )

    # Create SFTP client
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Retrieve file list from remote directory
    remote_files = sftp.listdir(REMOTE_DIR)


    # =====================================================
    # Process each file from SFTP
    # =====================================================

    for file_name in remote_files:

        # Only process CSV files
        if not file_name.lower().endswith(".csv"):
            continue

        # Skip files already ingested
        if file_name in already_landed_files:

            print(f"Skipping already landed file: {file_name}")
            continue

        # Build file paths
        remote_path = f"{REMOTE_DIR}/{file_name}"
        local_tmp_path = f"/tmp/{file_name}"
        landing_path = f"{LANDING_DIR}/{file_name}"


        # =================================================
        # Download and land file
        # =================================================

        try:

            print(f"Downloading: {remote_path}")

            # Download file from SFTP to local temp storage
            sftp.get(remote_path, local_tmp_path)

            # Retrieve file size
            file_size = os.path.getsize(local_tmp_path)

            # Generate SHA256 file hash
            file_hash = calculate_sha256(local_tmp_path)

            # Copy file into Unity Catalog landing volume
            dbutils.fs.cp(
                f"file:{local_tmp_path}",
                landing_path
            )

            # Record successful ingestion metadata
            manifest_rows.append(Row(

                source_system=SOURCE_SYSTEM,
                file_name=file_name,
                remote_path=remote_path,
                landing_path=landing_path,
                file_size_bytes=file_size,
                file_hash=file_hash,
                ingested_at=datetime.now(timezone.utc),
                status="LANDED",
                error_message=None

            ))

            print(f"Landed file: {landing_path}")


        # ================================================
        # Handle file-level ingestion failures
        # ================================================

        except Exception as file_error:

            manifest_rows.append(Row(

                source_system=SOURCE_SYSTEM,
                file_name=file_name,
                remote_path=remote_path,
                landing_path=None,
                file_size_bytes=None,
                file_hash=None,
                ingested_at=datetime.now(timezone.utc),
                status="FAILED",
                error_message=str(file_error)

            ))

            print(f"Failed to process file {file_name}: {file_error}")


# =========================================================
# Cleanup connections safely
# =========================================================

finally:

    if sftp:
        sftp.close()

    if transport:
        transport.close()


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

print(f"Completed SFTP ingestion. Files recorded: {len(manifest_rows)}")