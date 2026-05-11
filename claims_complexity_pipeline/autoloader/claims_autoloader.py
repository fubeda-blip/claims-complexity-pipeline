# =========================================================
# Databricks Auto Loader Task: Claims CSV to Bronze
# =========================================================
# Purpose:
# 1. Read raw Claims CSV files from the landing volume
# 2. Incrementally ingest new files using Auto Loader
# 3. Add ingestion metadata columns
# 4. Handle schema tracking and schema evolution
# 5. Write raw records into a Bronze Delta table
# =========================================================


# =========================================================
# Import required libraries
# =========================================================

from pyspark.sql.functions import current_timestamp, input_file_name


# =========================================================
# Configuration
# =========================================================
# Define source landing path, schema path, checkpoint path,
# and target Bronze Delta table
# =========================================================

# Unity Catalog Volume where the SFTP ingestion task lands CSV files
LANDING_DIR = "/Volumes/main/raw_files/landing/claims"

# Location where Auto Loader stores inferred schema metadata
SCHEMA_LOCATION = "/Volumes/main/raw_files/schema/claims"

# Location where Auto Loader stores checkpoint/progress metadata
CHECKPOINT_LOCATION = "/Volumes/main/raw_files/checkpoints/claims"

# Target Bronze Delta table
BRONZE_TABLE = "main.bronze.raw_claims"


# =========================================================
# Create target schema if needed
# =========================================================
# The Bronze schema stores raw ingested Delta tables
# =========================================================

spark.sql("""
    create schema if not exists main.bronze
""")


# =========================================================
# Ensure required directories exist
# =========================================================
# These directories support Auto Loader state management
# =========================================================

dbutils.fs.mkdirs(LANDING_DIR)
dbutils.fs.mkdirs(SCHEMA_LOCATION)
dbutils.fs.mkdirs(CHECKPOINT_LOCATION)


# =========================================================
# Build Auto Loader read stream
# =========================================================
# Auto Loader uses format("cloudFiles") to incrementally
# discover and process new files from the landing directory.
# =========================================================

claims_stream_df = (
    spark.readStream

        # Use Databricks Auto Loader
        .format("cloudFiles")

        # Source file format is CSV
        .option("cloudFiles.format", "csv")

        # CSV files contain a header row
        .option("header", "true")

        # Store schema inference/evolution metadata here
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)

        # Infer column types from incoming CSV files
        # For Bronze, you can also set this to false and keep all fields as strings
        .option("inferSchema", "true")

        # Rescue unexpected or malformed data instead of failing immediately
        .option("cloudFiles.schemaEvolutionMode", "rescue")

        # Store rescued/unexpected fields in this column
        .option("rescuedDataColumn", "_rescued_data")

        # Read from the landing volume
        .load(LANDING_DIR)

        # Add timestamp showing when Databricks ingested the record
        .withColumn("_ingested_at", current_timestamp())

        # Add source file name/path for lineage and debugging
        .withColumn("_source_file", input_file_name())
)


# =========================================================
# Write stream to Bronze Delta table
# =========================================================
# availableNow=True makes this behave like a scheduled batch:
# process all currently available files, then stop.
# =========================================================

query = (
    claims_stream_df.writeStream

        # Write incrementally to a Delta table
        .option("checkpointLocation", CHECKPOINT_LOCATION)

        # Process all available files once and then terminate
        .trigger(availableNow=True)

        # Write to the managed Bronze Delta table
        .toTable(BRONZE_TABLE)
)


# =========================================================
# Wait for Auto Loader job to finish
# =========================================================
# This is useful in Databricks Workflows so downstream tasks
# do not start until ingestion completes.
# =========================================================

query.awaitTermination()


# =========================================================
# Final logging message
# =========================================================

print(f"Completed Auto Loader ingestion into table: {BRONZE_TABLE}")