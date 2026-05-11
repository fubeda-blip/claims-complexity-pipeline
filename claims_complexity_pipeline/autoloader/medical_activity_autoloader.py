# =========================================================
# Databricks Auto Loader Task: Medical Activity JSON to Bronze
# =========================================================
# Purpose:
# 1. Read raw Medical Activity JSON files from the landing volume
# 2. Incrementally ingest new JSON files using Auto Loader
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

# Unity Catalog Volume where the API ingestion task lands JSON files
LANDING_DIR = "/Volumes/main/raw_files/landing/medical_activity"

# Location where Auto Loader stores inferred JSON schema metadata
SCHEMA_LOCATION = "/Volumes/main/raw_files/schema/medical_activity"

# Location where Auto Loader stores checkpoint/progress metadata
CHECKPOINT_LOCATION = "/Volumes/main/raw_files/checkpoints/medical_activity"

# Target Bronze Delta table
BRONZE_TABLE = "main.bronze.raw_medical_activity"


# =========================================================
# Create target schema if needed
# =========================================================

spark.sql("""
    create schema if not exists main.bronze
""")


# =========================================================
# Ensure required directories exist
# =========================================================

dbutils.fs.mkdirs(LANDING_DIR)
dbutils.fs.mkdirs(SCHEMA_LOCATION)
dbutils.fs.mkdirs(CHECKPOINT_LOCATION)


# =========================================================
# Build Auto Loader read stream
# =========================================================
# format("cloudFiles") tells Databricks to use Auto Loader.
# Auto Loader tracks new files and prevents reprocessing.
# =========================================================

medical_activity_stream_df = (
    spark.readStream

        # Use Databricks Auto Loader
        .format("cloudFiles")

        # Source file format is JSON
        .option("cloudFiles.format", "json")

        # Store inferred schema metadata here
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)

        # Allow unexpected/new JSON fields to be rescued
        # instead of breaking the pipeline
        .option("cloudFiles.schemaEvolutionMode", "rescue")

        # Store unexpected fields here
        .option("rescuedDataColumn", "_rescued_data")

        # Read raw JSON files from the landing volume
        .load(LANDING_DIR)

        # Add ingestion timestamp
        .withColumn("_ingested_at", current_timestamp())

        # Add source file path/name for lineage
        .withColumn("_source_file", input_file_name())
)


# =========================================================
# Write stream to Bronze Delta table
# =========================================================
# availableNow=True processes all currently available files
# and then stops, which works well for scheduled workflows.
# =========================================================

query = (
    medical_activity_stream_df.writeStream

        # Checkpoint tracks which files have already been processed
        .option("checkpointLocation", CHECKPOINT_LOCATION)

        # Batch-style streaming trigger
        .trigger(availableNow=True)

        # Write to Bronze Delta table
        .toTable(BRONZE_TABLE)
)


# =========================================================
# Wait for Auto Loader job to finish
# =========================================================

query.awaitTermination()


# =========================================================
# Final logging message
# =========================================================

print(f"Completed Auto Loader ingestion into table: {BRONZE_TABLE}")