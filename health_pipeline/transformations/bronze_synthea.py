from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME = "/Volumes/health/raw/synthea"
TABLES = ["patients", "encounters", "conditions", "medications", "observations",
          "procedures", "immunizations", "allergies", "careplans", "claims",
          "payers", "payer_transitions", "providers", "organizations"]

def make_bronze_table(name):
    @dp.table(
        name=f"health.bronze.{name}",
        comment=f"Raw Synthea {name}.csv, ingested incrementally with Auto Loader. All columns kept as strings.",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(f"{VOLUME}/batch_*/{name}.csv")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )
    return bronze

for t in TABLES:
    make_bronze_table(t)