from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME = "/Volumes/health/raw/synthea"
BUNDLE_SCHEMA = "struct<entry: array<struct<fullUrl: string, resource: string>>>"

@dp.table(
    name="health.bronze.fhir_resources",
    comment="One row per FHIR resource, exploded from Synthea patient bundles.",
    table_properties={"quality": "bronze"},
)
def fhir_resources():
    bundles = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("pathGlobFilter", "*.json")
        .load(f"{VOLUME}/fhir_*/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return (
        bundles
        .withColumn("bundle", F.from_json("value", BUNDLE_SCHEMA))
        .select("_source_file", F.explode("bundle.entry").alias("e"))
        .select(
            F.get_json_object("e.resource", "$.resourceType").alias("resource_type"),
            F.get_json_object("e.resource", "$.id").alias("resource_id"),
            F.col("e.fullUrl").alias("full_url"),
            F.col("e.resource").alias("resource_json"),
            "_source_file",
            F.current_timestamp().alias("_ingested_at"),
        )
    )