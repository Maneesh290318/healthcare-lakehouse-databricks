from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ANSI mode is on in serverless, so a plain cast fails the pipeline on one bad value.
# try_cast returns NULL instead, and the expectations below catch the NULLs.
def ts(c):  return F.expr(f"try_cast({c} AS TIMESTAMP)")
def dt(c):  return F.expr(f"try_cast({c} AS DATE)")
def num(c): return F.expr(f"try_cast({c} AS DOUBLE)")

# ---------- ENCOUNTERS ----------
ENCOUNTER_RULES = {
    "valid_encounter_id": "encounter_id IS NOT NULL",
    "valid_patient_id":   "patient_id IS NOT NULL",
    "valid_start":        "start_ts IS NOT NULL",
}

@dp.table(name="health.silver.encounters",
          comment="Typed encounters. Rows failing ENCOUNTER_RULES are dropped and counted.",
          table_properties={"quality": "silver"})
@dp.expect_all_or_drop(ENCOUNTER_RULES)
@dp.expect("stop_after_start", "stop_ts IS NULL OR stop_ts >= start_ts")
def encounters():
    return (spark.readStream.table("health.bronze.encounters").select(
        F.col("Id").alias("encounter_id"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.col("PROVIDER").alias("provider_id"),
        F.col("PAYER").alias("payer_id"),
        F.lower("ENCOUNTERCLASS").alias("encounter_class"),
        F.col("CODE").alias("snomed_code"),
        F.col("DESCRIPTION").alias("description"),
        ts("START").alias("start_ts"),
        ts("STOP").alias("stop_ts"),
        num("BASE_ENCOUNTER_COST").alias("base_cost"),
        num("TOTAL_CLAIM_COST").alias("total_claim_cost"),
        num("PAYER_COVERAGE").alias("payer_coverage"),
        F.col("REASONCODE").alias("reason_code"),
        F.col("REASONDESCRIPTION").alias("reason_description"),
        "_source_file", "_ingested_at"))

@dp.table(name="health.silver.quarantine_encounters",
          comment="Encounter rows that fail the silver rules, kept for investigation.")
def quarantine_encounters():
    return (spark.readStream.table("health.bronze.encounters")
            .where("Id IS NULL OR PATIENT IS NULL OR try_cast(START AS TIMESTAMP) IS NULL"))

# ---------- OBSERVATIONS ----------
@dp.table(name="health.silver.observations",
          comment="Typed observations (labs, vitals, surveys) with a numeric value column.",
          table_properties={"quality": "silver"})
@dp.expect_all_or_drop({"valid_patient_id": "patient_id IS NOT NULL",
                        "valid_observed_ts": "observed_ts IS NOT NULL"})
@dp.expect("numeric_has_value", "value_type <> 'numeric' OR value_numeric IS NOT NULL")
def observations():
    return (spark.readStream.table("health.bronze.observations").select(
        ts("DATE").alias("observed_ts"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.col("CATEGORY").alias("category"),
        F.col("CODE").alias("loinc_code"),
        F.col("DESCRIPTION").alias("description"),
        F.col("VALUE").alias("value_raw"),
        F.expr("CASE WHEN TYPE = 'numeric' THEN try_cast(VALUE AS DOUBLE) END").alias("value_numeric"),
        F.col("UNITS").alias("units"),
        F.col("TYPE").alias("value_type"),
        "_source_file", "_ingested_at"))

# ---------- CONDITIONS & MEDICATIONS ----------
@dp.table(name="health.silver.conditions", table_properties={"quality": "silver"})
@dp.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
def conditions():
    return (spark.readStream.table("health.bronze.conditions").select(
        dt("START").alias("onset_date"), dt("STOP").alias("resolved_date"),
        F.col("PATIENT").alias("patient_id"), F.col("ENCOUNTER").alias("encounter_id"),
        F.col("CODE").alias("snomed_code"), F.col("DESCRIPTION").alias("description"),
        "_ingested_at"))

@dp.table(name="health.silver.medications", table_properties={"quality": "silver"})
@dp.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
def medications():
    return (spark.readStream.table("health.bronze.medications").select(
        ts("START").alias("start_ts"), ts("STOP").alias("stop_ts"),
        F.col("PATIENT").alias("patient_id"), F.col("ENCOUNTER").alias("encounter_id"),
        F.col("PAYER").alias("payer_id"),
        F.col("CODE").alias("rxnorm_code"), F.col("DESCRIPTION").alias("description"),
        num("BASE_COST").alias("base_cost"), num("TOTALCOST").alias("total_cost"),
        F.expr("try_cast(DISPENSES AS INT)").alias("dispenses"),
        "_ingested_at"))

# ---------- PATIENTS: SCD TYPE 2 ----------
@dp.table(name="health.silver.patients_staged",
          comment="Typed patient rows feeding the SCD Type 2 dimension.")
@dp.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
def patients_staged():
    return (spark.readStream.table("health.bronze.patients").select(
        F.col("Id").alias("patient_id"),
        dt("BIRTHDATE").alias("birth_date"), dt("DEATHDATE").alias("death_date"),
        F.col("SSN").alias("ssn"), F.col("FIRST").alias("first_name"), F.col("LAST").alias("last_name"),
        F.col("GENDER").alias("gender"), F.col("RACE").alias("race"), F.col("ETHNICITY").alias("ethnicity"),
        F.col("MARITAL").alias("marital_status"),
        F.col("ADDRESS").alias("address"), F.col("CITY").alias("city"), F.col("STATE").alias("state"),
        F.col("COUNTY").alias("county"), F.col("ZIP").alias("zip"),
        num("HEALTHCARE_EXPENSES").alias("healthcare_expenses"),
        num("HEALTHCARE_COVERAGE").alias("healthcare_coverage"),
        "_source_file", "_ingested_at"))

dp.create_streaming_table(name="health.silver.dim_patient",
                          comment="Patient dimension, SCD Type 2: every address or attribute change keeps history.")

dp.create_auto_cdc_flow(
    target="health.silver.dim_patient",
    source="health.silver.patients_staged",
    keys=["patient_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type="2",
    except_column_list=["_source_file"],
    track_history_except_column_list=["_ingested_at"],
)

# ---------- SMALL DIMENSIONS (batch reads, so these become materialized views) ----------
@dp.table(name="health.silver.dim_provider")
def dim_provider():
    return (spark.read.table("health.bronze.providers").select(
        F.col("Id").alias("provider_id"), F.col("ORGANIZATION").alias("organization_id"),
        F.col("NAME").alias("provider_name"), F.col("GENDER").alias("gender"),
        F.col("SPECIALITY").alias("specialty"), F.col("CITY").alias("city"), F.col("STATE").alias("state"))
        .dropDuplicates(["provider_id"]))

@dp.table(name="health.silver.dim_organization")
def dim_organization():
    return (spark.read.table("health.bronze.organizations").select(
        F.col("Id").alias("organization_id"), F.col("NAME").alias("organization_name"),
        F.col("CITY").alias("city"), F.col("STATE").alias("state"), F.col("ZIP").alias("zip"))
        .dropDuplicates(["organization_id"]))

@dp.table(name="health.silver.dim_payer")
def dim_payer():
    return (spark.read.table("health.bronze.payers").select(
        F.col("Id").alias("payer_id"), F.col("NAME").alias("payer_name"),
        F.col("OWNERSHIP").alias("ownership"))
        .dropDuplicates(["payer_id"]))

# ---------- FHIR: parse Patient resources ----------
@dp.table(name="health.silver.fhir_patients",
          comment="Patient resources parsed from FHIR bundles.")
def fhir_patients():
    r = "resource_json"
    return (spark.readStream.table("health.bronze.fhir_resources")
            .where("resource_type = 'Patient'")
            .select(
                F.col("resource_id").alias("patient_id"),
                F.get_json_object(r, "$.gender").alias("gender"),
                F.expr(f"try_cast(get_json_object({r}, '$.birthDate') AS DATE)").alias("birth_date"),
                F.get_json_object(r, "$.name[0].family").alias("last_name"),
                F.get_json_object(r, "$.address[0].city").alias("city"),
                F.get_json_object(r, "$.address[0].state").alias("state")))