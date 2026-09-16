"""Glue Job 01 — Bronze: CSVs raw -> Parquet particionado por ano.

Parametros (Default arguments do Job):
  --BUCKET_NAME  nome do bucket S3 (sem s3://)
  --JOB_NAME     injetado automaticamente pelo Glue
"""
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from pipeline_utils import sanitize_colname, dedupe_names

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = f"s3://{args['BUCKET_NAME']}"
RAW_DIR = f"{BUCKET}/raw"
BRONZE_DIR = f"{BUCKET}/bronze"
DICT_DIR = f"{BUCKET}/dictionary"

print("RAW:", RAW_DIR)
print("BRONZE:", BRONZE_DIR)

RAW_FILES = {
    2023: f"{RAW_DIR}/2023.csv",
    2024: f"{RAW_DIR}/2024.csv",
    2025: f"{RAW_DIR}/2025.csv",
}

dfs_bronze = {}
for ano, path in RAW_FILES.items():
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(path)
    )
    dfs_bronze[ano] = df
    print(f"{ano}: {df.count():,} linhas x {len(df.columns)} colunas")

column_maps = {}
for ano, df in dfs_bronze.items():
    original_cols = df.columns
    sanitized = dedupe_names([sanitize_colname(c) for c in original_cols])
    dfs_bronze[ano] = df.toDF(*sanitized)
    column_maps[ano] = list(zip(original_cols, sanitized))
    print(f"{ano}: exemplo -> {column_maps[ano][:3]}")

for ano, mapping in column_maps.items():
    rows = [(bruta, saneada) for bruta, saneada in mapping]
    df_map = spark.createDataFrame(rows, ["coluna_bruta", "coluna_saneada"])
    out = f"{DICT_DIR}/bronze_column_mapping_{ano}"
    df_map.coalesce(1).write.mode("overwrite").option("header", True).csv(out)
    print(f"Mapa salvo: {out}")

ingestion_ts = datetime.now(timezone.utc).isoformat()
for ano, df in dfs_bronze.items():
    dfs_bronze[ano] = (
        df
        .withColumn("_ano_pesquisa", F.lit(ano))
        .withColumn("_arquivo_origem", F.lit(f"{ano}.csv"))
        .withColumn("_data_ingestao", F.lit(ingestion_ts))
    )

for ano, df in dfs_bronze.items():
    out_path = f"{BRONZE_DIR}/ano={ano}"
    df.write.mode("overwrite").parquet(out_path)
    print(f"Salvo: {out_path}")

for ano in RAW_FILES:
    df_check = spark.read.parquet(f"{BRONZE_DIR}/ano={ano}")
    original = dfs_bronze[ano].count()
    status = "OK" if df_check.count() == original else "DIVERGENTE"
    print(f"{ano}: {df_check.count()} linhas | original {original} | {status}")

job.commit()
print("Bronze concluida.")
