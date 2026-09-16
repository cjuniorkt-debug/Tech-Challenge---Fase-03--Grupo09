"""Glue Job 02 — Silver: unifica Bronze 2023/2024/2025 -> pesquisa_unificada.

Parametros:
  --BUCKET_NAME  nome do bucket S3 (sem s3://)
  --JOB_NAME     injetado automaticamente pelo Glue
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = f"s3://{args['BUCKET_NAME']}"
BRONZE_DIR = f"{BUCKET}/bronze"
SILVER_DIR = f"{BUCKET}/silver"
DICT_PATH = f"{BUCKET}/dictionary/mapa_campos.csv"

print("BRONZE:", BRONZE_DIR)
print("SILVER:", SILVER_DIR)
print("DICT:", DICT_PATH)

dfs_bronze = {}
for ano in (2023, 2024, 2025):
    dfs_bronze[ano] = spark.read.parquet(f"{BRONZE_DIR}/ano={ano}")
    print(f"{ano}: {dfs_bronze[ano].count():,} linhas x {len(dfs_bronze[ano].columns)} colunas")

mapa_campos = spark.read.option("header", True).csv(DICT_PATH).toPandas()
print(f"{len(mapa_campos)} campos no dicionario")

problemas = []
for ano, col_dicionario in [(2023, "coluna_2023"), (2024, "coluna_2024"), (2025, "coluna_2025")]:
    colunas_reais = set(dfs_bronze[ano].columns)
    for _, row in mapa_campos.dropna(subset=[col_dicionario]).iterrows():
        if row[col_dicionario] not in colunas_reais:
            problemas.append((ano, row["campo_padronizado"], row[col_dicionario]))

if problemas:
    print(f"ATENCAO: {len(problemas)} colunas do dicionario nao encontradas:")
    for p in problemas:
        print(" ", p)
else:
    print("OK: todas as colunas do dicionario existem na Bronze.")


def selecionar_e_renomear(df_bronze, mapa, coluna_ano):
    mapa_ano = mapa.dropna(subset=[coluna_ano])
    colunas_lineage = ["_ano_pesquisa", "_arquivo_origem", "_data_ingestao"]
    exprs = [F.col(c).alias(c) for c in colunas_lineage]
    exprs += [
        F.col(row[coluna_ano]).alias(row["campo_padronizado"])
        for _, row in mapa_ano.iterrows()
    ]
    return df_bronze.select(*exprs)


df_2023_silver = selecionar_e_renomear(dfs_bronze[2023], mapa_campos, "coluna_2023")
df_2024_silver = selecionar_e_renomear(dfs_bronze[2024], mapa_campos, "coluna_2024")
df_2025_silver = selecionar_e_renomear(dfs_bronze[2025], mapa_campos, "coluna_2025")

df_silver = (
    df_2023_silver
    .unionByName(df_2024_silver, allowMissingColumns=True)
    .unionByName(df_2025_silver, allowMissingColumns=True)
)
print(f"Silver unificado: {df_silver.count():,} linhas x {len(df_silver.columns)} colunas")

df_silver = (
    df_silver
    .withColumn("data_hora_envio", F.to_timestamp("data_hora_envio", "dd/MM/yyyy HH:mm:ss"))
    .withColumn("idade", F.col("idade").cast(IntegerType()))
    .withColumn("_ano_pesquisa", F.col("_ano_pesquisa").cast(IntegerType()))
)

campos_booleanos = [
    "atua_como_gestor", "possui_data_lake", "possui_data_warehouse",
    "satisfeito_atualmente", "vive_no_brasil", "vive_no_estado_de_formacao",
]
mapa_bool = {"1": "Sim", "TRUE": "Sim", "0": "Não", "FALSE": "Não"}
bool_expr = F.create_map([F.lit(x) for pair in mapa_bool.items() for x in pair])
for c in campos_booleanos:
    df_silver = df_silver.withColumn(c, bool_expr[F.col(c)])

mapa_consolidacao = {
    "cargo_atual": {
        "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect":
            "Engenheiro de Dados/Data Engineer/Data Architect",
    },
    "ai_generativa_e_llm_e_uma_prioridade": {
        "Mais ou menos... É uma das várias iniciativas que estamos impulsionando, mas não é uma prioridade (iniciativas isoladas e pouco foco).":
            "Mais ou menos... É uma das várias iniciativas que estamos impulsionando, mas não é uma prioridade (tratam-se de iniciativas isoladas e com pouco foco).",
    },
    "numero_de_funcionarios": {
        "de 501 a 100": "de 501 a 1.000",
    },
    "faixa_salarial": {
        "de R$ 101/mês a R$ 2.000/mês": "de R$ 1.001/mês a R$ 2.000/mês",
        "de R$ 25.001/mês a R$ 3000/mês": "de R$ 25.001/mês a R$ 30.000/mês",
    },
}
for coluna, mapa in mapa_consolidacao.items():
    expr = F.col(coluna)
    for de, para in mapa.items():
        expr = F.when(F.col(coluna) == de, F.lit(para)).otherwise(expr)
    df_silver = df_silver.withColumn(coluna, expr)

print("Padronizacao aplicada.")
df_silver.groupBy("_ano_pesquisa").count().orderBy("_ano_pesquisa").show()

dup = (
    df_silver.groupBy("_ano_pesquisa", "token").count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"Tokens duplicados no mesmo ano: {dup}")

out_path = f"{SILVER_DIR}/pesquisa_unificada"
df_silver.coalesce(1).write.mode("overwrite").parquet(out_path)
print(f"Salvo: {out_path}")

df_check = spark.read.parquet(out_path)
print(f"Validacao: {df_check.count()} linhas x {len(df_check.columns)} colunas")

job.commit()
print("Silver concluida.")
