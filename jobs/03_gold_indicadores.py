"""Glue Job 03 — Gold: gera 8 marts analiticos a partir da Silver.

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

from pipeline_utils import (
    explode_multiselect,
    descobrir_vocabulario_multiselect,
    explode_multiselect_por_vocabulario,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = f"s3://{args['BUCKET_NAME']}"
SILVER_DIR = f"{BUCKET}/silver"
GOLD_DIR = f"{BUCKET}/gold"

print("SILVER:", SILVER_DIR)
print("GOLD:", GOLD_DIR)

df_silver = spark.read.parquet(f"{SILVER_DIR}/pesquisa_unificada")
df_silver.cache()
print(f"Silver: {df_silver.count():,} linhas x {len(df_silver.columns)} colunas")

COLUNAS_RESPONDENTES = [
    "_ano_pesquisa", "token",
    "faixa_idade", "genero", "cor_raca_etnia", "pcd", "regiao_onde_mora", "uf_onde_mora",
    "nivel_de_ensino", "area_de_formacao",
    "situacao_de_trabalho", "setor", "numero_de_funcionarios",
    "cargo_atual", "nivel", "faixa_salarial",
    "tempo_de_experiencia_em_dados", "tempo_de_experiencia_em_ti",
    "atua_como_gestor", "modelo_de_trabalho_atual", "modelo_de_trabalho_ideal",
    "satisfeito_atualmente", "planos_de_mudar_de_emprego_6m", "participou_de_entrevistas_ultimos_6m",
    "ai_generativa_e_llm_e_uma_prioridade", "usa_chatgpt_ou_copilot_no_trabalho",
    "empresa_esta_conseguindo_ter_bons_resultados_com_llms",
    "possui_data_lake", "possui_data_warehouse",
]
gold_respondentes = df_silver.select(*COLUNAS_RESPONDENTES)

gold_panorama_mercado = (
    df_silver
    .groupBy("_ano_pesquisa", "situacao_de_trabalho", "setor", "numero_de_funcionarios", "modelo_de_trabalho_atual")
    .agg(F.count("*").alias("qtd_respondentes"))
    .orderBy("_ano_pesquisa", F.desc("qtd_respondentes"))
)

gold_perfis_profissionais = (
    df_silver
    .groupBy("_ano_pesquisa", "cargo_atual", "nivel", "faixa_salarial")
    .agg(F.count("*").alias("qtd_respondentes"))
    .orderBy("_ano_pesquisa", F.desc("qtd_respondentes"))
)

gold_diversidade = (
    df_silver
    .groupBy("_ano_pesquisa", "cargo_atual", "nivel", "genero", "cor_raca_etnia", "pcd")
    .agg(F.count("*").alias("qtd_respondentes"))
    .orderBy("_ano_pesquisa", F.desc("qtd_respondentes"))
)

CAMPOS_TECNOLOGIA = [
    ("banco_de_dados_dia_a_dia", "Banco de Dados"),
    ("cloud_dia_a_dia", "Cloud"),
    ("linguagem_de_programacao_dia_a_dia", "Linguagem de Programação"),
    ("ferramenta_de_bi_dia_a_dia", "Ferramenta de BI"),
]
total_respondentes_ano = (
    df_silver.groupBy("_ano_pesquisa").agg(F.countDistinct("token").alias("total_respondentes_ano"))
)
tecnologias_explodidas = []
for coluna, categoria in CAMPOS_TECNOLOGIA:
    base = df_silver.select("_ano_pesquisa", "token", coluna).filter(F.col(coluna).isNotNull())
    explodido = explode_multiselect(base, coluna, coluna_saida="tecnologia")
    explodido = explodido.withColumn("categoria", F.lit(categoria))
    tecnologias_explodidas.append(explodido)

df_tecnologias = tecnologias_explodidas[0]
for df_extra in tecnologias_explodidas[1:]:
    df_tecnologias = df_tecnologias.unionByName(df_extra)

gold_adocao_tecnologias = (
    df_tecnologias
    .groupBy("_ano_pesquisa", "categoria", "tecnologia")
    .agg(F.countDistinct("token").alias("qtd_respondentes"))
    .join(total_respondentes_ano, on="_ano_pesquisa")
    .withColumn(
        "pct_respondentes_ano",
        F.round(F.col("qtd_respondentes") / F.col("total_respondentes_ano") * 100, 1),
    )
    .drop("total_respondentes_ano")
    .orderBy("_ano_pesquisa", "categoria", F.desc("qtd_respondentes"))
)

uso_individual_base = (
    df_silver.select("_ano_pesquisa", "token", "cargo_atual", "setor", "usa_chatgpt_ou_copilot_no_trabalho")
    .filter(F.col("usa_chatgpt_ou_copilot_no_trabalho").isNotNull())
)
valores_distintos = [
    r[0] for r in uso_individual_base.select("usa_chatgpt_ou_copilot_no_trabalho").distinct().collect()
]
vocabulario_ia = descobrir_vocabulario_multiselect(valores_distintos)
print(f"{len(vocabulario_ia)} opcoes atomicas descobertas")

uso_individual = explode_multiselect_por_vocabulario(
    uso_individual_base, "usa_chatgpt_ou_copilot_no_trabalho", vocabulario_ia, coluna_saida="valor"
).withColumn("dimensao", F.lit("uso_individual_ia"))

prioridade_empresa = (
    df_silver.select(
        "_ano_pesquisa", "token", "cargo_atual", "setor",
        F.col("ai_generativa_e_llm_e_uma_prioridade").alias("valor"),
    )
    .filter(F.col("valor").isNotNull())
    .withColumn("dimensao", F.lit("prioridade_empresa_ia"))
)
resultado_llms = (
    df_silver.select(
        "_ano_pesquisa", "token", "cargo_atual", "setor",
        F.col("empresa_esta_conseguindo_ter_bons_resultados_com_llms").alias("valor"),
    )
    .filter(F.col("valor").isNotNull())
    .withColumn("dimensao", F.lit("resultado_llms_empresa"))
)
df_ia = uso_individual.unionByName(prioridade_empresa).unionByName(resultado_llms)

gold_adocao_ia = (
    df_ia
    .groupBy("_ano_pesquisa", "dimensao", "cargo_atual", "setor", "valor")
    .agg(F.countDistinct("token").alias("qtd_respondentes"))
    .orderBy("_ano_pesquisa", "dimensao", F.desc("qtd_respondentes"))
)

gold_perfil_regional = (
    df_silver
    .groupBy("_ano_pesquisa", "regiao_onde_mora", "nivel", "modelo_de_trabalho_atual")
    .agg(F.count("*").alias("qtd_respondentes"))
    .orderBy("_ano_pesquisa", F.desc("qtd_respondentes"))
)


def preparar_dimensao(coluna, nome_dimensao, multiselect=False):
    base = (
        df_silver.select(
            "_ano_pesquisa", "token", "setor", "numero_de_funcionarios",
            F.col(coluna).alias(coluna),
        )
        .filter(F.col(coluna).isNotNull())
    )
    if multiselect:
        base = explode_multiselect(base, coluna, coluna_saida="valor")
    else:
        base = base.withColumnRenamed(coluna, "valor")
    return base.withColumn("dimensao", F.lit(nome_dimensao))


dimensoes = [
    preparar_dimensao("possui_data_lake", "possui_data_lake"),
    preparar_dimensao("possui_data_warehouse", "possui_data_warehouse"),
    preparar_dimensao("desafios_como_gestor", "desafio_gestor", multiselect=True),
    preparar_dimensao("motivos_para_nao_usar_ai_generativa_e_llm", "motivo_nao_uso_ia", multiselect=True),
    preparar_dimensao("planos_de_mudar_de_emprego_6m", "risco_turnover"),
]
df_maturidade = dimensoes[0]
for d in dimensoes[1:]:
    df_maturidade = df_maturidade.unionByName(d)

gold_maturidade_e_desafios = (
    df_maturidade
    .groupBy("_ano_pesquisa", "dimensao", "setor", "numero_de_funcionarios", "valor")
    .agg(F.countDistinct("token").alias("qtd_respondentes"))
    .orderBy("_ano_pesquisa", "dimensao", F.desc("qtd_respondentes"))
)

tabelas_gold = {
    "gold_respondentes": gold_respondentes,
    "gold_panorama_mercado": gold_panorama_mercado,
    "gold_perfis_profissionais": gold_perfis_profissionais,
    "gold_diversidade": gold_diversidade,
    "gold_adocao_tecnologias": gold_adocao_tecnologias,
    "gold_adocao_ia": gold_adocao_ia,
    "gold_perfil_regional": gold_perfil_regional,
    "gold_maturidade_e_desafios": gold_maturidade_e_desafios,
}

for nome, df in tabelas_gold.items():
    print(f"{nome:32s} {df.count():>8} linhas x {len(df.columns)} colunas")

for nome, df in tabelas_gold.items():
    out_path = f"{GOLD_DIR}/{nome}"
    df.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"Salvo: {out_path}")

for nome, df in tabelas_gold.items():
    out_path = f"{GOLD_DIR}/{nome}"
    df_check = spark.read.parquet(out_path)
    status = "OK" if df_check.count() == df.count() else "DIVERGENTE"
    print(f"{nome:32s} {df_check.count()} linhas | {status}")

job.commit()
print("Gold concluida.")
