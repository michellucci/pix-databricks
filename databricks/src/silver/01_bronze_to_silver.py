# Databricks notebook source
import sys

from pyspark.sql import functions as F

# Cole o caminho da pasta do Workspace que contém formatacao.py.
pasta_modulos = "/Workspace/Users/michellucci_ti@hotmail.com/pix/utils"

if pasta_modulos not in sys.path:
    sys.path.insert(0, pasta_modulos)

from functions import formatar_nome

# COMMAND ----------

bronze = spark.table("workspace.bronze.pix_municipio")
tabela_silver = "workspace.silver.pix_municipio"

codigo_ibge = F.trim(F.col("Municipio_Ibge").cast("string"))

silver = bronze.select(
    F.to_date(
        F.concat(F.col("AnoMes").cast("string"), F.lit("01")),
        "yyyyMMdd",
    ).alias("competencia"),
    F.when(codigo_ibge != "", codigo_ibge).alias("codigo_ibge"),
    formatar_nome(F.col("Municipio")).alias("municipio"),
    formatar_nome(F.col("Estado")).alias("estado"),
    F.col("QT_PagadorPF").cast("bigint").alias("qtd_pix_enviados_pf"),
    F.col("QT_PagadorPJ").cast("bigint").alias("qtd_pix_enviados_pj"),
    F.col("VL_PagadorPF").cast("decimal(20,2)").alias("valor_pix_enviados_pf"),
    F.col("VL_PagadorPJ").cast("decimal(20,2)").alias("valor_pix_enviados_pj"),
    "_arquivo_origem",
    "_ingerido_em",
)

silver.write.format("delta").mode("overwrite").saveAsTable(tabela_silver)

# COMMAND ----------

# MAGIC %skip
# MAGIC silver_gravada = spark.table(tabela_silver)
# MAGIC
# MAGIC display(silver_gravada.groupBy("competencia").count().orderBy("competencia"))
# MAGIC
# MAGIC display(
# MAGIC     silver_gravada
# MAGIC     .select("municipio", "estado")
# MAGIC     .distinct()
# MAGIC     .filter(F.col("municipio").rlike(r"(?U)\b(da|de|das|do|dos|no|na)\b"))
# MAGIC     .orderBy("municipio")
# MAGIC     .limit(15)
# MAGIC )