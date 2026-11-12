# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

origem = "/Volumes/workspace/raw/pix_raw/"
tabela = "workspace.bronze.pix_municipio"

# COMMAND ----------

# Identifica a estrutura dos JSONs sem definir tipos de negócio.
estrutura = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "FAILFAST")
    .json(origem + "*.json")
    .schema
)

# Lê os bytes originais para identificar arquivos de conteúdo idêntico.
arquivos = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.json")
    .load(origem)
    .select(
        F.col("path").alias("_arquivo_origem"),
        F.col("modificationTime").alias("_arquivo_modificado_em"),
        F.sha2("content", 256).alias("_hash_arquivo"),
        F.col("content").cast("string").alias("_json")
    )
)

lote = (
    arquivos
    .withColumn(
        "_resposta",
        F.from_json("_json", estrutura, {"mode": "FAILFAST"})
    )
    .select(
        "_arquivo_origem",
        "_arquivo_modificado_em",
        "_hash_arquivo",
        F.posexplode("_resposta.value").alias("_posicao_registro", "_registro")
    )
    .select(
        "_registro.*",
        "_arquivo_origem",
        "_arquivo_modificado_em",
        "_hash_arquivo",
        "_posicao_registro"
    )
    .withColumn("_ingerido_em", F.current_timestamp())
)

# COMMAND ----------

# Escolhe o arquivo mais recente de cada competência.
# Todas as linhas do mesmo arquivo recebem a mesma classificação.
ordem_arquivo = Window.partitionBy("AnoMes").orderBy(
    F.col("_arquivo_modificado_em").desc(),
    F.col("_arquivo_origem").desc()
)

atual = (
    lote
    .withColumn("_ordem_arquivo", F.dense_rank().over(ordem_arquivo))
    .filter(F.col("_ordem_arquivo") == 1)
    .drop("_ordem_arquivo")
)

chaves = ["AnoMes", "Municipio_Ibge"]

# A chave precisa identificar uma única linha na coleta.
duplicadas = (
    atual.groupBy(*chaves)
    .count()
    .filter(F.col("count") > 1)
)

if duplicadas.limit(1).count():
    display(duplicadas)
    raise ValueError("A coleta contém chaves repetidas. Precisamos inspecioná-las.")

if not spark.catalog.tableExists(tabela):
    atual.write.format("delta").mode("error").saveAsTable(tabela)
else:
    destino = spark.table(tabela)

    # Detecta versões acumuladas pelo código anterior.
    duplicadas_destino = (
        destino.groupBy(*chaves)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicadas_destino.limit(1).count():
        raise ValueError(
            "A Bronze já contém várias versões da mesma chave. "
            "Precisamos consolidá-la antes de aplicar este MERGE."
        )

    colunas_dados = [
        coluna for coluna in atual.columns
        if not coluna.startswith("_")
    ]

    comparacao_chaves = " AND ".join(
        f"destino.`{coluna}` <=> origem.`{coluna}`"
        for coluna in chaves
    )

    comparacao_dados = " AND ".join(
        f"destino.`{coluna}` <=> origem.`{coluna}`"
        for coluna in colunas_dados
    )

    (
        DeltaTable.forName(spark, tabela)
        .alias("destino")
        .merge(atual.alias("origem"), comparacao_chaves)
        .whenMatchedUpdateAll(condition=f"NOT ({comparacao_dados})")
        .whenNotMatchedInsertAll()
        .execute()
    )

display(
    spark.table(tabela)
    .groupBy("AnoMes")
    .count()
    .orderBy("AnoMes")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.pix_municipio LIMIT 7;