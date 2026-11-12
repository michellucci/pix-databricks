# Databricks notebook source
from pyspark.sql import functions as F

silver = spark.table("workspace.silver.pix_municipio")
tabela_gold = "workspace.gold.pix_estado_mes"

gold = (
    silver
    .filter(F.col("codigo_ibge").rlike(r"^[0-9]{7}$"))
    .groupBy("competencia", "estado")
    .agg(
        F.sum("qtd_pix_enviados_pf").alias("qtd_pix_enviados_pf"),
        F.sum("qtd_pix_enviados_pj").alias("qtd_pix_enviados_pj"),
        F.sum("valor_pix_enviados_pf").alias("valor_pix_enviados_pf"),
        F.sum("valor_pix_enviados_pj").alias("valor_pix_enviados_pj"),
    )
    .withColumn(
        "qtd_pix_enviados",
        F.col("qtd_pix_enviados_pf") + F.col("qtd_pix_enviados_pj"),
    )
    .withColumn(
        "valor_pix_enviados",
        F.col("valor_pix_enviados_pf") + F.col("valor_pix_enviados_pj"),
    )
    .withColumn(
        "ticket_medio_pix",
        F.when(
            F.col("qtd_pix_enviados") > 0,
            (F.col("valor_pix_enviados") / F.col("qtd_pix_enviados"))
            .cast("decimal(20,2)"),
        ),
    )
    .withColumn(
        "ticket_medio_pix_pf",
        F.when(
            F.col("qtd_pix_enviados_pf") > 0,
            (F.col("valor_pix_enviados_pf") / F.col("qtd_pix_enviados_pf"))
            .cast("decimal(20,2)"),
        ),
    )
    .withColumn(
        "ticket_medio_pix_pj",
        F.when(
            F.col("qtd_pix_enviados_pj") > 0,
            (F.col("valor_pix_enviados_pj") / F.col("qtd_pix_enviados_pj"))
            .cast("decimal(20,2)"),
        ),
    )
)

gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela_gold)

display(
    spark.table(tabela_gold)
    .orderBy(F.col("competencia").desc(), F.col("valor_pix_enviados").desc())
)