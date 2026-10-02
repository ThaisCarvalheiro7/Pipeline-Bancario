# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install azure-storage-blob

# COMMAND ----------

import io
from datetime import date

import pandas as pd
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------


# COMMAND ----------

STORAGE_ACCOUNT = "stpipelinebancario"
CONTAINER_LANDING = "landing"

account_key = dbutils.secrets.get(scope="adls-scope", key="storage-account-key")
account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"

blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)

# COMMAND ----------


# COMMAND ----------

data_ingestao = "2026-09-06"
ano, mes, dia = data_ingestao.split("-")

TABELAS = ["clientes", "contas", "transacoes"]

# COMMAND ----------


# COMMAND ----------


def ingerir_tabela(nome_tabela: str):
    caminho_blob = f"{nome_tabela}/ano={ano}/mes={mes}/dia={dia}/{nome_tabela}.csv"

    print(f"Baixando: {CONTAINER_LANDING}/{caminho_blob}")

    container_client = blob_service_client.get_container_client(CONTAINER_LANDING)
    blob_client = container_client.get_blob_client(caminho_blob)

    conteudo = blob_client.download_blob().readall()
    df_pandas = pd.read_csv(io.BytesIO(conteudo))

    df_spark = spark.createDataFrame(df_pandas)

    df_bronze = (
        df_spark.withColumn("_arquivo_origem", lit(caminho_blob))
        .withColumn("_data_ingestao", lit(data_ingestao))
        .withColumn("_data_carga_bronze", current_timestamp())
    )

    (
        df_bronze.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"bronze.{nome_tabela}")
    )

    print(f"  -> {df_bronze.count()} linhas gravadas em bronze.{nome_tabela}")


# COMMAND ----------


# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

for tabela in TABELAS:
    ingerir_tabela(tabela)

print("\nIngestão Bronze concluída.")