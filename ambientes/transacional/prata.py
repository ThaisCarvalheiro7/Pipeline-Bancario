# Databricks notebook source
# MAGIC %md
# MAGIC # Transformação Silver — Pipeline Bancário

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver — Clientes

# COMMAND ----------

df_clientes_bronze = spark.table("bronze.clientes")

df_clientes_silver = (
    df_clientes_bronze
    .dropDuplicates(["id_cliente"])
    .withColumn("email_informado", F.col("email").isNotNull())
    .withColumn("email", F.coalesce(F.col("email"), F.lit("nao_informado")))
    .select(
        "id_cliente", "nome", "cpf", "data_nascimento",
        "email", "email_informado", "cidade", "estado",
        "_data_ingestao",
    )
)

df_clientes_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver.clientes")

print(f"silver.clientes: {df_clientes_silver.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver — Contas

# COMMAND ----------

df_contas_bronze = spark.table("bronze.contas")

df_contas_silver = df_contas_bronze.dropDuplicates(["id_conta"]).select(
    "id_conta", "id_cliente", "agencia", "tipo_conta",
    "data_abertura", "saldo_inicial", "_data_ingestao",
)

df_contas_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver.contas")

print(f"silver.contas: {df_contas_silver.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — Transações

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Padronizar os 3 formatos de data

# COMMAND ----------

df_transacoes_bronze = spark.table("bronze.transacoes")

df_data_padronizada = df_transacoes_bronze.withColumn(
    "data_transacao_ts",
    F.coalesce(
        F.to_timestamp("data_transacao", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("data_transacao", "dd/MM/yyyy HH:mm"),
        F.to_timestamp("data_transacao", "yyyy-MM-dd'T'HH:mm:ss"),
    ),
)

qtd_data_invalida = df_data_padronizada.filter(
    F.col("data_transacao_ts").isNull()
).count()
print(f"Linhas com data em formato não reconhecido: {qtd_data_invalida}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Remover duplicatas

# COMMAND ----------

qtd_antes = df_data_padronizada.count()
df_sem_duplicatas = df_data_padronizada.dropDuplicates(["id_transacao"])
qtd_depois = df_sem_duplicatas.count()

print(f"Duplicatas removidas: {qtd_antes - qtd_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Regras de negócio e integridade referencial

# COMMAND ----------

contas_validas = df_contas_silver.select(F.col("id_conta").alias("id_conta_valida"))

df_avaliada = (
    df_sem_duplicatas
    .join(
        contas_validas,
        df_sem_duplicatas.id_conta_origem == contas_validas.id_conta_valida,
        "left",
    )
    .withColumn("origem_existe", F.col("id_conta_valida").isNotNull())
    .drop("id_conta_valida")
    .join(
        contas_validas,
        df_sem_duplicatas.id_conta_destino == contas_validas.id_conta_valida,
        "left",
    )
    .withColumn(
        "destino_valido",
        F.col("id_conta_destino").isNull() | F.col("id_conta_valida").isNotNull(),
    )
    .drop("id_conta_valida")
    .withColumn("valor_valido", F.col("valor") > 0)
    .withColumn("data_valida", F.col("data_transacao_ts").isNotNull())
    .withColumn(
        "transacao_valida",
        F.col("origem_existe")
        & F.col("destino_valido")
        & F.col("valor_valido")
        & F.col("data_valida"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Separar válidas de quarentena

# COMMAND ----------

colunas_finais = [
    "id_transacao", "id_conta_origem", "id_conta_destino", "tipo_transacao",
    "valor", "data_transacao_ts", "canal", "_data_ingestao",
]

df_transacoes_validas = df_avaliada.filter(F.col("transacao_valida")).select(
    *colunas_finais
).withColumnRenamed("data_transacao_ts", "data_transacao")

df_transacoes_quarentena = df_avaliada.filter(~F.col("transacao_valida")).select(
    *colunas_finais,
    "origem_existe", "destino_valido", "valor_valido", "data_valida",
)

df_transacoes_validas.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver.transacoes")

df_transacoes_quarentena.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver.transacoes_quarentena")

print(f"\nsilver.transacoes (válidas):      {df_transacoes_validas.count()}")
print(f"silver.transacoes_quarentena:       {df_transacoes_quarentena.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumo da execução

# COMMAND ----------

print("Transformação Silver concluída.\n")
print(f"clientes:              {df_clientes_silver.count()}")
print(f"contas:                {df_contas_silver.count()}")
print(f"transacoes (válidas):  {df_transacoes_validas.count()}")
print(f"transacoes (quarentena): {df_transacoes_quarentena.count()}")