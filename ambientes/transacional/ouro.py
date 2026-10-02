# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold — Pipeline Bancário
# MAGIC Lê a Silver (já limpa e validada) e constrói tabelas de negócio prontas
# MAGIC para consumo — o tipo de tabela que alimentaria um dashboard ou um
# MAGIC relatório para a área de negócio do banco.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# COMMAND ----------

df_transacoes = spark.table("silver.transacoes")
df_contas = spark.table("silver.contas")
df_clientes = spark.table("silver.clientes")

df_transacoes_enriquecidas = (
    df_transacoes
    .join(
        df_contas.select("id_conta", "id_cliente"),
        df_transacoes.id_conta_origem == df_contas.id_conta,
        "inner",
    )
    .join(
        df_clientes.select("id_cliente", "nome", "cidade", "estado"),
        "id_cliente",
        "inner",
    )
    .withColumn("data_transacao_dia", F.to_date("data_transacao"))
)

print(f"Transações enriquecidas: {df_transacoes_enriquecidas.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Métricas por cliente e dia
# MAGIC Volume e valor total movimentado por cliente, por dia — a granularidade

# COMMAND ----------

df_metricas_cliente_dia = (
    df_transacoes_enriquecidas
    .groupBy("id_cliente", "nome", "data_transacao_dia")
    .agg(
        F.count("id_transacao").alias("qtd_transacoes"),
        F.sum("valor").alias("valor_total"),
        F.round(F.avg("valor"), 2).alias("ticket_medio"),
    )
    .orderBy("data_transacao_dia", F.desc("valor_total"))
)

df_metricas_cliente_dia.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("gold.metricas_cliente_dia")

print(f"gold.metricas_cliente_dia: {df_metricas_cliente_dia.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ticket médio por tipo de transação
# MAGIC Compara o comportamento entre PIX, TED, cartão etc
# MAGIC qual canal concentra mais volume financeiro vs. mais quantidade de operações.

# COMMAND ----------

df_ticket_medio_tipo = (
    df_transacoes_enriquecidas
    .groupBy("tipo_transacao")
    .agg(
        F.count("id_transacao").alias("qtd_transacoes"),
        F.sum("valor").alias("valor_total"),
        F.round(F.avg("valor"), 2).alias("ticket_medio"),
        F.round(F.stddev("valor"), 2).alias("desvio_padrao"),
    )
    .orderBy(F.desc("valor_total"))
)

df_ticket_medio_tipo.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("gold.ticket_medio_por_tipo")

print(f"gold.ticket_medio_por_tipo: {df_ticket_medio_tipo.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ranking de clientes por movimentação
# MAGIC Quem mais movimenta dinheiro no período
# MAGIC ou o time comercial de um banco olhariam de perto.

# COMMAND ----------

janela_ranking = Window.orderBy(F.desc("valor_total_movimentado"))

df_ranking_clientes = (
    df_transacoes_enriquecidas
    .groupBy("id_cliente", "nome", "cidade", "estado")
    .agg(
        F.count("id_transacao").alias("qtd_transacoes"),
        F.sum("valor").alias("valor_total_movimentado"),
    )
    .withColumn("posicao_ranking", F.dense_rank().over(janela_ranking))
    .orderBy("posicao_ranking")
)

df_ranking_clientes.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("gold.ranking_clientes")

print(f"gold.ranking_clientes: {df_ranking_clientes.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transações atípicas (detecção estatística de outliers)
# MAGIC Regra: uma transação é atípica quando o valor está a mais de 3 desvios-padrão
# MAGIC acima da média **do próprio tipo de transação** (comparar um PIX com a média
# MAGIC de PIX, não com a média geral, já que os tipos têm faixas de valor muito diferentes).

# COMMAND ----------

janela_por_tipo = Window.partitionBy("tipo_transacao")

df_transacoes_atipicas = (
    df_transacoes_enriquecidas
    .withColumn("media_tipo", F.avg("valor").over(janela_por_tipo))
    .withColumn("desvio_tipo", F.stddev("valor").over(janela_por_tipo))
    .withColumn(
        "limite_atipico",
        F.col("media_tipo") + (3 * F.col("desvio_tipo")),
    )
    .withColumn(
        "eh_atipica",
        F.col("valor") > F.col("limite_atipico"),
    )
    .filter(F.col("eh_atipica"))
    .select(
        "id_transacao", "id_cliente", "nome", "tipo_transacao",
        "valor", "data_transacao", "canal",
        F.round("media_tipo", 2).alias("media_tipo"),
        F.round("limite_atipico", 2).alias("limite_atipico"),
    )
    .orderBy(F.desc("valor"))
)

df_transacoes_atipicas.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("gold.transacoes_atipicas")

print(f"gold.transacoes_atipicas: {df_transacoes_atipicas.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("Camada Gold concluída.\n")
print(f"metricas_cliente_dia:    {df_metricas_cliente_dia.count()}")
print(f"ticket_medio_por_tipo:   {df_ticket_medio_tipo.count()}")
print(f"ranking_clientes:        {df_ranking_clientes.count()}")
print(f"transacoes_atipicas:     {df_transacoes_atipicas.count()}")