from pyspark.sql.functions import current_timestamp

SOURCE_TABLE = "`recarga-pay`.bronze.transactions"
TARGET_TABLE = "`recarga-pay`.silver.transactions"
TABLE_COMMENT = "Transações com transformações aplicadas (movidas da Bronze para Silver)."

# --- Lógica de Transformação ---
print(f"Iniciando transformação Silver para: {TARGET_TABLE}")

# Ler dados da Bronze
df_bronze = spark.table(SOURCE_TABLE)

# Aplicar transformações (que estavam na Bronze)
df_silver = df_bronze.withColumn("_silver_processing_timestamp", current_timestamp())

# Escrever na tabela Silver
(df_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))

# Adicionar um comentário à tabela para governança
spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS '{TABLE_COMMENT}'")

print(f"Transformação Silver concluída para {TARGET_TABLE}")

# --- Verificação ---
print("Amostra dos dados Silver:")
spark.table(TARGET_TABLE).show(5, truncate=False)
