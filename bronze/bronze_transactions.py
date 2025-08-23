from pyspark.sql.functions import current_timestamp


SOURCE_TABLE = "`recarga-pay`.raw.raw_transactions"
TARGET_TABLE = "`recarga-pay`.bronze.transactions"
TABLE_COMMENT = "Dados brutos de transações, cópia 1:1 da origem."

# --- Lógica de Ingestão ---
print(f"Iniciando a carga da tabela: {TARGET_TABLE}")

# Criar schema bronze se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS `recarga-pay`.bronze")

# Ler a tabela de origem
df_raw = spark.table(SOURCE_TABLE)

# Adicionar metadados de ingestão
df_bronze = df_raw.withColumn("_bronze_ingestion_timestamp", current_timestamp())

# Escrever na tabela bronze, sobrescrevendo a versão anterior
(df_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))

# Adicionar um comentário à tabela para governança
spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS '{TABLE_COMMENT}'")

print(f"Carga da tabela {TARGET_TABLE} concluída com sucesso.")

# --- Verificação ---
print("Amostra dos dados carregados:")
spark.table(TARGET_TABLE).show(5, truncate=False)