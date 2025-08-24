from pyspark.sql.functions import col, current_timestamp, upper
from pyspark.sql.types import StringType

SOURCE_TABLE = "`recarga-pay`.bronze.transactions"
TARGET_TABLE = "`recarga-pay`.silver.transactions"
TABLE_COMMENT = "Transações com transformações aplicadas (movidas da Bronze para Silver) e padronização UPPER."

# --- Lógica de Transformação ---
print(f"Iniciando transformação Silver para: {TARGET_TABLE}")

# Ler dados da Bronze
df_bronze = spark.table(SOURCE_TABLE)

# Remover coluna _bronze_ingestion_timestamp (as colunas já estão em inglês na bronze)
df_cleaned = df_bronze.drop("_bronze_ingestion_timestamp")

# Aplicar transformações (que estavam na Bronze)
df_silver = df_cleaned.withColumn("_silver_processing_timestamp", current_timestamp())

# Converter todos os campos de letras para maiúsculo e deixar padronizado
string_columns = [field.name for field in df_silver.schema.fields if isinstance(field.dataType, StringType)]

df_final = df_silver
for col_name in string_columns:
    df_final = df_final.withColumn(col_name, upper(col(col_name)))

# Escrever na tabela Silver
(df_final.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))

# Adicionar um comentário à tabela para governança
spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS '{TABLE_COMMENT}'")

print(f"Transformação Silver concluída para {TARGET_TABLE}")

# --- Verificação ---
print("Amostra dos dados Silver:")
spark.table(TARGET_TABLE).show(5, truncate=False)
