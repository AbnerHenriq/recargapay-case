from pyspark.sql.functions import col, current_timestamp, when, round
from pyspark.sql.types import DecimalType

SOURCE_TABLE = "`recarga-pay`.silver.transactions"
TARGET_TABLE = "`recarga-pay`.gold.fact_transactions"
TABLE_COMMENT = "Fato de transações com métricas financeiras derivadas para análise dimensional."

# --- Lógica de Transformação Gold ---
print(f"Iniciando criação da tabela de fatos: {TARGET_TABLE}")

# Criar schema gold se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS `recarga-pay`.gold")

# Ler dados da tabela Silver
df_silver = spark.table(SOURCE_TABLE)

# Aplicar apenas transformações financeiras da própria transação
df_fact_transactions = (df_silver
    # Criar colunas derivadas de métricas financeiras
    .withColumn("net_revenue", 
        round(col("transaction_fee") - col("cashback"), 2))
    
    .withColumn("cashback_fee_ratio",
        when(col("transaction_fee") > 0,
            round(col("cashback") / col("transaction_fee"), 4))
        .otherwise(None)
    )
    
    .withColumn("cashback_tpv_ratio",
        when(col("product_amount") > 0,
            round(col("cashback") / col("product_amount"), 4))
        .otherwise(None)
    )
    
    # Adicionar metadados de processamento Gold
    .withColumn("_gold_processing_timestamp", current_timestamp())
)

# Selecionar apenas as colunas relevantes para a tabela de fatos
df_final = df_fact_transactions.select(
        # Chaves e identificadores
        "transaction_id",
        "user_id",
        
        # Dimensões temporais (originais, sem transformação)
        "transaction_date",
        
        # Atributos da transação
        "product_category",
        "product_name",
        "merchant_name",
        "payment_method",
        
        # Valores monetários originais
        "product_amount",
        "transaction_fee",
        "cashback",
        
        # Métricas derivadas financeiras
        "net_revenue",
        "cashback_fee_ratio",
        "cashback_tpv_ratio",
        
        # Metadados
        "_gold_processing_timestamp"
    )

# Escrever na tabela Gold
(df_final.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))

# Adicionar comentário à tabela para governança
spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS '{TABLE_COMMENT}'")

print(f"Tabela de fatos Gold {TARGET_TABLE} criada com sucesso!")

# --- Verificação ---
print("Amostra dos dados da tabela de fatos:")
df_final.show(5, truncate=False)

print(f"Total de transações na tabela de fatos: {df_final.count()}")

# Verificar distribuição por categoria
print("\nDistribuição de transações por categoria:")
df_final.groupBy("product_category").count().orderBy("count", ascending=False).show()
