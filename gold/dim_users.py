from pyspark.sql.functions import current_timestamp, col, sum, when, coalesce, lit

SOURCE_TABLE_USERS = "`recarga-pay`.silver.users"
SOURCE_TABLE_TRANSACTIONS = "`recarga-pay`.silver.transactions"
TARGET_TABLE = "`recarga-pay`.gold.dim_users"
TABLE_COMMENT = "Dimensão de usuários com perfil do cliente, atributos estáticos e métricas agregadas (TPV lifetime, cashback, loyalty points) para análise dimensional."

# --- Lógica de Transformação Gold ---
print(f"Iniciando criação da dimensão Gold: {TARGET_TABLE}")

# Criar schema gold se não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS `recarga-pay`.gold")

df_silver_users = spark.table(SOURCE_TABLE_USERS)
df_silver_transactions = spark.table(SOURCE_TABLE_TRANSACTIONS)

# Calcular métricas agregadas por usuário das transações
df_user_metrics = (df_silver_transactions
    .groupBy("user_id")
    .agg(
        # TPV Lifetime - soma total de todas as transações por usuário
        coalesce(sum("tpv"), lit(0)).alias("tpv_lifetime"),
        
        # Cashback - soma total de cashback por usuário
        coalesce(sum("cashback"), lit(0)).alias("total_cashback"),
        
        # Loyalty Points - soma total de pontos de fidelidade por usuário
        coalesce(sum("loyalty_points"), lit(0)).alias("total_loyalty_points")
    )
)

# Fazer join entre usuários e métricas calculadas
df_dim_users = (df_silver_users
    .join(df_user_metrics, "user_id", "left")
    .select(
        "user_id",
        "name",
        "email", 
        "phone",
        "birth_date",
        "gender",
        "marital_status",
        "occupation",
        "income_range",
        "state",
        "device",
        "activation_date",
        # Adicionar as métricas calculadas
        "tpv_lifetime",
        "total_cashback",
        "total_loyalty_points"
    )
    # Adicionar metadados de processamento Gold
    .withColumn("_gold_processing_timestamp", current_timestamp())
)

# Escrever na tabela Gold
(df_dim_users.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))

# Adicionar comentário à tabela para governança
spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS '{TABLE_COMMENT}'")

print(f"Dimensão Gold {TARGET_TABLE} criada com sucesso!")

# --- Verificação ---
print("Amostra dos dados da dimensão de usuários:")
df_dim_users.show(5, truncate=False)

print(f"Total de usuários na dimensão: {df_dim_users.count()}")
