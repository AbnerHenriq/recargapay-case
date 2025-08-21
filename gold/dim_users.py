from pyspark.sql.functions import current_timestamp

SOURCE_TABLE = "`recarga-pay`.silver.users"
TARGET_TABLE = "`recarga-pay`.gold.dim_users"
TABLE_COMMENT = "Dimensão de usuários com perfil do cliente e atributos estáticos para análise dimensional."

# --- Lógica de Transformação Gold ---
print(f"Iniciando criação da dimensão Gold: {TARGET_TABLE}")

df_silver = spark.table(SOURCE_TABLE)

# Apenas seleção de colunas relevantes para a dimensão (sem transformações)
df_dim_users = (df_silver
    .select(
        "user_id",
        "nome",
        "email", 
        "telefone",
        "data_nascimento",
        "genero",
        "estado_civil",
        "ocupacao",
        "faixa_renda",
        "estado",
        "dispositivo",
        "data_cadastro"
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
