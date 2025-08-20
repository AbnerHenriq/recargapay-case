from pyspark.sql.functions import col, when, upper, regexp_replace, current_timestamp, length, concat, lit, expr
from pyspark.sql.types import StringType

SOURCE_TABLE = "`recarga-pay`.bronze.users"
TARGET_TABLE = "`recarga-pay`.silver.users"
TABLE_COMMENT = "Usuários com limpeza avançada (telefone padronizado, textos em maiúsculas), transformações movidas da Bronze para Silver."

# --- Lógica de Transformação ---
print(f"Iniciando transformação Silver para: {TARGET_TABLE}")

df_bronze = spark.table(SOURCE_TABLE)

# Aplicar as transformações de limpeza e padronização (que estavam na Bronze)
df_cleaned = (df_bronze
    # Adiciona uma coluna 'telefone_limpo' para o tratamento sequencial
    .withColumn("telefone_limpo", regexp_replace(col("telefone"), "[^0-9]", ""))
    
    # Aplica a lógica de padronização do telefone
    .withColumn("telefone_normalizado",
        # REGRA 1: Corrige o prefixo '550' incorreto, removendo o '0'
        when(col("telefone_limpo").startswith("550"), concat(lit("55"), expr("substring(telefone_limpo, 4)")))
        
        # REGRA 2: Se já começa com '55' e tem tamanho válido (12 ou 13 dígitos), já está correto.
        .when(col("telefone_limpo").startswith("55") & length(col("telefone_limpo")).isin(12, 13), col("telefone_limpo"))
        
        # REGRA 3: Se começa com '0' (ligação nacional), remove o '0' e adiciona '55'
        .when(col("telefone_limpo").startswith("0") & length(col("telefone_limpo")).isin(10, 11), concat(lit("55"), expr("substring(telefone_limpo, 2)")))
        
        # REGRA 4: Se tem 10 ou 11 dígitos (DDD + Número), apenas adiciona '55'
        .when(length(col("telefone_limpo")).isin(10, 11), concat(lit("55"), col("telefone_limpo")))
        
        # Se não se encaixar em nenhuma regra, mantém o número limpo para análise posterior
        .otherwise(col("telefone_limpo"))
    )
    # Substitui a coluna original de telefone pela versão normalizada e remove as colunas intermediárias
    .withColumn("telefone", col("telefone_normalizado"))
    .drop("telefone_limpo", "telefone_normalizado")
    
    # Mantém as outras transformações
    .withColumn("genero",
        when(upper(col("genero")).isin("F", "FEM"), "Feminino")
        .when(upper(col("genero")).isin("M", "MASC"), "Masculino")
        .otherwise("Outro")
    )
    .withColumn("estado_civil", regexp_replace(col("estado_civil"), "/a", ""))
    .withColumn("_silver_processing_timestamp", current_timestamp())
)

# Converter todos os campos de letras para maiúsculo e deixar padronizado
string_columns = [field.name for field in df_cleaned.schema.fields if isinstance(field.dataType, StringType)]

df_final = df_cleaned
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
print("Amostra dos dados com telefone normalizado:")
df_final.select("user_id", "telefone").show(10, truncate=False)
