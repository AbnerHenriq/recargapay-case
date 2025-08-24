from pyspark.sql.functions import col, when, upper, regexp_replace, current_timestamp, length, concat, lit, expr, datediff, current_date
from pyspark.sql.types import StringType
from macro_transform import normalize_state_to_uf

SOURCE_TABLE = "`recarga-pay`.bronze.users"
TARGET_TABLE = "`recarga-pay`.silver.users"
TABLE_COMMENT = "Usuários com limpeza avançada (telefone padronizado, textos em maiúsculas), transformações movidas da Bronze para Silver, colunas em inglês e categorização de faixa etária."

# --- Lógica de Transformação ---
print(f"Iniciando transformação Silver para: {TARGET_TABLE}")

df_bronze = spark.table(SOURCE_TABLE)

# Remover coluna _bronze_ingestion_timestamp e renomear colunas para inglês
df_renamed = (df_bronze
    .drop("_bronze_ingestion_timestamp")
    .withColumnRenamed("id_usuario", "user_id")
    .withColumnRenamed("nome", "name")
    .withColumnRenamed("email", "email")
    .withColumnRenamed("telefone", "phone")
    .withColumnRenamed("data_nascimento", "birth_date")
    .withColumnRenamed("genero", "gender")
    .withColumnRenamed("estado_civil", "marital_status")
    .withColumnRenamed("ocupacao", "occupation")
    .withColumnRenamed("faixa_renda", "income_range")
    .withColumnRenamed("estado", "state")
    .withColumnRenamed("dispositivo", "device")
    .withColumnRenamed("data_ativacao", "activation_date")
)

# Aplicar as transformações de limpeza e padronização (que estavam na Bronze)
df_cleaned = (df_renamed
    # Adiciona uma coluna 'telefone_limpo' para o tratamento sequencial
    .withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))
    
    # Aplica a lógica de padronização do telefone
    .withColumn("phone_normalized",
        # REGRA 1: Corrige o prefixo '550' incorreto, removendo o '0'
        when(col("phone_clean").startswith("550"), concat(lit("55"), expr("substring(phone_clean, 4)")))
        
        # REGRA 2: Se já começa com '55' e tem tamanho válido (12 ou 13 dígitos), já está correto.
        .when(col("phone_clean").startswith("55") & length(col("phone_clean")).isin(12, 13), col("phone_clean"))
        
        # REGRA 3: Se começa com '0' (ligação nacional), remove o '0' e adiciona '55'
        .when(col("phone_clean").startswith("0") & length(col("phone_clean")).isin(10, 11), concat(lit("55"), expr("substring(phone_clean, 2)")))
        
        # REGRA 4: Se tem 10 ou 11 dígitos (DDD + Número), apenas adiciona '55'
        .when(length(col("phone_clean")).isin(10, 11), concat(lit("55"), col("phone_clean")))
        
        # Se não se encaixar em nenhuma regra, mantém o número limpo para análise posterior
        .otherwise(col("phone_clean"))
    )
    # Substitui a coluna original de telefone pela versão normalizada e remove as colunas intermediárias
    .withColumn("phone", col("phone_normalized"))
    .drop("phone_clean", "phone_normalized")
    
    # Mantém as outras transformações
    .withColumn("gender",
        when(upper(col("gender")).isin("F", "FEM"), "FEMININO")
        .when(upper(col("gender")).isin("M", "MASC"), "MASCULINO")
        .otherwise("OUTRO")
    )
    .withColumn("marital_status", regexp_replace(col("marital_status"), "/a", ""))
    
    # Adicionar categorização de faixa etária
    .withColumn(
        "faixa_etaria",
        when(datediff(current_date(), col("birth_date")) / 365.25 < 18, "MENOR_18")
        .when(datediff(current_date(), col("birth_date")) / 365.25 <= 25, "18-25")
        .when(datediff(current_date(), col("birth_date")) / 365.25 <= 35, "26-35")
        .when(datediff(current_date(), col("birth_date")) / 365.25 <= 45, "36-45")
        .when(datediff(current_date(), col("birth_date")) / 365.25 <= 60, "46-60")
        .otherwise("60+")
    )
    
    .withColumn("_silver_processing_timestamp", current_timestamp())
)

# Normalizar estados para UF antes de converter para maiúsculo
df_final = normalize_state_to_uf(df_cleaned, "state")

# Converter todos os campos de letras para maiúsculo e deixar padronizado
string_columns = [field.name for field in df_final.schema.fields if isinstance(field.dataType, StringType)]

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
