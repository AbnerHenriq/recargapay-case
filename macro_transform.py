from pyspark.sql.functions import col, when

def normalize_state_to_uf(df, state_column="state"):
    """
    Normaliza nomes de estados para suas respectivas UFs (Unidades Federativas).
    
    Args:
        df: DataFrame do PySpark
        state_column: Nome da coluna que contém os estados (padrão: "state")
    
    Returns:
        DataFrame com a coluna de estado normalizada para UF
    """
    
    # Mapeamento de nomes de estados para UFs
    state_mapping = {
        "ACRE": "AC",
        "ALAGOAS": "AL", 
        "AMAPÁ": "AP",
        "AMAZONAS": "AM",
        "BAHIA": "BA",
        "CEARÁ": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPÍRITO SANTO": "ES",
        "GOIÁS": "GO",
        "MARANHÃO": "MA",
        "MATO GROSSO": "MT",
        "MATO GROSSO DO SUL": "MS",
        "MINAS GERAIS": "MG",
        "PARÁ": "PA",
        "PARAÍBA": "PB",
        "PARANÁ": "PR",
        "PERNAMBUCO": "PE",
        "PIAUÍ": "PI",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS",
        "RONDÔNIA": "RO",
        "RORAIMA": "RR",
        "SANTA CATARINA": "SC",
        "SÃO PAULO": "SP",
        "SERGIPE": "SE",
        "TOCANTINS": "TO"
    }
    
    # Aplicar o mapeamento
    df_normalized = df
    for full_name, uf in state_mapping.items():
        df_normalized = df_normalized.withColumn(
            state_column,
            when(col(state_column) == full_name, uf).otherwise(col(state_column))
        )
    
    return df_normalized
