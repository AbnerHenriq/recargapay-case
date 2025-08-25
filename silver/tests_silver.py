import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run_silver_tests():
    results = {}

    # --- silver_users ---
    try:
        df = spark.read.table("`recarga-pay`.silver.users")
        gdf = ge.SparkDataFrame(df)

        results["silver.users"] = [
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_be_unique("user_id"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("phone"),
            gdf.expect_column_values_to_not_be_null("name"),
            
            # Teste para a nova coluna faixa_etaria
            gdf.expect_column_values_to_not_be_null("faixa_etaria"),
            gdf.expect_column_values_to_be_in_set("faixa_etaria", ["MENOR_18", "18-25", "26-35", "36-45", "46-60", "60+"]),
            
            # Testes para gender (gênero)
            gdf.expect_column_values_to_not_be_null("gender"),
            gdf.expect_column_values_to_be_in_set("gender", ["MASCULINO", "FEMININO", "OUTRO"]),
            
            # Testes para state (estado) - deve estar normalizado para UF
            gdf.expect_column_values_to_not_be_null("state"),
            gdf.expect_column_values_to_be_in_set("state", ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]),
            
            # Testes para occupation (ocupação)
            gdf.expect_column_values_to_not_be_null("occupation"),
            gdf.expect_column_values_to_be_in_set("occupation", ["APOSENTADO", "DESEMPREGADO", "AUTÔNOMO", "ESTUDANTE", "EMPREGADO"]),
            
            # Testes para income_range (faixa de renda)
            gdf.expect_column_values_to_not_be_null("income_range"),
            gdf.expect_column_values_to_be_in_set("income_range", ["ALTA", "BAIXA", "MÉDIA"]),
        ]
    except Exception as e:
        results["silver.users"] = [{"success": False, "error": str(e)}]

    # --- silver_transactions ---
    try:
        df = spark.read.table("`recarga-pay`.silver.transactions")
        gdf = ge.SparkDataFrame(df)

        checks = [
            gdf.expect_column_values_to_not_be_null("transaction_id"),
            gdf.expect_column_values_to_be_unique("transaction_id"),
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_not_be_null("product_amount"),
            gdf.expect_column_values_to_not_be_null("transaction_date"),
        ]
        if "status" in df.columns:
            checks.append(
                gdf.expect_column_values_to_be_in_set("status", ["PAID", "FAILED", "PENDING"])
            )

        results["silver.transactions"] = checks
    except Exception as e:
        results["silver.transactions"] = [{"success": False, "error": str(e)}]

    return results
