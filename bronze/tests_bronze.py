import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run_bronze_tests():
    results = {}

    # --- bronze_users ---
    try:
        df = spark.read.table("`recarga-pay`.bronze.users")
        gdf = ge.SparkDFDataset(df)

        results["bronze.users"] = [
            gdf.expect_column_values_to_not_be_null("id_usuario"),
            gdf.expect_column_values_to_be_unique("id_usuario"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("nome"),
        ]
    except Exception as e:
        results["bronze.users"] = [{"success": False, "error": str(e)}]

    # --- bronze_transactions ---
    try:
        df = spark.read.table("`recarga-pay`.bronze.transactions")
        gdf = ge.SparkDFDataset(df)

        checks = [
            gdf.expect_column_values_to_not_be_null("id_transacao"),
            gdf.expect_column_values_to_be_unique("id_transacao"),
            gdf.expect_column_values_to_not_be_null("id_usuario"),
            gdf.expect_column_values_to_not_be_null("valor_produto"),
            gdf.expect_column_values_to_not_be_null("data_transacao"),
        ]
        if "status" in df.columns:
            checks.append(
                gdf.expect_column_values_to_be_in_set("status", ["paid", "failed", "pending"])
            )

        results["bronze.transactions"] = checks
    except Exception as e:
        results["bronze.transactions"] = [{"success": False, "error": str(e)}]

    return results
