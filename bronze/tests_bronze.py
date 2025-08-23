import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run_bronze_tests():
    results = {}

    # --- bronze_users ---
    try:
        df = spark.read.table("`recarga-pay`.bronze.users")
        gdf = ge.dataset.SparkDFDataset(df)

        results["`recarga-pay`.bronze.users"] = [
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_be_unique("user_id"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("data_ativacao"),
        ]
    except Exception as e:
        results["`recarga-pay`.bronze.users"] = [{"success": False, "error": str(e)}]

    # --- bronze_transactions ---
    try:
        df = spark.read.table("`recarga-pay`.bronze.transactions")
        gdf = ge.dataset.SparkDFDataset(df)

        checks = [
            gdf.expect_column_values_to_not_be_null("transaction_id"),
            gdf.expect_column_values_to_be_unique("transaction_id"),
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_not_be_null("product_amount"),
            gdf.expect_column_values_to_not_be_null("transaction_date"),
        ]

        results["`recarga-pay`.bronze.transactions"] = checks
    except Exception as e:
        results["`recarga-pay`.bronze.users"] = [{"success": False, "error": str(e)}]

    return results
