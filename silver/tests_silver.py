import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run_silver_tests():
    results = {}

    # --- silver_users ---
    try:
        df = spark.read.table("`recarga-pay`.silver.users")
        gdf = ge.dataset.SparkDFDataset(df)

        results["`recarga-pay`.silver.users"] = [
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_be_unique("user_id"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("data_ativacao"),
            gdf.expect_column_values_to_not_be_null("_silver_processing_timestamp"),
        ]
    except Exception as e:
        results["`recarga-pay`.silver.users"] = [{"success": False, "error": str(e)}]

    # --- silver_transactions ---
    try:
        df = spark.read.table("`recarga-pay`.silver.transactions")
        gdf = ge.dataset.SparkDFDataset(df)

        checks = [
            gdf.expect_column_values_to_not_be_null("transaction_id"),
            gdf.expect_column_values_to_be_unique("transaction_id"),
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_not_be_null("product_amount"),
            gdf.expect_column_values_to_not_be_null("transaction_date"),
            gdf.expect_column_values_to_not_be_null("_silver_processing_timestamp"),
        ]

        results["`recarga-pay`.silver.transactions"] = checks
    except Exception as e:
        results["`recarga-pay`.silver.transactions"] = [{"success": False, "error": str(e)}]

    return results
