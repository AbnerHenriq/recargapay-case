import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run_silver_tests():
    results = {}

    # --- silver_users ---
    try:
        df = spark.read.table("silver.silver_users")
        gdf = ge.SparkDFDataset(df)

        results["silver.silver_users"] = [
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_be_unique("user_id"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("created_at"),
            gdf.expect_column_values_to_not_be_null("updated_at"),
        ]
    except Exception as e:
        results["silver.silver_users"] = [{"success": False, "error": str(e)}]

    # --- silver_transactions ---
    try:
        df = spark.read.table("silver.silver_transactions")
        gdf = ge.SparkDFDataset(df)

        checks = [
            gdf.expect_column_values_to_not_be_null("transaction_id"),
            gdf.expect_column_values_to_be_unique("transaction_id"),
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_not_be_null("amount"),
            gdf.expect_column_values_to_not_be_null("created_at"),
            gdf.expect_column_values_to_not_be_null("updated_at"),
        ]
        if "status" in df.columns:
            checks.append(
                gdf.expect_column_values_to_be_in_set("status", ["paid", "failed", "pending"])
            )

        results["silver.silver_transactions"] = checks
    except Exception as e:
        results["silver.silver_transactions"] = [{"success": False, "error": str(e)}]

    return results
