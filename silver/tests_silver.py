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
