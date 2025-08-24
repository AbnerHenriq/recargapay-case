import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run_gold_tests():
    results = {}

    # --- dim_users ---
    try:
        df = spark.read.table("gold.dim_users")
        gdf = ge.SparkDFDataset(df)

        results["gold.dim_users"] = [
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_be_unique("user_id"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("data_ativacao"),
            gdf.expect_column_values_to_not_be_null("_gold_processing_timestamp"),
        ]
    except Exception as e:
        results["gold.dim_users"] = [{"success": False, "error": str(e)}]

    # --- fact_transactions ---
    try:
        df = spark.read.table("gold.fact_transactions")
        gdf = ge.SparkDFDataset(df)

        checks = [
            gdf.expect_column_values_to_not_be_null("transaction_id"),
            gdf.expect_column_values_to_be_unique("transaction_id"),
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_not_be_null("product_amount"),
            gdf.expect_column_values_to_not_be_null("transaction_date"),
            gdf.expect_column_values_to_not_be_null("net_revenue"),
            gdf.expect_column_values_to_be_between(
                "cashback_fee_ratio", 0, 1
            ),
            gdf.expect_column_values_to_be_between(
                "cashback_tpv_ratio", 0, 1
            ),
            gdf.expect_column_values_to_not_be_null("_gold_processing_timestamp"),
        ]
        
        results["gold.fact_transactions"] = checks
    except Exception as e:
        results["gold.fact_transactions"] = [{"success": False, "error": str(e)}]

    return results
