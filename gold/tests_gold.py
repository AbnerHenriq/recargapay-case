import great_expectations as ge
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

def run_gold_tests():
    results = {}

    # --- dim_users ---
    try:
        df = spark.read.table("gold.dim_users")
        gdf = ge.dataset.SparkDFDataset(df)

        results["gold.dim_users"] = [
            gdf.expect_column_values_to_not_be_null("user_id"),
            gdf.expect_column_values_to_be_unique("user_id"),
            gdf.expect_column_values_to_not_be_null("email"),
            gdf.expect_column_values_to_not_be_null("created_at"),
            gdf.expect_column_values_to_not_be_null("updated_at"),
        ]
    except Exception as e:
        results["gold.dim_users"] = [{"success": False, "error": str(e)}]

    # --- fact_transactions ---
    try:
        df = spark.read.table("gold.fact_transactions")
        gdf = ge.dataset.SparkDFDataset(df)

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
                gdf.expect_column_values_to_be_in_set(
                    "status", ["paid", "failed", "pending"]
                )
            )
        if "cashback_fee_ratio" in df.columns:
            checks.append(
                gdf.expect_column_values_to_not_be_null("cashback_fee_ratio")
            )
            checks.append(
                gdf.expect_column_values_to_be_between(
                    "cashback_fee_ratio", min_value=0, max_value=1
                )
            )
        if "cashback_tpv_ratio" in df.columns:
            checks.append(
                gdf.expect_column_values_to_not_be_null("cashback_tpv_ratio")
            )
            checks.append(
                gdf.expect_column_values_to_be_between(
                    "cashback_tpv_ratio", min_value=0, max_value=1
                )
            )
        if "net_revenue" in df.columns:
            checks.append(gdf.expect_column_values_to_not_be_null("net_revenue"))
            if {"transaction_fee", "cashback"}.issubset(df.columns):
                invalid_count = df.filter(
                    (col("transaction_fee") < col("cashback"))
                    & (col("net_revenue") > 0)
                ).count()
                checks.append(
                    {
                        "success": invalid_count == 0,
                        "unexpected_count": invalid_count,
                        "expectation_type": "net_revenue_consistency",
                    }
                )

        results["gold.fact_transactions"] = checks
    except Exception as e:
        results["gold.fact_transactions"] = [{"success": False, "error": str(e)}]

    return results
