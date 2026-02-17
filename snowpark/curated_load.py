import os
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, when, sum as sum_, current_timestamp,
    year, month, row_number, to_date
)
from snowflake.snowpark.window import Window


def main(session: snowpark.Session):

    session.sql("USE DATABASE SNOWPARK_DB").collect()
    session.sql("USE SCHEMA SNOWPARK_DB.TRANSFORMED").collect()

    df_sales = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")

    # Standardize country names (business semantics)
    df_sales_std = df_sales.withColumn(
        "COUNTRY",
        when(col("COUNTRY") == "USA", "United States")
        .when(col("COUNTRY") == "EIRE", "Ireland")
        .when(col("COUNTRY") == "Unspecified", "Unknown")
        .when(col("COUNTRY") == "European Community", "Europe")
        .otherwise(col("COUNTRY"))
    )

    # CURATED TABLE 1: SALES_BY_COUNTRY
    df_sales_by_country = df_sales_std.groupBy("COUNTRY").agg(
        sum_("SALES_AMOUNT").alias("TOTAL_SALES"),
        sum_("QUANTITY").alias("TOTAL_QUANTITY"),
        sum_("PROFIT").alias("TOTAL_PROFIT")
    ).withColumn("INSERT_DTS", current_timestamp())

    df_sales_by_country.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.SALES_BY_COUNTRY")

    
    # CURATED TABLE 2: CATEGORY_PERFORMANCE
    df_category_performance = df_sales_std.groupBy(
        "COUNTRY", "CATEGORY"
    ).agg(
        sum_("SALES_AMOUNT").alias("TOTAL_SALES"),
        sum_("QUANTITY").alias("TOTAL_QUANTITY"),
        sum_("PROFIT").alias("TOTAL_PROFIT")
    ).withColumn(
        "PROFIT_MARGIN",
        when(col("TOTAL_SALES") != 0,
             col("TOTAL_PROFIT") / col("TOTAL_SALES"))
        .otherwise(0)
    ).withColumn("INSERT_DTS", current_timestamp())

    df_category_performance.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.CATEGORY_PERFORMANCE")


    # CURATED TABLE 3: MONTHLY_SALES_TREND
    df_monthly_sales = (
        df_sales_std
        .withColumn("YEAR", year(col("ORDER_DATE")))
        .withColumn("MONTH", month(col("ORDER_DATE")))
        .groupBy("YEAR", "MONTH", "COUNTRY")
        .agg(
            sum_("SALES_AMOUNT").alias("TOTAL_SALES"),
            sum_("QUANTITY").alias("TOTAL_QUANTITY"),
            sum_("PROFIT").alias("TOTAL_PROFIT")
        )
        .withColumn("INSERT_DTS", current_timestamp())
    )

    df_monthly_sales.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.MONTHLY_SALES_TREND")


    # CURATED TABLE 4: INDIA_SALES_VS_TARGET
    session.sql("USE SCHEMA SNOWPARK_DB.RAW").collect()

    df_india_targets = (
        session.table("SNOWPARK_DB.RAW.INDIA_SALES_TARGETS")
        .withColumn("TARGET_DATE", to_date(col("ORDER_MONTH"), "MON-YY"))
        .withColumn("YEAR", year(col("TARGET_DATE")))
        .withColumn("MONTH", month(col("TARGET_DATE")))
    )

    df_india_actuals = (
        df_sales_std
        .filter(col("COUNTRY") == "INDIA")
        .withColumn("YEAR", year(col("ORDER_DATE")))
        .withColumn("MONTH", month(col("ORDER_DATE")))
        .groupBy("YEAR", "MONTH", "CATEGORY")
        .agg(sum_("SALES_AMOUNT").alias("ACTUAL_SALES"))
    )

    df_india_vs_target = (
        df_india_actuals.join(
            df_india_targets,
            (df_india_actuals["YEAR"] == df_india_targets["YEAR"]) &
            (df_india_actuals["MONTH"] == df_india_targets["MONTH"]) &
            (df_india_actuals["CATEGORY"] == df_india_targets["CATEGORY"]),
            how="left"
        )
        .select(
            df_india_actuals["YEAR"],
            df_india_actuals["MONTH"],
            df_india_actuals["CATEGORY"],
            df_india_actuals["ACTUAL_SALES"],
            df_india_targets["TARGET_AMOUNT"],
            (df_india_actuals["ACTUAL_SALES"] -
             df_india_targets["TARGET_AMOUNT"]).alias("VARIANCE"),
            current_timestamp().alias("INSERT_DTS")
        )
    )

    df_india_vs_target.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.INDIA_SALES_VS_TARGET")


    # CURATED TABLE 5: TOP_PRODUCTS_BY_REVENUE
    session.sql("USE SCHEMA SNOWPARK_DB.TRANSFORMED").collect()

    df_product_sales = df_sales_std.groupBy(
        "COUNTRY", "PRODUCT_ID", "PRODUCT_NAME"
    ).agg(
        sum_("SALES_AMOUNT").alias("TOTAL_SALES"),
        sum_("QUANTITY").alias("TOTAL_QUANTITY"),
        sum_("PROFIT").alias("TOTAL_PROFIT")
    )

    window_spec = Window.partitionBy("COUNTRY") \
        .orderBy(col("TOTAL_SALES").desc())

    df_top_products = (
        df_product_sales
        .withColumn("PRODUCT_RANK", row_number().over(window_spec))
        .withColumn("INSERT_DTS", current_timestamp())
    )

    df_top_products.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.TOP_PRODUCTS_BY_REVENUE")

    return "TRANSFORMED → CURATED load completed successfully"


# Entry point for GitHub Actions / CLI execution
if __name__ == "__main__":

    session = Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "SNOWPARK_DB"),
    }).create()

    main(session)
    session.close()