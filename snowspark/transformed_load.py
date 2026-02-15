import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, current_timestamp

def main(session: snowpark.Session):

    
    session.sql("USE SCHEMA SNOWPARK_DB.RAW").collect()

    # INDIA TRANSFORMED (join orders + order_details)
    df_india = session.sql("""
        SELECT
            o.ORDER_ID,
            o.ORDER_DATE,
            'INDIA' AS COUNTRY,
            o.STATE,
            o.CITY,
            d.CATEGORY,
            d.SUB_CATEGORY,
            NULL AS PRODUCT_ID,
            NULL AS PRODUCT_NAME,
            d.QUANTITY,
            d.AMOUNT AS SALES_AMOUNT,
            d.PROFIT,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.RAW.INDIA_ORDERS o
        JOIN SNOWPARK_DB.RAW.INDIA_ORDER_DETAILS d
          ON o.ORDER_ID = d.ORDER_ID
    """)

    # USA TRANSFORMED
    df_usa = session.sql("""
        SELECT
            ORDER_ID,
            ORDER_DATE,
            COUNTRY,
            STATE,
            CITY,
            CATEGORY,
            SUB_CATEGORY,
            PRODUCT_ID,
            PRODUCT_NAME,
            QUANTITY,
            SALES AS SALES_AMOUNT,
            PROFIT,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.RAW.USA_SALES_ORDER
    """)

    # UK TRANSFORMED
    df_uk = session.sql("""
        SELECT
            ORDER_ID,
            ORDER_DATE,
            COUNTRY,
            NULL AS STATE,
            NULL AS CITY,
            NULL AS CATEGORY,
            NULL AS SUB_CATEGORY,
            PRODUCT_ID,
            PRODUCT_DESCRIPTION AS PRODUCT_NAME,
            QUANTITY,
            QUANTITY * UNIT_PRICE AS SALES_AMOUNT,
            NULL AS PROFIT,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.RAW.UK_SALES_ORDER
    """)

    # UNION ALL COUNTRIES
    df_global_sales = (
        df_india
        .union_by_name(df_usa)
        .union_by_name(df_uk)
    )

    
    df_global_sales.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")

    return "RAW → TRANSFORMED load completed successfully"
