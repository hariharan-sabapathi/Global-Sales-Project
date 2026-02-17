import os
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def main(session: snowpark.Session):
    
    session.sql("USE DATABASE SNOWPARK_DB").collect()
    session.sql("USE SCHEMA SNOWPARK_DB.STAGGING").collect()

    session.sql("TRUNCATE TABLE INDIA_ORDERS").collect()
    session.sql("TRUNCATE TABLE INDIA_ORDER_DETAILS").collect()
    session.sql("TRUNCATE TABLE INDIA_SALES_TARGETS").collect()
    session.sql("TRUNCATE TABLE USA_SALES_ORDER_CP").collect()
    session.sql("TRUNCATE TABLE UK_SALES_ORDER_CP").collect()

    
    # LOAD INDIA CSV FILES
    session.sql("""
        COPY INTO INDIA_ORDERS
        FROM @SNOWPARK_STAGE/india/orders.csv
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            DATE_FORMAT = 'DD-MM-YYYY'
        )
    """).collect()

    session.sql("""
        COPY INTO INDIA_ORDER_DETAILS
        FROM @SNOWPARK_STAGE/india/order_details.csv
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
    """).collect()

    session.sql("""
        COPY INTO INDIA_SALES_TARGETS
        FROM @SNOWPARK_STAGE/india/sales_targets.csv
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
    """).collect()


    # LOAD USA PARQUET FILE
    session.sql("""
        COPY INTO USA_SALES_ORDER_CP
        FROM @SNOWPARK_STAGE/usa/usa_orders.parquet
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """).collect()


    # LOAD UK CSV FILE
    session.sql("""
        COPY INTO UK_SALES_ORDER_CP
        FROM @SNOWPARK_STAGE/uk/uk_orders.csv
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            ENCODING = 'UTF8'
        )
        ON_ERROR = 'CONTINUE'
    """).collect()


    session.sql("USE SCHEMA SNOWPARK_DB.RAW").collect()

    # INDIA RAW TABLES
    df_india_orders = session.sql("""
        SELECT
            ORDER_ID,
            ORDER_DATE,
            CUSTOMER_NAME,
            STATE,
            CITY,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.STAGGING.INDIA_ORDERS
    """)

    df_india_orders.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.RAW.INDIA_ORDERS")

    df_india_order_details = session.sql("""
        SELECT
            ORDER_ID,
            TO_NUMBER(REPLACE(AMOUNT, ',', '')) AS AMOUNT,
            TO_NUMBER(REPLACE(PROFIT, ',', '')) AS PROFIT,
            TO_NUMBER(QUANTITY) AS QUANTITY,
            CATEGORY,
            SUB_CATEGORY,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.STAGGING.INDIA_ORDER_DETAILS
    """)

    df_india_order_details.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.RAW.INDIA_ORDER_DETAILS")

    df_india_sales_targets = session.sql("""
        SELECT
            ORDER_MONTH,
            CATEGORY,
            TO_NUMBER(REPLACE(TARGET, ',', '')) AS TARGET_AMOUNT,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.STAGGING.INDIA_SALES_TARGETS
    """)

    df_india_sales_targets.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.RAW.INDIA_SALES_TARGETS")

    # USA RAW TABLE
    df_usa_sales = session.sql("""
        SELECT
            "Row ID"       AS ROW_ID,
            "Order ID"     AS ORDER_ID,
            "Order Date"   AS ORDER_DATE,
            "Ship Date"    AS SHIP_DATE,
            "Ship Mode"    AS SHIP_MODE,
            "Customer ID"  AS CUSTOMER_ID,
            "Country"      AS COUNTRY,
            "City"         AS CITY,
            "State"        AS STATE,
            "Postal Code"  AS POSTAL_CODE,
            "Region"       AS REGION,
            "Product ID"   AS PRODUCT_ID,
            "Category"     AS CATEGORY,
            "Sub-Category" AS SUB_CATEGORY,
            "Product Name" AS PRODUCT_NAME,
            TRY_TO_NUMBER(REPLACE("Sales", ',', ''))    AS SALES,
            TRY_TO_NUMBER(REPLACE("Quantity", ',', '')) AS QUANTITY,
            TRY_TO_NUMBER(REPLACE("Discount", ',', '')) AS DISCOUNT,
            TRY_TO_NUMBER(REPLACE("Profit", ',', ''))   AS PROFIT,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.STAGGING.USA_SALES_ORDER_CP
    """)

    df_usa_sales.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.RAW.USA_SALES_ORDER")

    # UK RAW TABLE
    df_uk_sales = session.sql("""
        SELECT
            INVOICE_NO AS ORDER_ID,
            STOCK_CODE AS PRODUCT_ID,
            DESCRIPTION AS PRODUCT_DESCRIPTION,
            TRY_TO_NUMBER(QUANTITY) AS QUANTITY,
            TO_TIMESTAMP_NTZ(INVOICE_DATE, 'MM/DD/YYYY HH24:MI') AS ORDER_DATE,
            TRY_TO_NUMBER(REPLACE(UNIT_PRICE, ',', '')) AS UNIT_PRICE,
            CUSTOMER_ID,
            COUNTRY,
            CURRENT_TIMESTAMP() AS INSERT_DTS
        FROM SNOWPARK_DB.STAGGING.UK_SALES_ORDER_CP
    """)

    df_uk_sales.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.RAW.UK_SALES_ORDER")

    return "STAGGING → RAW load completed successfully"


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