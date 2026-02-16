import os
import sys
from snowflake.snowpark import Session

sql_file = sys.argv[1]

connection_parameters = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
}

session = Session.builder.configs(connection_parameters).create()

with open(sql_file) as f:
    statements = f.read().split(";")

for stmt in statements:
    if stmt.strip():
        session.sql(stmt).collect()

session.close()
