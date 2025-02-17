import snowflake.connector

conn = snowflake.connector.connect(
    user="RMATE",
    password="Happyneu123@",
    account="qiqzsry-hv39958.snowflakecomputing.com"
)

print("✅ Connection successful!")
conn.close()
