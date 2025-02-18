from dotenv import load_dotenv
import os
import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt

# ✅ Load environment variables explicitly
dotenv_path = "/app/.env"
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    st.error(f"❌ .env file not found at {dotenv_path}")

# ✅ Streamlit Page Config
st.set_page_config(page_title="Snowflake Query & Visualization", layout="wide")
st.sidebar.title("📊 Navigation")

# ✅ Load Snowflake Credentials from Environment Variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")

# ✅ Validate Credentials
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE]):
    st.error("❌ Missing Snowflake credentials or database name. Check your `.env` file or input.")
    st.stop()

# ✅ Function to Establish Snowflake Connection
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            client_session_keep_alive=True,
            login_timeout=60,
            autocommit=True
        )
        return conn
    except Exception as e:
        st.error(f"❌ Snowflake connection failed: {e}")
        return None

# ✅ Fetch List of Schemas
def get_schema_list():
    try:
        conn = get_snowflake_connection()
        if conn:
            query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
            df = pd.read_sql(query, conn)
            conn.close()
            return df["SCHEMA_NAME"].tolist() if "SCHEMA_NAME" in df.columns else []
    except Exception as e:
        st.error(f"❌ Error fetching schema list: {e}")
        return []

# ✅ Fetch List of Tables
def get_table_list(schema):
    try:
        conn = get_snowflake_connection()
        if conn:
            query = f"SELECT TABLE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
            df = pd.read_sql(query, conn)
            conn.close()

            if df.empty:
                st.warning("⚠️ No tables found in the selected schema.")
                return []

            return df["TABLE_NAME"].tolist() if "TABLE_NAME" in df.columns else []
    except Exception as e:
        st.error(f"❌ Error fetching table list: {e}")
        return []

# ✅ Fetch Data with Filters and Pagination
def fetch_filtered_data(schema, table_name, offset=0, limit=5000, filters=None):
    try:
        conn = get_snowflake_connection()
        if conn:
            # Base query
            query = f'SELECT * FROM {SNOWFLAKE_DATABASE}.{schema}."{table_name}"'

            # Apply filters in SQL query for efficiency
            where_clauses = []
            if filters:
                for column, value in filters.items():
                    if isinstance(value, tuple):  # Numerical range filter
                        where_clauses.append(f'"{column}" BETWEEN {value[0]} AND {value[1]}')
                    elif value and value != "":  # Categorical selection
                        where_clauses.append(f'"{column}" = \'{value}\'')

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            query += f' LIMIT {limit} OFFSET {offset}'

            df = pd.read_sql(query, conn)
            conn.close()
            return df if not df.empty else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error fetching filtered data: {e}")
        return pd.DataFrame()

# ✅ Sidebar - Select Schema
schemas = get_schema_list()
if schemas:
    SNOWFLAKE_SCHEMA = st.sidebar.selectbox("Select Schema", schemas)
else:
    st.error("❌ No schemas found. Check database connection or permissions.")
    SNOWFLAKE_SCHEMA = None

# ✅ Sidebar - Select View
view_option = st.sidebar.radio("Choose View:", ["View Snowflake Tables", "Query Snowflake Table", "Visualizations"])

# ✅ Snowflake Table Viewer with Dynamic Filters & Pagination
if view_option == "View Snowflake Tables" and SNOWFLAKE_SCHEMA:
    st.title("📂 Snowflake Table Viewer")

    tables = get_table_list(SNOWFLAKE_SCHEMA)

    if tables:
        selected_table = st.sidebar.selectbox("Select a Table", tables)

        if selected_table:
            st.sidebar.subheader("🔍 Pagination & Row Limit")
            row_limit = st.sidebar.slider("Rows per Page", 100, 10000, 5000, 500)
            page_number = st.sidebar.number_input("Page Number", min_value=1, value=1, step=1)
            offset = (page_number - 1) * row_limit

            # Fetch FULL data (first 5000 rows) to get filter options
            full_df = fetch_filtered_data(SNOWFLAKE_SCHEMA, selected_table, limit=5000)

            filters = {}
            if not full_df.empty:
                st.sidebar.subheader("🎯 Column Filters")

                for column in full_df.columns:
                    unique_values = full_df[column].dropna().unique()
                    if len(unique_values) < 15:  # Categorical filter
                        filters[column] = st.sidebar.selectbox(f"Filter {column}", [""] + list(unique_values), key=column)
                    elif pd.api.types.is_numeric_dtype(full_df[column]):  # Numerical range filter
                        min_val, max_val = int(full_df[column].min()), int(full_df[column].max())
                        filters[column] = st.sidebar.slider(f"Filter {column}", min_val, max_val, (min_val, max_val), key=column)

            # Apply button for filters
            apply_filters = st.sidebar.button("Apply Filters")

            # Fetch paginated & filtered data only if "Apply Filters" is clicked
            if apply_filters:
                filtered_df = fetch_filtered_data(SNOWFLAKE_SCHEMA, selected_table, offset=offset, limit=row_limit, filters=filters)

                if not filtered_df.empty:
                    st.subheader(f"📄 Filtered Data from `{selected_table}` (Page {page_number})")
                    st.data_editor(filtered_df)  # Efficient DataFrame rendering
                else:
                    st.warning("⚠️ No data available with the applied filters.")
    else:
        st.error("⚠️ No tables found. Check your **database connection** or **permissions**.")
