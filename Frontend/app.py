import streamlit as st
from dotenv import load_dotenv
import os
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt

# ✅ Ensure Streamlit Page Config is the first command
st.set_page_config(page_title="Snowflake Query & Visualization", layout="wide")
st.sidebar.title("📊 Navigation")

# ✅ Load environment variables explicitly
dotenv_path = "/app/.env"
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    st.error(f"❌ .env file not found at {dotenv_path}")

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
            return df["TABLE_NAME"].tolist() if "TABLE_NAME" in df.columns else []
    except Exception as e:
        st.error(f"❌ Error fetching table list: {e}")
        return []

# ✅ Fetch Data with Filters and Pagination
def fetch_filtered_data(schema, table_name, offset=0, limit=5000, filters=None):
    try:
        conn = get_snowflake_connection()
        if conn:
            query = f'SELECT * FROM {SNOWFLAKE_DATABASE}.{schema}."{table_name}"'
            where_clauses = []
            if filters:
                for column, value in filters.items():
                    if isinstance(value, tuple):  
                        where_clauses.append(f'"{column}" BETWEEN {value[0]} AND {value[1]}')
                    elif value and value != "":  
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

# ✅ Execute Custom SQL Query
def execute_snowflake_query(query):
    try:
        conn = get_snowflake_connection()
        if conn:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
    except Exception as e:
        st.error(f"❌ Error executing query: {e}")
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

# ✅ Snowflake Table Viewer with Filters & Pagination
if view_option == "View Snowflake Tables" and SNOWFLAKE_SCHEMA:
    st.title("📂 Snowflake Table Viewer")
    tables = get_table_list(SNOWFLAKE_SCHEMA)
    if tables:
        selected_table = st.sidebar.selectbox("Select a Table", tables)
        if selected_table:
            row_limit = st.sidebar.slider("Rows per Page", 100, 10000, 5000, 500)
            page_number = st.sidebar.number_input("Page Number", min_value=1, value=1, step=1)
            offset = (page_number - 1) * row_limit

            full_df = fetch_filtered_data(SNOWFLAKE_SCHEMA, selected_table, limit=5000)
            filters = {}
            if not full_df.empty:
                st.sidebar.subheader("🎯 Column Filters")
                for column in full_df.columns:
                    unique_values = full_df[column].dropna().unique()
                    if len(unique_values) < 15:
                        filters[column] = st.sidebar.selectbox(f"Filter {column}", [""] + list(unique_values))
                    elif pd.api.types.is_numeric_dtype(full_df[column]):
                        min_val, max_val = int(full_df[column].min()), int(full_df[column].max())
                        filters[column] = st.sidebar.slider(f"Filter {column}", min_val, max_val, (min_val, max_val))

            if st.sidebar.button("Apply Filters"):
                filtered_df = fetch_filtered_data(SNOWFLAKE_SCHEMA, selected_table, offset=offset, limit=row_limit, filters=filters)
                st.subheader(f"📄 Filtered Data from `{selected_table}` (Page {page_number})")
                st.data_editor(filtered_df)

# ✅ Query Snowflake Table
if view_option == "Query Snowflake Table" and SNOWFLAKE_SCHEMA:
    st.title("📝 Query Snowflake Table")
    custom_query = st.text_area("Write your SQL Query below:", height=200, value=f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TABLE_NAME LIMIT 100;")
    if st.button("Run Query"):
        result_df = execute_snowflake_query(custom_query)
        if not result_df.empty:
            st.subheader("📄 Query Results")
            st.data_editor(result_df)
            st.write(f"✅ Returned {len(result_df)} rows.")

# ✅ Data Visualization
if view_option == "Visualizations" and SNOWFLAKE_SCHEMA:
    st.title("📊 Data Visualization")
    tables = get_table_list(SNOWFLAKE_SCHEMA)
    if tables:
        selected_table = st.sidebar.selectbox("Select a Table for Visualization", tables)
        if selected_table:
            sample_df = fetch_filtered_data(SNOWFLAKE_SCHEMA, selected_table, limit=1000)
            if not sample_df.empty:
                numeric_columns = sample_df.select_dtypes(include=["number"]).columns.tolist()
                if numeric_columns:
                    x_column = st.sidebar.selectbox("Select X-axis Column", numeric_columns)
                    y_column = st.sidebar.selectbox("Select Y-axis Column", numeric_columns)
                    plot_type = st.sidebar.radio("Select Plot Type", ["Scatter Plot", "Line Chart", "Bar Chart"])

                    fig, ax = plt.subplots(figsize=(8, 5))
                    if plot_type == "Scatter Plot":
                        ax.scatter(sample_df[x_column], sample_df[y_column])
                    elif plot_type == "Line Chart":
                        ax.plot(sample_df[x_column], sample_df[y_column])
                    elif plot_type == "Bar Chart":
                        ax.bar(sample_df[x_column].astype(str), sample_df[y_column])

                    st.pyplot(fig)
