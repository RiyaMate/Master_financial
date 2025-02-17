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
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# ✅ Validate Credentials
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
    st.error("❌ Missing Snowflake credentials. Check your `.env` file or environment variables.")
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
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True,
            login_timeout=60,
            autocommit=True
        )
        return conn
    except Exception as e:
        st.error(f"❌ Snowflake connection failed: {e}")
        return None

# ✅ Fetch List of Tables
def get_table_list():
    try:
        conn = get_snowflake_connection()
        if conn:
            query = f"SHOW TABLES IN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}"
            df = pd.read_sql(query, conn)
            conn.close()
            return df["name"].tolist() if "name" in df.columns else []
    except Exception as e:
        st.error(f"❌ Error fetching table list: {e}")
        return []

# ✅ Fetch Data from a Selected Table
def fetch_table_data(table_name, filters=None):
    try:
        conn = get_snowflake_connection()
        if conn:
            query = f'SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."{table_name}" LIMIT 1000'
            df = pd.read_sql(query, conn)
            conn.close()

            if not df.empty and filters:
                for column, value in filters.items():
                    if value and value != "":  
                        df = df[df[column] == value]

            return df if not df.empty else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error fetching data from `{table_name}`: {e}")
        return pd.DataFrame()

# ✅ Execute Custom SQL Query
def execute_query(query):
    try:
        conn = get_snowflake_connection()
        if conn:
            if not query.strip().lower().startswith("select"):
                st.error("❌ Only SELECT queries are allowed for security reasons.")
                return pd.DataFrame()
            df = pd.read_sql(query, conn)
            conn.close()
            return df if not df.empty else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Query Execution Failed: {e}")
        return pd.DataFrame()

# ✅ Sidebar - Select View
view_option = st.sidebar.radio("Choose View:", ["View Snowflake Tables", "Query Snowflake Table", "Visualizations"])

# ✅ Snowflake Table Viewer Feature with Column Filters
if view_option == "View Snowflake Tables":
    st.title("📂 Snowflake Table Viewer")
    tables = get_table_list()

    if tables:
        selected_table = st.sidebar.selectbox("Select a Table", tables)
        if selected_table:
            raw_df = fetch_table_data(selected_table)

            if not raw_df.empty:
                st.sidebar.subheader("🔍 Apply Column Filters")

                filters = {}
                for column in raw_df.columns:
                    unique_values = raw_df[column].dropna().unique()
                    if len(unique_values) < 15:
                        filters[column] = st.sidebar.selectbox(f"Filter by {column}", [""] + list(unique_values), index=0)
                    elif raw_df[column].dtype in ["int64", "float64"]:
                        min_val, max_val = int(raw_df[column].min()), int(raw_df[column].max())
                        filters[column] = st.sidebar.slider(f"Filter {column}", min_val, max_val, (min_val, max_val))

                filtered_df = fetch_table_data(selected_table, filters)
                if filtered_df.empty:
                    st.warning("⚠️ No data matches the selected filters. Showing unfiltered data.")
                    st.dataframe(raw_df.head(10))
                else:
                    st.subheader(f"📄 Filtered Data from `{selected_table}`")
                    st.dataframe(filtered_df)
            else:
                st.warning("⚠️ No data available in this table.")
    else:
        st.error("⚠️ No tables found. Check your **database connection** or **permissions**.")

# ✅ Query Execution Feature
elif view_option == "Query Snowflake Table":
    st.title("📝 Execute Custom SQL Query on Snowflake")
    query = st.text_area("Enter your SQL query (Only SELECT queries allowed)", "SELECT * FROM PUBLIC.SAMPLE_TABLE LIMIT 10")

    if st.button("Run Query"):
        df = execute_query(query)

        if not df.empty:
            st.dataframe(df)
        else:
            st.warning("⚠️ No data returned from query.")

# ✅ Visualization Feature
elif view_option == "Visualizations":
    st.title("📈 Data Visualizations")
    tables = get_table_list()

    if tables:
        selected_table = st.sidebar.selectbox("Select a Table for Visualization", tables)
        if selected_table:
            df = fetch_table_data(selected_table)

            if not df.empty:
                st.dataframe(df)

                viz_type = st.sidebar.selectbox("Choose Visualization Type", ["Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot", "Histogram"])

                cat_cols = df.select_dtypes(include=['object']).columns.tolist()
                num_cols = df.select_dtypes(include=['number']).columns.tolist()

                if viz_type == "Bar Chart" and cat_cols:
                    cat_col = st.selectbox("Select Categorical Column", cat_cols)
                    fig, ax = plt.subplots()
                    df[cat_col].value_counts().plot(kind='bar', ax=ax)
                    st.pyplot(fig)

                elif viz_type == "Line Chart" and num_cols:
                    num_col = st.selectbox("Select Numerical Column", num_cols)
                    fig, ax = plt.subplots()
                    df[num_col].plot(kind='line', ax=ax)
                    st.pyplot(fig)

                elif viz_type == "Pie Chart" and cat_cols:
                    cat_col_pie = st.selectbox("Select Categorical Column for Pie Chart", cat_cols)
                    fig, ax = plt.subplots()
                    df[cat_col_pie].value_counts().plot(kind='pie', autopct='%1.1f%%', ax=ax)
                   
