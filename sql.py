import streamlit as st
import psycopg2
import pandas as pd
import sqlparse
from psycopg2.extras import DictCursor
import json
import os
import time

# Query templates
QUERY_TEMPLATES = {
    "Count Records": "SELECT COUNT(*) FROM {table_name};",
    "Sample Records": "SELECT * FROM {table_name} LIMIT 10;",
    "Column Info": """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """
}

# Initialize session state
if 'connected' not in st.session_state:
    st.session_state['connected'] = False
if 'saved_queries' not in st.session_state:
    st.session_state['saved_queries'] = {}
    if os.path.exists('saved_queries.json'):
        with open('saved_queries.json', 'r') as f:
            st.session_state['saved_queries'] = json.load(f)
if 'query_history' not in st.session_state:
    st.session_state['query_history'] = []
if 'current_schema' not in st.session_state:
    st.session_state['current_schema'] = None

# Page config
st.set_page_config(page_title="PostgreSQL Streamlit Interface", layout="wide")

# Main app
st.title("PostgreSQL Streamlit Interface")

# Function to get available databases
def get_databases(username, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=username,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT datname FROM pg_database 
            WHERE datistemplate = false 
            AND datname NOT IN ('postgres')
            ORDER BY datname;
        """)
        databases = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return databases
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

# Function to get tables
def get_tables():
    try:
        conn = create_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            cursor.close()
            conn.close()
            return tables
        return []
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

# Function to get schema info
def get_schema_info(table_name):
    try:
        conn = create_connection()
        if conn:
            query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """
            return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error fetching schema: {str(e)}")
        return None

# Function to create database connection
def create_connection():
    try:
        conn = psycopg2.connect(
            dbname=st.session_state.db_name,
            user=st.session_state.username,
            password=st.session_state.password,
            host=st.session_state.host,
            port=st.session_state.port
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

# Execute query and return results
def run_query(query):
    start_time = time.time()
    try:
        with st.spinner('Executing query...'):
            conn = create_connection()
            if conn:
                # Set timeout for query
                with conn.cursor() as cursor:
                    cursor.execute("SET statement_timeout = 30000;")  # 30 seconds
                df = pd.read_sql_query(query, conn)
                conn.close()
                query_time = time.time() - start_time
                
                # Add to query history
                st.session_state['query_history'].append({
                    'query': query,
                    'timestamp': pd.Timestamp.now(),
                    'rows_returned': len(df),
                    'execution_time': query_time
                })
                
                return df, query_time
        return None, 0
    except psycopg2.errors.QueryCanceled:
        st.error("Query timed out after 30 seconds")
        return None, 0
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None, 0

# Save query to file
def save_query(name, query):
    st.session_state['saved_queries'][name] = query
    with open('saved_queries.json', 'w') as f:
        json.dump(st.session_state['saved_queries'], f)

# Delete query from saved queries
def delete_query(name):
    if name in st.session_state['saved_queries']:
        del st.session_state['saved_queries'][name]
        with open('saved_queries.json', 'w') as f:
            json.dump(st.session_state['saved_queries'], f)

# Sidebar
with st.sidebar:
    st.header("Database Connection")
    
    # Connection parameters
    st.session_state.username = st.text_input("Username", "astraus")
    st.session_state.password = st.text_input("Password", "", type="password")
    st.session_state.host = st.text_input("Host", "localhost")
    st.session_state.port = st.text_input("Port", "5432")
    
    # Get available databases button
    if st.button("List Available Databases"):
        databases = get_databases(
            st.session_state.username,
            st.session_state.password,
            st.session_state.host,
            st.session_state.port
        )
        if databases:
            st.session_state['databases'] = databases
            st.success("Retrieved database list!")
    
    # Database selection
    if 'databases' in st.session_state:
        st.session_state.db_name = st.selectbox(
            "Select Database",
            options=st.session_state['databases'],
            index=st.session_state['databases'].index('ramp') if 'ramp' in st.session_state['databases'] else 0
        )
    else:
        st.session_state.db_name = st.text_input("Database name", "ramp")

    if st.button("Connect"):
        conn = create_connection()
        if conn:
            conn.close()
            st.session_state.connected = True
            st.success("Connected successfully!")
        else:
            st.session_state.connected = False
    
    st.markdown("---")
    
    # Query Management section
    st.header("Query Management")
    
    # Load saved query
    if st.session_state['saved_queries']:
        st.subheader("Load Query")
        selected_query = st.selectbox(
            "Select a saved query",
            options=list(st.session_state['saved_queries'].keys()),
            key="query_select"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Load"):
                st.session_state['current_query'] = st.session_state['saved_queries'][selected_query]
        with col2:
            if st.button("Delete"):
                delete_query(selected_query)
                st.rerun()
    
    # Save query section
    st.subheader("Save Query")
    query_name = st.text_input("Query name")
    if st.button("Save Query"):
        if query_name and st.session_state.get('current_query'):
            save_query(query_name, st.session_state['current_query'])
            st.success(f"Query saved as '{query_name}'")
        else:
            st.warning("Please provide both a name and a query")

# Main content area
if st.session_state.connected:
    # Create tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs(["Query Editor", "Table Explorer", "Query History", "Templates"])
    
    with tab1:
        st.header("SQL Query")
        query = st.text_area(
            "Enter your SQL query:",
            height=200,
            value=st.session_state.get('current_query', '')
        )
        
        # Store current query in session state
        st.session_state['current_query'] = query
        
        # Run query button
        if st.button("Run Query"):
            if query:
                # Format and display the query
                formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
                st.code(formatted_query, language="sql")
                
                # Execute query and show results
                results, query_time = run_query(query)
                if results is not None:
                    # Display query statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Rows", len(results))
                    with col2:
                        st.metric("Columns", len(results.columns))
                    with col3:
                        st.metric("Query Time", f"{query_time:.2f}s")
                    
                    # Display results
                    st.dataframe(results)
                    
                    # Export options
                    if not results.empty:
                        csv = results.to_csv(index=False)
                        st.download_button(
                            label="Download Results as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv"
                        )
    
    with tab2:
        st.header("Table Explorer")
        tables = get_tables()
        if tables:
            selected_table = st.selectbox(
                "Select a table",
                options=[table[0] for table in tables]
            )
            
            if selected_table:
                schema_info = get_schema_info(selected_table)
                if schema_info is not None:
                    st.subheader("Schema Information")
                    st.dataframe(schema_info)
                    
                    # Quick action buttons
                    if st.button("Sample Data"):
                        query = f"SELECT * FROM {selected_table} LIMIT 10;"
                        st.session_state['current_query'] = query
                        results, query_time = run_query(query)
                        if results is not None:
                            st.dataframe(results)
    
    with tab3:
        st.header("Query History")
        if st.session_state['query_history']:
            history_df = pd.DataFrame(st.session_state['query_history'])
            st.dataframe(history_df)
            
            if st.button("Clear History"):
                st.session_state['query_history'] = []
                st.rerun()
    
    with tab4:
        st.header("Query Templates")
        tables = get_tables()
        if tables:
            selected_table = st.selectbox(
                "Select a table",
                options=[table[0] for table in tables],
                key="template_table"
            )
            
            selected_template = st.selectbox(
                "Select a template",
                options=list(QUERY_TEMPLATES.keys())
            )
            
            if st.button("Load Template"):
                template_query = QUERY_TEMPLATES[selected_template].format(
                    table_name=selected_table
                )
                st.session_state['current_query'] = template_query
                st.rerun()

else:
    st.warning("Please connect to database using the sidebar.")