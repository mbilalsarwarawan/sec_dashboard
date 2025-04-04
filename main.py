import streamlit as st
import psycopg2
import pandas as pd
import math
from datetime import date
import tornado.websocket
import tornado.iostream

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="sec_database",
            user="sec_user",
            password="mypassword",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Initialize session state variables for search results and WHERE clause
if 'df_results' not in st.session_state:
    st.session_state.df_results = None
if 'where_clause' not in st.session_state:
    st.session_state.where_clause = ""

st.set_page_config(page_title="SEC Filings Data Viewer", layout="wide")
st.title("📊 SEC Filings Data Viewer")

# Sidebar filters
st.sidebar.header("🔍 Filters")
date_filter_mode = st.sidebar.radio("Date Filter Mode", ["No Date Filter", "Single Date", "Date Range"], index=0)

# Date inputs
selected_date = None
start_date = None
end_date = None
if date_filter_mode == "Single Date":
    selected_date = st.sidebar.date_input("Select Date", date.today())
elif date_filter_mode == "Date Range":
    start_date = st.sidebar.date_input("Start Date", date.today())
    end_date = st.sidebar.date_input("End Date", date.today())

# Other filters
cik_input = st.sidebar.text_input("Enter CIK (Exact Match)")
company_name_input = st.sidebar.text_input("Enter Company Name (Partial Match)")

def run_search_logic():
    conn = None
    try:
        filter_conditions = []
        
        # Date handling
        if date_filter_mode == "Single Date" and selected_date:
            selected_date_str = selected_date.strftime("%Y-%m-%d")
            filter_conditions.append(f"date = '{selected_date_str}'")
        elif date_filter_mode == "Date Range" and start_date and end_date:
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            filter_conditions.append(f"date BETWEEN '{start_date_str}' AND '{end_date_str}'")
        
        if cik_input:
            filter_conditions.append(f"cik = '{cik_input}'")
        
        if company_name_input:
            filter_conditions.append(f"\"Company Name\" ILIKE '%{company_name_input}%'")
        
        # Construct and store WHERE clause for pagination use
        where_clause = " WHERE " + " AND ".join(filter_conditions) if filter_conditions else ""
        st.session_state.where_clause = where_clause
        
        # Get database connection
        conn = get_db_connection()
        if conn is None:
            return
        
        cursor = conn.cursor()
        
        # Count total records
        cursor.execute(f"SELECT COUNT(*) FROM sec_data {where_clause}")
        total_count = cursor.fetchone()[0]
        
        # Pagination setup
        rows_per_page = 10000
        max_page = math.ceil(total_count / rows_per_page) if total_count > 0 else 1
        page = 1  # always start from first page for new searches
        
        # Get data for the first page
        final_query = f"""
            SELECT * FROM sec_data {where_clause}
            ORDER BY date, cik
            LIMIT {rows_per_page} OFFSET 0
        """
        df = pd.read_sql(final_query, conn)
        
        # Store results in session state
        st.session_state.df_results = {
            'data': df,
            'page': page,
            'max_page': max_page,
            'total_count': total_count
        }
    except psycopg2.Error as db_err:
        st.error(f"Database error: {db_err}")
    except tornado.websocket.WebSocketClosedError:
        # WebSocket connection closed. Optionally log or ignore.
        st.warning("WebSocket closed during search.")
    except tornado.iostream.StreamClosedError:
        st.warning("Stream closed during search.")
    except Exception as ex:
        st.error(f"An error occurred: {ex}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

# Create a placeholder for the search button in the sidebar
search_placeholder = st.sidebar.empty()
if search_placeholder.button("🔍 Search"):
    # Remove the button and show a spinner in its place
    search_placeholder.empty()
    with st.spinner("Searching..."):
         run_search_logic()
    # Recreate the search button after the search is complete
    search_placeholder.button("🔍 Search", key="search_button")

# Display persistent results and handle pagination
if st.session_state.df_results is not None:
    current_page = st.sidebar.number_input(
        "Page Number",
        min_value=1,
        max_value=st.session_state.df_results['max_page'],
        value=st.session_state.df_results['page'],
        step=1
    )
    
    # If the page number changes, fetch corresponding data
    if current_page != st.session_state.df_results['page']:
        offset = (current_page - 1) * 10000
        final_query = f"""
            SELECT * FROM sec_data {st.session_state.where_clause}
            ORDER BY date, cik
            LIMIT 10000 OFFSET {offset}
        """
        conn = None
        try:
            conn = get_db_connection()
            if conn:
                st.session_state.df_results['data'] = pd.read_sql(final_query, conn)
                st.session_state.df_results['page'] = current_page
        except psycopg2.Error as db_err:
            st.error(f"Database error: {db_err}")
        except tornado.websocket.WebSocketClosedError:
            st.warning("WebSocket closed during pagination.")
        except tornado.iostream.StreamClosedError:
            st.warning("Stream closed during pagination.")
        except Exception as ex:
            st.error(f"An error occurred: {ex}")
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    st.write(f"Displaying page {current_page} of {st.session_state.df_results['max_page']} (Total records: {st.session_state.df_results['total_count']})")
    st.dataframe(st.session_state.df_results['data'])
    
    # Download button for current page
    csv_data = st.session_state.df_results['data'].to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download This Page",
        csv_data,
        "filtered_data_page.csv",
        "text/csv"
    )
