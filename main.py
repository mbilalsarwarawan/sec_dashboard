import duckdb
import streamlit as st
import os
import glob
import math
from datetime import date
import pandas as pd

# Define folder path containing your CSV files
folder_path = os.path.abspath("files/")
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
if not csv_files:
    st.error("🚨 No CSV files found! Please check the folder path.")
    st.stop()

# Base query (without filters) that reads from CSVs.
# This query is NOT executed unless at least one filter is provided.
base_query = f"""
SELECT 
    date, 
    cik, 
    COALESCE("Company Name", 'Unknown') AS Company_Name, 
    total_records, 
    defm14a, 
    sc13e3a, 
    sc13e3, 
    "10q", 
    "10-q", 
    "8k", 
    form4, 
    "10k", 
    "10-k", 
    proxy, 
    ownership
FROM read_csv_auto('{folder_path}/*.csv',
    header=true, 
    quote='"', 
    strict_mode=false, 
    ignore_errors=true
)
"""

# Setup Streamlit layout
st.set_page_config(page_title="SEC Filings Data Viewer", layout="wide")
st.title("📊 SEC Filings Data Viewer")

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Choose a date filter mode
date_filter_mode = st.sidebar.radio("Date Filter Mode", 
                                    ["No Date Filter", "Single Date", "Date Range"],
                                    index=0)

filter_conditions = []  # Will accumulate SQL WHERE conditions

# For Single Date or Date Range, we want to provide default values.
if date_filter_mode == "Single Date":
    selected_date = st.sidebar.date_input("Select Date", date.today())
    # Add filter condition
    filter_conditions.append(f"date = '{selected_date}'")
elif date_filter_mode == "Date Range":
    start_date = st.sidebar.date_input("Start Date", date.today())
    end_date = st.sidebar.date_input("End Date", date.today())
    if start_date and end_date:
        filter_conditions.append(f"date >= '{start_date}' AND date <= '{end_date}'")
# When "No Date Filter" is selected, we do not add any date condition.

# CIK filter: Exact match (if provided)
cik_input = st.sidebar.text_input("Enter CIK (Exact Match)")
if cik_input:
    filter_conditions.append(f"CAST(cik AS VARCHAR) = '{cik_input}'")

# Check if at least one filter is provided.
if not filter_conditions:
    st.info("Please select at least one filter (date or CIK) to load data.")
    st.stop()  # Do not execute further queries

# Build WHERE clause from filters
where_clause = " WHERE " + " AND ".join(filter_conditions)

# Build final filtered query with LIMIT/OFFSET for pagination.
filtered_query = base_query + where_clause

# Count total matching records (to set up pagination)
count_query = f"SELECT COUNT(*) AS total FROM ({filtered_query}) AS subquery"
try:
    total_count_df = duckdb.sql(count_query).fetchdf()
    total_count = total_count_df.iloc[0]["total"]
except Exception as e:
    st.error(f"🚨 Error fetching record count: {e}")
    st.stop()

# Pagination settings: load only 10,000 records per page.
rows_per_page = 10000
max_page = math.ceil(total_count / rows_per_page) if total_count > 0 else 1
st.sidebar.markdown(f"**Total matching records:** {total_count}")

page = st.sidebar.number_input("Page Number", min_value=1, max_value=max_page, value=1, step=1)
offset = (page - 1) * rows_per_page

# Append ORDER BY, LIMIT, and OFFSET to the query
final_query = filtered_query + f" ORDER BY date, cik LIMIT {rows_per_page} OFFSET {offset}"

try:
    df = duckdb.sql(final_query).fetchdf()
except Exception as e:
    st.error(f"🚨 Error loading data: {e}")
    st.stop()

st.write(f"Displaying page {page} of {max_page}")
st.dataframe(df)

# Download button for the current page's data
csv_data = df.to_csv(index=False).encode("utf-8")
st.download_button("📥 Download This Page", csv_data, "filtered_data_page.csv", "text/csv")
