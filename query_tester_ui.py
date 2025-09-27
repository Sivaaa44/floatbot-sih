import streamlit as st
import pandas as pd
import duckdb
from database import DatabaseManager
import os

# Set pandas display options
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def main():
    st.set_page_config(page_title="SQL Query Tester", layout="wide")
    st.title("SQL Query Tester")

    # Debug info
    db_path = os.path.join(os.path.dirname(__file__), "argo_floats.db")
    st.sidebar.write(f"Database path: {db_path}")
    st.sidebar.write(f"Database exists: {os.path.exists(db_path)}")

    # Initialize database manager with error handling
    try:
        db = DatabaseManager()
        st.sidebar.success("Database connected successfully!")
    except Exception as e:
        st.sidebar.error(f"Database connection failed: {str(e)}")
        return



    # Create main query editor
    st.markdown("### SQL Query Editor")
    st.markdown("*Write multiple queries and highlight the one you want to execute*")
    query_editor = st.text_area(
        "",  # No label, we used markdown above
        height=200)
        

    # Create area for selected query
    col1, col2 = st.columns([4, 1])
    with col1:
        selected_query = st.text_area(
            "",  # No label
            key="selected_query",
            height=100,
            placeholder="Highlighted query will appear here..."
        )
    with col2:
        st.markdown("###")  # Add some spacing
        execute_button = st.button("Execute Selected Query")

  

    # Execute query button
    if execute_button:
        if selected_query:
            try:
                st.info("Executing selected query...")
                
                result = db.execute_query(selected_query)
                if result.success:
                    # Handle duplicate columns for JOIN queries
                    renamed_headers = []
                    seen = set()
                    for header in result.headers:
                        if header in seen:
                            renamed_headers.append(f"{header}_joined")
                        else:
                            renamed_headers.append(header)
                            seen.add(header)
                    
                    df = pd.DataFrame(result.data, columns=renamed_headers)
                    
                    # Display results
                    st.success(f"Query executed successfully! Found {len(df)} records.")
                    
                    # Show dataframe with pagination
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        page_size = st.slider("Rows per page", min_value=10, max_value=100, value=50)
                    with col2:
                        total_pages = (len(df) + page_size - 1) // page_size
                        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1) - 1
                    
                    start_idx = page * page_size
                    end_idx = start_idx + page_size
                    
                    st.dataframe(
                        df.iloc[start_idx:end_idx],
                        use_container_width=True,
                        height=400
                    )
                    
                    st.write(f"Showing {start_idx+1} to {min(end_idx, len(df))} of {len(df)} records")
                    
                    # Add download button
                    st.download_button(
                        label="Download full results as CSV",
                        data=df.to_csv(index=False).encode('utf-8'),
                        file_name='query_results.csv',
                        mime='text/csv'
                    )
                    
                    # Show statistics for numeric columns
                    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                    if not numeric_cols.empty:
                        with st.expander("View Numeric Column Statistics"):
                            st.dataframe(
                                df[numeric_cols].describe(),
                                use_container_width=True
                            )
                else:
                    st.error(f"Error executing query: {result.error}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.error("Please check your query syntax and try again.")
        else:
            st.warning("Please highlight a query to execute.")

   

if __name__ == "__main__":
    main()