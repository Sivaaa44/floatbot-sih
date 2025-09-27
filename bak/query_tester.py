import duckdb
import pandas as pd
from database import DatabaseManager

def test_query(sql: str):
    """
    Execute a query and display the results in a readable format
    """
    db = DatabaseManager()
    result = db.execute_query(sql)
    
    if result.success:
        # Convert to pandas DataFrame for better display
        df = pd.DataFrame(result.data, columns=result.headers)
        
        print("\n=== Query Results ===")
        print(f"Records found: {len(df)}")
        print("\nData Preview:")
        print(df)
        
        # Show basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if not numeric_cols.empty:
            print("\nNumeric Column Statistics:")
            print(df[numeric_cols].describe())
    else:
        print(f"\nError executing query: {result.error}")

def main():
    print("=== Argo Float Database Query Tester ===")
    print("Enter your SQL query (press Enter twice to execute):")
    print("Type 'exit' to quit")
    
    while True:
        # Collect multi-line query
        lines = []
        while True:
            line = input()
            if line.lower() == 'exit':
                return
            if line == '':
                break
            lines.append(line)
        
        if not lines:
            continue
            
        query = ' '.join(lines)
        print("\nExecuting query:")
        print("-" * 50)
        print(query)
        print("-" * 50)
        
        test_query(query)
        print("\nEnter next query (or 'exit' to quit):")

if __name__ == "__main__":
    main()