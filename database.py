import duckdb
from models import QueryResult
from config import config

class DatabaseManager:
    def __init__(self):
        self.db_path = config.database_path
    
    def execute_query(self, sql: str) -> QueryResult:
        conn = duckdb.connect(self.db_path)
        try:
            result = conn.execute(sql).fetchall()
            headers = [desc[0] for desc in conn.description]
            print(f"headers: {headers}")
            return QueryResult(sql=sql, data=result, headers=headers, success=True)
        except Exception as e:
            return QueryResult(sql=sql, data=[("Error", str(e))], headers=["Error", "Details"], success=False, error=str(e))
        finally:
            conn.close()