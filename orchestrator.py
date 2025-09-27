from models import OceanographicResponse
from config import config
from database import DatabaseManager
from sql_generator import SQLGenerator
from llm_analyzer import LLMAnalyzer  # Changed to use LLMAnalyzer
from chart_analyzer import ChartAnalyzer

class OrchestratorService:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.sql_generator = SQLGenerator()
        self.analyzer = LLMAnalyzer()  # Using LLMAnalyzer instead of DataAnalyzer
        self.chart_analyzer = ChartAnalyzer()
        self._semantic_model = None
    
    @property
    def semantic_model(self):
        if self._semantic_model is None:
            self._semantic_model = config.load_semantic_model()
        return self._semantic_model
    
    def process_question(self, user_question: str, mode: str = "research") -> OceanographicResponse:
        """
        Process user question and return an OceanographicResponse.
        mode: "research" or "explore" (affects LLM analysis style)
        """
        try:
            sql = self.sql_generator.generate_sql(user_question, self.semantic_model)
            query_result = self.db_manager.execute_query(sql)
            print(f"Query Result: {query_result}")
            print("+" * 100)
            if not query_result.success:
                return OceanographicResponse(
                    question=user_question, sql=sql, results=query_result.data,
                    headers=query_result.headers, analysis=f"Query failed: {query_result.error}",
                    chart_config=None, success=False, error=query_result.error
                )
            
            # Use LLM analyzer for both analysis and response generation
            analysis_result = self.analyzer.analyze_data(user_question, query_result, mode=mode)
            print(f"Analysis Result: {analysis_result}")
            print("+" * 100)
            
            # The insights from LLM are already in natural language format
            text_response = "\n\n".join(analysis_result.key_insights)
            print(f"Text Response: {text_response}")
            
            chart_config = self.chart_analyzer.suggest_chart(user_question, query_result)
            
            return OceanographicResponse(
                question=user_question, sql=sql, results=query_result.data,
                headers=query_result.headers, analysis=text_response,
                chart_config=chart_config, success=True
            )
        except Exception as e:
            return OceanographicResponse(
                question=user_question, sql=f"-- Error: {str(e)}", results=[],
                headers=[], analysis=f"Error: {str(e)}", chart_config=None,
                success=False, error=str(e)
            )

