import os
from typing import List, Dict, Optional, Tuple
import google.generativeai as genai
from models import QueryResult, AnalysisResult
from dotenv import load_dotenv
load_dotenv()
RESEARCH_MODE_PROMPT = """
You are an expert oceanographic analyst providing clear, concise insights about Argo float data. 

RESPONSE STYLE:
- Be direct and to the point
- Very technical and precise
- Include exact numeric values, statistics, and IDs
- Reference QC flags and data reliability
- Mention temporal or spatial trends
- Use scientific terminology appropriately
- 3-5 detailed sentences unless brevity is requested

KNOWLEDGE BASE:
- Floats collect temperature (°C), salinity (PSU), and pressure (dbar) data
- QC flags 1 or 2 indicate reliable data
- Surface: 0-50 dbar
- Thermocline: 10-300 dbar
- Deep ocean: 1000+ dbar
- Include statistical summaries, min/max/avg, counts, and trends- Include statistical summaries, min/max/avg, counts, and trends

FORMAT YOUR RESPONSE AS:
1. Direct, technical answer
2. Additional observations or patterns
3. Brief quality note (only if data quality issues exist)

Data provided will include:
1. User's original question
2. SQL query used
3. Column headers
4. Query results
5. Basic statistics about the results

Generate a natural, informative response that directly answers the user's question while providing relevant context and insights from the data.
"""
EXPLORE_MODE_PROMPT = """
You are an oceanography science explainer for students or non-technical users.

RESPONSE STYLE:
- Friendly, conversational, easy-to-understand
- Use analogies, examples, and simple explanations
- Highlight key observations clearly
- Avoid heavy jargon
- 2-3 sentences unless more explanation is needed

KNOWLEDGE BASE:
- Floats measure temperature (°C), salinity (PSU), pressure (dbar)
- Surface: 0-50 dbar, Thermocline: 10-300 dbar, Deep: 1000+ dbar
- Data quality flags: 1 or 2 = good, explain simply if issues exist
- Include trends, patterns, or notable observations in plain language

FORMAT RESPONSE:
1. Direct answer
2. One extra interesting insight or fun fact
3. Brief note if data quality is low
"""
class LLMAnalyzer:
    def __init__(self):
        # Configure Gemini (or your chosen LLM)
        GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
        if not GOOGLE_API_KEY:
            raise ValueError("GOOGLE_API_KEY not found in environment variables")
        genai.configure(api_key=GOOGLE_API_KEY)
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')


    def analyze_data(self, question: str, query_result: QueryResult, mode: str = "research") -> AnalysisResult:
        """
        Analyze query results using LLM and generate insights
        """
        if not query_result.success or not query_result.data:
            return AnalysisResult(0, 0, 0, key_insights=["No data available for analysis"])
        mode = mode.lower()
        if mode == "research":
         system_prompt = RESEARCH_MODE_PROMPT
        else:
         system_prompt = EXPLORE_MODE_PROMPT
        # Calculate basic statistics
        stats = self._calculate_basic_stats(query_result)
        
        # Prepare data for LLM analysis
        analysis_prompt = self._prepare_analysis_prompt(question, query_result, stats)
        try:
            # Generate analysis using LLM
            response = self.model.generate_content([analysis_prompt, system_prompt])
            insights = self._parse_llm_response(response.text)
        except Exception as e:
            insights = [f"Error generating analysis: {str(e)}"]

        # Return complete analysis result
        return AnalysisResult(
            record_count=stats['record_count'],
            float_count=stats['float_count'],
            profile_count=stats['profile_count'],
            depth_range=stats.get('depth_range'),
            temp_range=stats.get('temp_range'),
            quality_stats=stats.get('quality_stats', {}),
            geographic_bounds=stats.get('geographic_bounds'),
            key_insights=insights
        )

    def _calculate_basic_stats(self, query_result: QueryResult) -> Dict:
        """Calculate basic statistics from the query results"""
        data = query_result.data
        headers = query_result.headers
        col_idx = {col: i for i, col in enumerate(headers)}
        
        stats = {
            'record_count': len(data),
            'float_count': len({row[col_idx['FLOAT_ID']] for row in data if 'FLOAT_ID' in col_idx}),
            'profile_count': 0,  # Will be calculated if relevant columns exist
            'column_summaries': {}
        }

        # Generate summaries for numeric columns
        for col in headers:
            col_data = []
            for row in data:
                val = row[col_idx[col]]
                if val is not None:
                    try:
                        col_data.append(float(val))
                    except (ValueError, TypeError):
                        continue
            
            if col_data:
                stats['column_summaries'][col] = {
                    'min': min(col_data),
                    'max': max(col_data),
                    'avg': sum(col_data) / len(col_data)
                }

        return stats

    def _prepare_analysis_prompt(self, question: str, query_result: QueryResult, stats: Dict) -> str:
        """Prepare a focused prompt for the LLM analysis"""
        prompt = f"""
QUESTION: {question}

DATA:
{self._format_sample_data(query_result.data[:5], query_result.headers)}

STATISTICS:
{self._format_column_stats(stats['column_summaries'])}

Provide a clear, concise analysis focusing on directly answering the question. Include only relevant additional insights."""
        return prompt

    def _format_sample_data(self, data: List[Tuple], headers: List[str]) -> str:
        """Format sample data rows for the prompt"""
        if not data:
            return "No data available"
            
        formatted_rows = []
        formatted_rows.append(" | ".join(str(h) for h in headers))
        formatted_rows.append("-" * (len(formatted_rows[0])))
        
        for row in data:
            formatted_rows.append(" | ".join(str(val) for val in row))
            
        return "\n".join(formatted_rows)

    def _format_column_stats(self, column_stats: Dict) -> str:
        """Format column statistics for the prompt"""
        if not column_stats:
            return "No numeric columns available"
            
        formatted_stats = []
        for col, stats in column_stats.items():
            formatted_stats.append(f"{col}:")
            for stat, value in stats.items():
                formatted_stats.append(f"  - {stat}: {value:.2f}")
                
        return "\n".join(formatted_stats)

    def _parse_llm_response(self, response: str) -> List[str]:
        """Parse the LLM response into a list of insights"""
        # Split response into separate insights
        insights = [line.strip() for line in response.split('\n') if line.strip()]
        return insights