import google.generativeai as genai
from models import AnalysisResult
from config import config

class ResponseGenerator:
    def __init__(self, mode: str = "explore"):
        """
        mode: 'research' for technical, detailed responses
              'explore' for simple, student-friendly responses
        """
        self.mode = mode.lower()
        genai.configure(api_key=config.google_api_key)
        self._model = None

    @property
    def model(self):
        if self._model is None:
            self._model = genai.GenerativeModel(config.gemini_model)
        return self._model

    def generate_response(self, question: str, analysis: AnalysisResult) -> str:
        prompt = self._create_analysis_prompt(question, analysis)
        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            return f"Error generating response: {str(e)}"

    def _create_analysis_prompt(self, question: str, analysis: AnalysisResult) -> str:
        context = f"""
You are an expert oceanographer analyzing Argo float data. Answer the user's question based on the structured analysis provided.

User Question: {question}

Data Analysis:
- Total records: {analysis.record_count}
- Number of floats: {analysis.float_count}
- Number of profiles: {analysis.profile_count}
"""

        if analysis.depth_range:
            context += f"- Depth range: {analysis.depth_range[0]:.1f} to {analysis.depth_range[1]:.1f} dbar\n"

        if analysis.temp_range:
            context += f"- Temperature range: {analysis.temp_range[0]:.2f} to {analysis.temp_range[1]:.2f}°C\n"

        if analysis.geographic_bounds:
            gb = analysis.geographic_bounds
            context += f"- Geographic extent: {gb['lat_min']:.2f}°N to {gb['lat_max']:.2f}°N, {gb['lon_min']:.2f}°E to {gb['lon_max']:.2f}°E\n"

        if analysis.quality_stats:
            context += "- Data quality: " + ", ".join([f"{param}: {pct:.1f}% good" for param, pct in analysis.quality_stats.items()]) + "\n"

        if analysis.key_insights:
            context += "- Key insights: " + "; ".join(analysis.key_insights) + "\n"

        # Add mode-specific instructions
        RESEARCH_MODE_INSTRUCTIONS = """
- Be very technical and precise
- Include exact numeric values, statistics, and IDs
- Provide in-depth insights and observations
- Reference QC flags and data reliability
- Mention temporal or spatial trends
- Use scientific terminology appropriately
- 3-5 detailed sentences
"""

        EXPLORE_MODE_INSTRUCTIONS = """
- Friendly, conversational, easy-to-understand
- Use analogies, examples, and simple explanations
- Highlight key observations clearly
- Avoid heavy jargon
- 2-3 sentences unless more explanation is needed
"""

        instructions = RESEARCH_MODE_INSTRUCTIONS if self.mode == "research" else EXPLORE_MODE_INSTRUCTIONS
        context += instructions

        # Always append general instructions
        context += """
Instructions:
- Use the exact numbers provided in the analysis
- Explain oceanographic concepts when relevant
- Keep the tone professional but accessible
- Structure the response with clear sections if needed
- Don't invent or hallucinate any data not provided
- If the data is limited, acknowledge this appropriately

Generate a comprehensive response:
"""
        return context
