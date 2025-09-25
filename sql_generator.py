import google.generativeai as genai
from config import config
from typing import Dict, Any

class SQLGenerator:
    def __init__(self):
        genai.configure(api_key=config.google_api_key)
        self._model = None
        self._system_prompt = None
    
    @property
    def model(self):
        if self._model is None:
            self._model = genai.GenerativeModel(config.gemini_model)
        return self._model
    
    def create_system_prompt(self, semantic_model: Dict[str, Any]) -> str:
        table_descriptions = []
        for table_name, table_info in semantic_model['tables'].items():
            desc = table_info['description']
            columns = [f"- {col_name}: {col_info['type']} - {col_info['description']}" 
                      for col_name, col_info in table_info['columns'].items()]
            
            table_desc = f"\n{table_name.upper()} Table:\n"
            table_desc += f"Description: {desc}\n"
            table_desc += "Columns:\n" + "\n".join(columns)
            
            if 'primary_key' in table_info:
                if isinstance(table_info['primary_key'], list):
                    table_desc += f"\nPrimary Key: ({', '.join(table_info['primary_key'])})"
                else:
                    table_desc += f"\nPrimary Key: {table_info['primary_key']}"
            
            if 'foreign_keys' in table_info:
                table_desc += "\nForeign Keys:\n"
                for fk in table_info['foreign_keys']:
                    fk_cols = ', '.join(fk['columns']) if isinstance(fk['columns'], list) else fk['columns']
                    table_desc += f"- {fk_cols} → {fk['references']}\n"
            
            table_descriptions.append(table_desc)

        relationships_desc = "\nTable Relationships:\n"
        if 'relationships' in semantic_model:
            for rel in semantic_model['relationships']:
                rel_desc = rel.get('description', f"{rel['parent']} to {rel['child']}")
                relationships_desc += f"- {rel['name']}: {rel_desc} ({rel['type']})\n"
                relationships_desc += f"  {rel['parent']} → {rel['child']}\n"

        system_prompt = f"""You are a SQL query generator for an oceanographic database containing Argo float data. 
Your task is to generate DuckDB SQL queries based on user questions and the database schema provided below.

Database Schema:
{'\n'.join(table_descriptions)}

{relationships_desc}

Database Structure Summary:
- FLOAT table: Master metadata for each autonomous float (Primary Key: FLOAT_ID)
- PROFILES table: One record per dive cycle (Primary Key: FLOAT_ID, PROFILE_NUMBER)  
- MEASUREMENTS table: Individual sensor readings at each depth level (Foreign Key: FLOAT_ID, PROFILE_NUMBER)

OCEANOGRAPHIC BEST PRACTICES:
- Use quality control flags '1' and '2' for good data: TEMP_QC IN ('1','2'), PSAL_QC IN ('1','2'), PRES_QC IN ('1','2')
- Prefer ascending profiles for better data quality: DIRECTION = 'A'
- Temperature range: -2°C to 35°C (surface), -2°C to 4°C (deep)
- Salinity range: 30-40 PSU
- Pressure range: 0-6000 dbar (decibars ≈ depth in meters)
- Surface measurements: 0-50 dbar
- Thermocline: typically 10-300 dbar
- Deep ocean: 1000+ dbar

Important Guidelines:
1. Table names are: float, profiles, measurements (lowercase)
2. Column names are UPPERCASE (FLOAT_ID, PROFILE_NUMBER, PRES, TEMP, PSAL, etc.)
3. Text search requirements (CRITICAL):
   - ALWAYS use LOWER() for case-insensitive text comparisons on:
     * PI_NAME, PROJECT_NAME, OPERATING_INSTITUTION, FLOAT_OWNER, DEPLOYMENT_PLATFORM
   - Use exact matches for IDs:
     * FLOAT_ID, WMO_INST_TYPE, PLATFORM_NUMBER
   - Use ILIKE for partial matches on:
     * DEPLOYMENT_CRUISE_ID, STATION_PARAMETERS
4. Always use proper table joins based on relationships:
   - float.FLOAT_ID = profiles.FLOAT_ID
   - profiles.FLOAT_ID = measurements.FLOAT_ID AND profiles.PROFILE_NUMBER = measurements.PROFILE_NUMBER
5. For complete float data: JOIN all three tables
6. For specific profile data: JOIN float → profiles → measurements
7. Use appropriate aggregations (AVG, COUNT, SUM, MIN, MAX) when summarizing data
8. Include WHERE clauses to filter out NULL values or invalid measurements where appropriate
9. Use clear column aliasing for better readability
10. Format the SQL query with proper indentation and line breaks
11. For time-based queries, use JULD column (timestamp format)
12. Use PRES column for depth/pressure measurements
13. Use TEMP, PSAL columns for temperature and salinity
14. Consider using adjusted values (*_ADJUSTED columns) for scientific accuracy

Generate only the SQL query without any explanations. The query should be valid DuckDB SQL syntax."""

        return system_prompt
    
    def generate_sql(self, question: str, semantic_model: Dict[str, Any]) -> str:
        if self._system_prompt is None:
            self._system_prompt = self.create_system_prompt(semantic_model)
        
        full_prompt = f"{self._system_prompt}\n\nUser Question: {question}\n\nGenerate a valid DuckDB SQL query. Return ONLY SQL."

        try:
            response = self.model.generate_content(full_prompt)
            sql = response.text.strip()
            
            if sql.startswith("```"):
                sql = sql.split("```")[1].replace("sql", "", 1).strip()
            
            return sql
        except Exception as e:
            print(f"Error from Gemini: {e}")
            return "SELECT 'Error generating SQL' AS error;"