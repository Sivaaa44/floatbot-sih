
# import google.generativeai as genai
# import yaml
# import duckdb
# from dotenv import load_dotenv
# import os

# # Load environment variables
# load_dotenv()

# # Configure Gemini API
# GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
# if not GOOGLE_API_KEY:
#     raise ValueError("GOOGLE_API_KEY not found in environment variables")
# genai.configure(api_key=GOOGLE_API_KEY)


# def load_semantic_model():
#     """Load the semantic model from YAML file"""
#     with open('argo_semantic_model.yaml', 'r') as file:
#         return yaml.safe_load(file)


# def create_system_prompt(semantic_model):
#     """Create a system prompt that explains the database structure and how to generate SQL queries"""
    
#     # Build table descriptions from semantic model
#     table_descriptions = []
#     for table_name, table_info in semantic_model['tables'].items():
#         desc = table_info['description']
#         columns = [f"- {col_name}: {col_info['type']} - {col_info['description']}" 
#                   for col_name, col_info in table_info['columns'].items()]
        
#         table_desc = f"\n{table_name.upper()} Table:\n"
#         table_desc += f"Description: {desc}\n"
#         table_desc += "Columns:\n" + "\n".join(columns)
        
#         # Add primary key info
#         if 'primary_key' in table_info:
#             if isinstance(table_info['primary_key'], list):
#                 table_desc += f"\nPrimary Key: ({', '.join(table_info['primary_key'])})"
#             else:
#                 table_desc += f"\nPrimary Key: {table_info['primary_key']}"
        
#         # Add foreign key info
#         if 'foreign_keys' in table_info:
#             table_desc += "\nForeign Keys:\n"
#             for fk in table_info['foreign_keys']:
#                 fk_cols = ', '.join(fk['columns']) if isinstance(fk['columns'], list) else fk['columns']
#                 table_desc += f"- {fk_cols} → {fk['references']}\n"
        
#         table_descriptions.append(table_desc)

#     # Add relationship information
#     relationships_desc = "\nTable Relationships:\n"
#     if 'relationships' in semantic_model:
#         for rel in semantic_model['relationships']:
#             rel_desc = rel.get('description', f"{rel['parent']} to {rel['child']}")
#             relationships_desc += f"- {rel['name']}: {rel_desc} ({rel['type']})\n"
#             relationships_desc += f"  {rel['parent']} → {rel['child']}\n"

#     system_prompt = f"""You are a SQL query generator for an oceanographic database containing Argo float data. 
# Your task is to generate DuckDB SQL queries based on user questions and the database schema provided below.

# Database Schema:
# {'\n'.join(table_descriptions)}

# {relationships_desc}

# Database Structure Summary:
# - FLOAT table: Master metadata for each autonomous float (Primary Key: FLOAT_ID)
# - PROFILES table: One record per dive cycle (Primary Key: FLOAT_ID, PROFILE_NUMBER)  
# - MEASUREMENTS table: Individual sensor readings at each depth level (Foreign Key: FLOAT_ID, PROFILE_NUMBER)

# OCEANOGRAPHIC BEST PRACTICES:
# - Use quality control flags '1' and '2' for good data: TEMP_QC IN ('1','2'), PSAL_QC IN ('1','2'), PRES_QC IN ('1','2')
# - Prefer ascending profiles for better data quality: DIRECTION = 'A'
# - Temperature range: -2°C to 35°C (surface), -2°C to 4°C (deep)
# - Salinity range: 30-40 PSU
# - Pressure range: 0-6000 dbar (decibars ≈ depth in meters)
# - Surface measurements: 0-50 dbar
# - Thermocline: typically 10-300 dbar
# - Deep ocean: 1000+ dbar

# Important Guidelines:
# 1. Table names are: float, profiles, measurements (lowercase)
# 2. Column names are UPPERCASE (FLOAT_ID, PROFILE_NUMBER, PRES, TEMP, PSAL, etc.)
# 3. Text search requirements (CRITICAL):
#    - ALWAYS use LOWER() for case-insensitive text comparisons on:
#      * PI_NAME, PROJECT_NAME, OPERATING_INSTITUTION, FLOAT_OWNER, DEPLOYMENT_PLATFORM
#    - Use exact matches for IDs:
#      * FLOAT_ID, WMO_INST_TYPE, PLATFORM_NUMBER
#    - Use ILIKE for partial matches on:
#      * DEPLOYMENT_CRUISE_ID, STATION_PARAMETERS
# 4. Always use proper table joins based on relationships:
#    - float.FLOAT_ID = profiles.FLOAT_ID
#    - profiles.FLOAT_ID = measurements.FLOAT_ID AND profiles.PROFILE_NUMBER = measurements.PROFILE_NUMBER
# 5. For complete float data: JOIN all three tables
# 6. For specific profile data: JOIN float → profiles → measurements
# 7. Use appropriate aggregations (AVG, COUNT, SUM, MIN, MAX) when summarizing data
# 8. Include WHERE clauses to filter out NULL values or invalid measurements where appropriate
# 9. Use clear column aliasing for better readability
# 10. Format the SQL query with proper indentation and line breaks
# 11. For time-based queries, use JULD column (timestamp format)
# 12. Use PRES column for depth/pressure measurements
# 13. Use TEMP, PSAL columns for temperature and salinity
# 14. Consider using adjusted values (*_ADJUSTED columns) for scientific accuracy

# Example Query Patterns:
# - All data for a float: 
#   SELECT * FROM float f 
#   JOIN profiles p ON f.FLOAT_ID = p.FLOAT_ID 
#   JOIN measurements m ON p.FLOAT_ID = m.FLOAT_ID AND p.PROFILE_NUMBER = m.PROFILE_NUMBER 
#   WHERE f.FLOAT_ID = 'floatid'

# - Case-insensitive name search:
#   SELECT * FROM float 
#   WHERE LOWER(PI_NAME) = LOWER('Xavier CAPET')

# - Partial text search:
#   SELECT * FROM float 
#   WHERE DEPLOYMENT_CRUISE_ID ILIKE '%PIRATA%'

# - Temperature difference surface to deep:
#   WITH surface_deep AS (
#       SELECT FLOAT_ID, PROFILE_NUMBER,
#              MIN(PRES) as min_pres, MAX(PRES) as max_pres
#       FROM measurements
#       WHERE FLOAT_ID = 'floatid' AND PROFILE_NUMBER = 1
#         AND TEMP_QC IN ('1','2') AND PRES_QC IN ('1','2')
#       GROUP BY FLOAT_ID, PROFILE_NUMBER
#   ),
#   temps AS (
#       SELECT sd.FLOAT_ID, sd.PROFILE_NUMBER,
#              (SELECT TEMP FROM measurements m1 WHERE m1.FLOAT_ID = sd.FLOAT_ID AND m1.PROFILE_NUMBER = sd.PROFILE_NUMBER AND m1.PRES = sd.min_pres) as surface_temp,
#              (SELECT TEMP FROM measurements m2 WHERE m2.FLOAT_ID = sd.FLOAT_ID AND m2.PROFILE_NUMBER = sd.PROFILE_NUMBER AND m2.PRES = sd.max_pres) as deep_temp
#       FROM surface_deep sd
#   )
#   SELECT surface_temp, deep_temp, (surface_temp - deep_temp) as temp_difference
#   FROM temps;

# Generate only the SQL query without any explanations. The query should be valid DuckDB SQL syntax."""

#     return system_prompt

# def generate_sql_query(prompt, system_prompt):
#     """Generate SQL query using Gemini"""
#     try:
#         print(f"\n🔍 Generating SQL for: {prompt}")
        
#         # Configure the model with safety settings
#         generation_config = {
#             "temperature": 0.1,  # Very low temperature for precise SQL generation
#             "top_p": 0.8,
#             "top_k": 40,
#             "max_output_tokens": 2048,
#         }

#         safety_settings = [
#             {
#                 "category": "HARM_CATEGORY_DANGEROUS",
#                 "threshold": "BLOCK_MEDIUM_AND_ABOVE",
#             },
#         ]

#         model = genai.GenerativeModel(
#             model_name="gemini-2.0-flash-exp",
#             generation_config=generation_config,
#             safety_settings=safety_settings
#         )
        
#         # Combine system prompt and user query
#         full_prompt = f"{system_prompt}\n\nUser Question: {prompt}\n\nGenerate a valid DuckDB SQL query to answer this question. Return only the SQL query without any explanations or markdown formatting:"
        
#         print("📤 Sending request to Gemini...")
        
#         try:
#             response = model.generate_content(full_prompt)
#             print(f"📥 Raw response received: {len(response.text) if response.text else 0} characters")
            
#             if response.text:
#                 print(f"📝 Raw response preview: {response.text[:200]}...")
                
#                 # Clean up the response - remove any markdown formatting if present
#                 sql = response.text.strip()
#                 if sql.startswith("```sql"):
#                     sql = sql[6:]
#                 elif sql.startswith("```"):
#                     sql = sql[3:]
#                 if sql.endswith("```"):
#                     sql = sql[:-3]
                
#                 # Remove any explanatory text before or after the SQL
#                 lines = sql.split('\n')
#                 sql_lines = []
#                 in_sql = False
                
#                 for line in lines:
#                     line = line.strip()
#                     if line.upper().startswith(('SELECT', 'WITH', 'CREATE', 'INSERT', 'UPDATE', 'DELETE')):
#                         in_sql = True
#                     if in_sql and line:
#                         sql_lines.append(line)
#                     if line.endswith(';') and in_sql:
#                         break
                
#                 if sql_lines:
#                     clean_sql = '\n'.join(sql_lines)
#                     print(f"✅ Generated clean SQL ({len(clean_sql)} chars)")
#                     return clean_sql
#                 else:
#                     print(f"⚠️ Using raw SQL as fallback")
#                     return sql.strip()
#             else:
#                 print("❌ Empty response from model")
#                 raise Exception("Empty response from model")
#         except Exception as e:
#             print(f"❌ Error generating content: {str(e)}")
#             # Return a simple test query as fallback
#             return "SELECT 'Error: Could not generate query' as error_message;"
            
#     except Exception as e:
#         print(f"❌ Error initializing model: {str(e)}")
#         return "SELECT 'Error: Model initialization failed' as error_message;"

# def execute_query(sql):
#     """Execute the generated SQL query on DuckDB"""
#     conn = duckdb.connect('argo_floats.db')
#     try:
#         print("\nExecuting SQL:")
#         print("-" * 50)
#         print(sql)
#         print("-" * 50)
        
#         # Execute the query
#         result = conn.execute(sql).fetchall()
        
#         # Get column names from the last query
#         col_names = [desc[0] for desc in conn.description]
#         return result, col_names
#     except Exception as e:
#         print(f"Error executing query: {str(e)}")
#         return [("Error executing query", str(e))], ["Error", "Details"]
#     finally:
#         conn.close()

# def generate_summary(question, results, headers):
#     """Generate human-readable summary of query results"""
#     if not results or any("Error" in str(h) for h in headers):
#         return "Unable to generate summary - query returned an error."
    
#     try:
#         # Create a simple summary based on the data
#         num_rows = len(results)
        
#         if num_rows == 0:
#             return "No data found matching your query criteria."
        
#         # Basic summary template
#         summary = f"Based on your question '{question}', I found {num_rows} result{'s' if num_rows != 1 else ''} in the Argo float database.\n\n"
        
#         # Add context based on question type
#         question_lower = question.lower()
        
#         if 'temperature' in question_lower and 'profile' in question_lower:
#             if headers and len(results[0]) >= 3:
#                 try:
#                     surface_temp = results[0][0] if results[0][0] is not None else "N/A"
#                     deep_temp = results[0][1] if results[0][1] is not None else "N/A" 
#                     temp_diff = results[0][2] if results[0][2] is not None else "N/A"
                    
#                     summary += f"Temperature Analysis:\n"
#                     summary += f"• Surface Temperature: {surface_temp}°C\n"
#                     summary += f"• Deep Temperature: {deep_temp}°C\n"
#                     summary += f"• Temperature Difference: {temp_diff}°C\n\n"
#                     summary += "This temperature difference reflects the thermal stratification of the ocean, where surface waters are typically warmer due to solar heating, while deeper waters remain cold."
#                 except:
#                     pass
        
#         elif 'float' in question_lower and ('most' in question_lower or 'coverage' in question_lower):
#             summary += "Float Coverage Analysis:\n"
#             if num_rows > 0:
#                 summary += f"Found {num_rows} floats with varying levels of vertical coverage. "
#                 summary += "Floats with better coverage provide more comprehensive oceanographic data from surface to deep waters."
        
#         elif 'quality' in question_lower:
#             summary += "Data Quality Assessment:\n"
#             summary += f"Analyzed quality control statistics across {num_rows} parameter types. "
#             summary += "Quality flags help ensure scientific analyses use only reliable measurements."
        
#         else:
#             # Generic summary
#             if headers:
#                 summary += f"Key findings include data on: {', '.join(headers[:3])}{'...' if len(headers) > 3 else ''}.\n\n"
#             summary += "The results show oceanographic measurements from Argo autonomous floats, which provide critical data for understanding ocean conditions and climate patterns."
        
#         return summary
        
#     except Exception as e:
#         return f"Summary generation failed: {str(e)}"
#     """Format query results for display"""
#     if not results:
#         return "No results found."
    
#     # If headers aren't provided, use generic column names
#     if not headers:
#         headers = [f"Column_{i}" for i in range(len(results[0]))]
    
#     # Limit results display to first 20 rows
#     display_results = results[:20]
#     if len(results) > 20:
#         truncated_msg = f"\n... ({len(results) - 20} more rows truncated for display)"
#     else:
#         truncated_msg = ""
    
#     # Calculate column widths (max 50 chars per column for readability)
#     widths = []
#     for i in range(len(headers)):
#         max_width = max(len(str(headers[i])), 
#                        max(len(str(row[i])[:50]) for row in display_results))
#         widths.append(min(max_width, 50))
    
#     # Create the header
#     header = " | ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
#     separator = "-" * len(header)
    
#     # Format each row
#     rows = [header, separator]
#     for row in display_results:
#         formatted_row = " | ".join(f"{str(val)[:50]:<{w}}" for val, w in zip(row, widths))
#         rows.append(formatted_row)
    
#     return "\n".join(rows) + truncated_msg

# def main():
#     """Main interactive function"""
#     print("🌊 Argo FloatBot Query Generator")
#     print("=" * 60)
    
#     # Load semantic model
#     try:
#         semantic_model = load_semantic_model()
#         print("✅ Semantic model loaded successfully")
#     except Exception as e:
#         print(f"❌ Error loading semantic model: {e}")
#         return
    
#     # Create system prompt
#     system_prompt = create_system_prompt(semantic_model)
    
#     # Example questions for reference
#     example_questions = [
#         "Show me all floats and their basic information",
#         "What is the average temperature and salinity for each float with good quality data?",
#         "Find the deepest measurement (highest pressure) for each float",
#         "Which floats have the most profiles?",
#         "Show temperature profiles for float '6903016'",
#         "How much has temperature fallen from surface to deepest point in profile 1 of float 6903016?",
#         "Calculate average temperature at different pressure ranges (0-100, 100-500, 500+ decibars)",
#         "Find profiles with the highest and lowest salinities",
#         "Show data quality statistics for all floats"
#     ]
    
#     print("\n📋 Example Questions:")
#     for i, question in enumerate(example_questions, 1):
#         print(f"{i}. {question}")
    
#     print("\n" + "=" * 60)
    
#     while True:
#         print("\n🤖 Enter your question about Argo float data (or 'quit' to exit):")
#         user_input = input("> ").strip()
        
#         if user_input.lower() in ['quit', 'exit', 'q']:
#             print("👋 Goodbye!")
#             break
            
#         if not user_input:
#             continue
        
#         # Check if user entered a number to select example question
#         if user_input.isdigit():
#             question_num = int(user_input)
#             if 1 <= question_num <= len(example_questions):
#                 user_question = example_questions[question_num - 1]
#                 print(f"\n📝 Selected Question: {user_question}")
#             else:
#                 print("❌ Invalid question number")
#                 continue
#         else:
#             user_question = user_input
        
#         # Generate SQL query
#         print("\n🔍 Generating SQL query...")
#         sql_query = generate_sql_query(user_question, system_prompt)
        
#         print("\n📊 Generated SQL Query:")
#         print("-" * 50)
#         print(sql_query)
#         print("-" * 50)
        
#         # Ask user if they want to execute
#         execute_choice = input("\n⚡ Execute this query? (y/n/edit): ").strip().lower()
        
#         if execute_choice == 'edit':
#             print("\n✏️ Enter your modified SQL query:")
#             modified_sql = input("> ")
#             if modified_sql.strip():
#                 sql_query = modified_sql
        
#         if execute_choice in ['y', 'yes', 'edit']:
#             # Execute query
#             print("\n⚙️ Executing query...")
#             results, headers = execute_query(sql_query)
            
#             print(f"\n📈 Results ({len(results)} rows):")
          
#         else:
#             print("⏭️ Query not executed")

# if __name__ == "__main__":
#     main()





# import google.generativeai as genai
# import yaml
# import duckdb
# from dotenv import load_dotenv
# import os
# from transformers import pipeline
# import pandas as pd

# # Load environment variables
# load_dotenv()

# # Configure Gemini API
# GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
# if not GOOGLE_API_KEY:
#     raise ValueError("GOOGLE_API_KEY not found in environment variables")
# genai.configure(api_key=GOOGLE_API_KEY)

# # Initialize free text generation model for summaries
# try:
#     summarizer = pipeline("text2text-generation", model="google/flan-t5-base", max_length=512)
# except Exception as e:
#     print(f"Warning: Could not load summarization model: {e}")
#     summarizer = None


# def load_semantic_model():
#     """Load the semantic model from YAML file"""
#     with open('argo_semantic_model.yaml', 'r') as file:
#         return yaml.safe_load(file)


# def create_system_prompt(semantic_model):
#     """Create a system prompt that explains the database structure and how to generate SQL queries"""
    
#     # Build table descriptions from semantic model
#     table_descriptions = []
#     for table_name, table_info in semantic_model['tables'].items():
#         desc = table_info['description']
#         columns = [f"- {col_name}: {col_info['type']} - {col_info['description']}" 
#                   for col_name, col_info in table_info['columns'].items()]
        
#         table_desc = f"\n{table_name.upper()} Table:\n"
#         table_desc += f"Description: {desc}\n"
#         table_desc += "Columns:\n" + "\n".join(columns)
        
#         # Add primary key info
#         if 'primary_key' in table_info:
#             if isinstance(table_info['primary_key'], list):
#                 table_desc += f"\nPrimary Key: ({', '.join(table_info['primary_key'])})"
#             else:
#                 table_desc += f"\nPrimary Key: {table_info['primary_key']}"
        
#         # Add foreign key info
#         if 'foreign_keys' in table_info:
#             table_desc += "\nForeign Keys:\n"
#             for fk in table_info['foreign_keys']:
#                 fk_cols = ', '.join(fk['columns']) if isinstance(fk['columns'], list) else fk['columns']
#                 table_desc += f"- {fk_cols} → {fk['references']}\n"
        
#         table_descriptions.append(table_desc)

#     # Add relationship information
#     relationships_desc = "\nTable Relationships:\n"
#     if 'relationships' in semantic_model:
#         for rel in semantic_model['relationships']:
#             rel_desc = rel.get('description', f"{rel['parent']} to {rel['child']}")
#             relationships_desc += f"- {rel['name']}: {rel_desc} ({rel['type']})\n"
#             relationships_desc += f"  {rel['parent']} → {rel['child']}\n"

#     system_prompt = f"""You are a SQL query generator for an oceanographic database containing Argo float data. 
# Your task is to generate DuckDB SQL queries based on user questions and the database schema provided below.

# Database Schema:
# {'\n'.join(table_descriptions)}

# {relationships_desc}

# Database Structure Summary:
# - FLOAT table: Master metadata for each autonomous float (Primary Key: FLOAT_ID)
# - PROFILES table: One record per dive cycle (Primary Key: FLOAT_ID, PROFILE_NUMBER)  
# - MEASUREMENTS table: Individual sensor readings at each depth level (Foreign Key: FLOAT_ID, PROFILE_NUMBER)

# OCEANOGRAPHIC BEST PRACTICES:
# - Use quality control flags '1' and '2' for good data: TEMP_QC IN ('1','2'), PSAL_QC IN ('1','2'), PRES_QC IN ('1','2')
# - Prefer ascending profiles for better data quality: DIRECTION = 'A'
# - Temperature range: -2°C to 35°C (surface), -2°C to 4°C (deep)
# - Salinity range: 30-40 PSU
# - Pressure range: 0-6000 dbar (decibars ≈ depth in meters)
# - Surface measurements: 0-50 dbar
# - Thermocline: typically 10-300 dbar
# - Deep ocean: 1000+ dbar

# Important Guidelines:
# 1. Table names are: float, profiles, measurements (lowercase)
# 2. Column names are UPPERCASE (FLOAT_ID, PROFILE_NUMBER, PRES, TEMP, PSAL, etc.)
# 3. Text search requirements (CRITICAL):
#    - ALWAYS use LOWER() for case-insensitive text comparisons on:
#      * PI_NAME, PROJECT_NAME, OPERATING_INSTITUTION, FLOAT_OWNER, DEPLOYMENT_PLATFORM
#    - Use exact matches for IDs:
#      * FLOAT_ID, WMO_INST_TYPE, PLATFORM_NUMBER
#    - Use ILIKE for partial matches on:
#      * DEPLOYMENT_CRUISE_ID, STATION_PARAMETERS
# 4. Always use proper table joins based on relationships:
#    - float.FLOAT_ID = profiles.FLOAT_ID
#    - profiles.FLOAT_ID = measurements.FLOAT_ID AND profiles.PROFILE_NUMBER = measurements.PROFILE_NUMBER
# 5. For complete float data: JOIN all three tables
# 6. For specific profile data: JOIN float → profiles → measurements
# 7. Use appropriate aggregations (AVG, COUNT, SUM, MIN, MAX) when summarizing data
# 8. Include WHERE clauses to filter out NULL values or invalid measurements where appropriate
# 9. Use clear column aliasing for better readability
# 10. Format the SQL query with proper indentation and line breaks
# 11. For time-based queries, use JULD column (timestamp format)
# 12. Use PRES column for depth/pressure measurements
# 13. Use TEMP, PSAL columns for temperature and salinity
# 14. Consider using adjusted values (*_ADJUSTED columns) for scientific accuracy

# Example Query Patterns:
# - All data for a float: 
#   SELECT * FROM float f 
#   JOIN profiles p ON f.FLOAT_ID = p.FLOAT_ID 
#   JOIN measurements m ON p.FLOAT_ID = m.FLOAT_ID AND p.PROFILE_NUMBER = m.PROFILE_NUMBER 
#   WHERE f.FLOAT_ID = 'floatid'

# - Case-insensitive name search:
#   SELECT * FROM float 
#   WHERE LOWER(PI_NAME) = LOWER('Xavier CAPET')

# - Partial text search:
#   SELECT * FROM float 
#   WHERE DEPLOYMENT_CRUISE_ID ILIKE '%PIRATA%'

# - Temperature difference surface to deep:
#   WITH surface_deep AS (
#       SELECT FLOAT_ID, PROFILE_NUMBER,
#              MIN(PRES) as min_pres, MAX(PRES) as max_pres
#       FROM measurements
#       WHERE FLOAT_ID = 'floatid' AND PROFILE_NUMBER = 1
#         AND TEMP_QC IN ('1','2') AND PRES_QC IN ('1','2')
#       GROUP BY FLOAT_ID, PROFILE_NUMBER
#   ),
#   temps AS (
#       SELECT sd.FLOAT_ID, sd.PROFILE_NUMBER,
#              (SELECT TEMP FROM measurements m1 WHERE m1.FLOAT_ID = sd.FLOAT_ID AND m1.PROFILE_NUMBER = sd.PROFILE_NUMBER AND m1.PRES = sd.min_pres) as surface_temp,
#              (SELECT TEMP FROM measurements m2 WHERE m2.FLOAT_ID = sd.FLOAT_ID AND m2.PROFILE_NUMBER = sd.PROFILE_NUMBER AND m2.PRES = sd.max_pres) as deep_temp
#       FROM surface_deep sd
#   )
#   SELECT surface_temp, deep_temp, (surface_temp - deep_temp) as temp_difference
#   FROM temps;

# Generate only the SQL query without any explanations. The query should be valid DuckDB SQL syntax."""

#     return system_prompt

# def generate_sql_query(prompt, system_prompt):
#     """Generate SQL query using Gemini"""
#     try:
#         print(f"\n🔍 Generating SQL for: {prompt}")
        
#         # Configure the model with safety settings
#         generation_config = {
#             "temperature": 0.1,  # Very low temperature for precise SQL generation
#             "top_p": 0.8,
#             "top_k": 40,
#             "max_output_tokens": 2048,
#         }

#         safety_settings = [
#             {
#                 "category": "HARM_CATEGORY_DANGEROUS",
#                 "threshold": "BLOCK_MEDIUM_AND_ABOVE",
#             },
#         ]

#         model = genai.GenerativeModel(
#             model_name="gemini-2.0-flash-exp",
#             generation_config=generation_config,
#             safety_settings=safety_settings
#         )
        
#         # Combine system prompt and user query
#         full_prompt = f"{system_prompt}\n\nUser Question: {prompt}\n\nGenerate a valid DuckDB SQL query to answer this question. Return only the SQL query without any explanations or markdown formatting:"
        
#         print("📤 Sending request to Gemini...")
        
#         try:
#             response = model.generate_content(full_prompt)
#             print(f"📥 Raw response received: {len(response.text) if response.text else 0} characters")
            
#             if response.text:
#                 print(f"📝 Raw response preview: {response.text[:200]}...")
                
#                 # Clean up the response - remove any markdown formatting if present
#                 sql = response.text.strip()
#                 if sql.startswith("```sql"):
#                     sql = sql[6:]
#                 elif sql.startswith("```"):
#                     sql = sql[3:]
#                 if sql.endswith("```"):
#                     sql = sql[:-3]
                
#                 # Remove any explanatory text before or after the SQL
#                 lines = sql.split('\n')
#                 sql_lines = []
#                 in_sql = False
                
#                 for line in lines:
#                     line = line.strip()
#                     if line.upper().startswith(('SELECT', 'WITH', 'CREATE', 'INSERT', 'UPDATE', 'DELETE')):
#                         in_sql = True
#                     if in_sql and line:
#                         sql_lines.append(line)
#                     if line.endswith(';') and in_sql:
#                         break
                
#                 if sql_lines:
#                     clean_sql = '\n'.join(sql_lines)
#                     print(f"✅ Generated clean SQL ({len(clean_sql)} chars)")
#                     return clean_sql
#                 else:
#                     print(f"⚠️ Using raw SQL as fallback")
#                     return sql.strip()
#             else:
#                 print("❌ Empty response from model")
#                 raise Exception("Empty response from model")
#         except Exception as e:
#             print(f"❌ Error generating content: {str(e)}")
#             # Return a simple test query as fallback
#             return "SELECT 'Error: Could not generate query' as error_message;"
            
#     except Exception as e:
#         print(f"❌ Error initializing model: {str(e)}")
#         return "SELECT 'Error: Model initialization failed' as error_message;"

# def execute_query(sql):
#     """Execute the generated SQL query on DuckDB"""
#     conn = duckdb.connect('argo_floats.db')
#     try:
#         print("\nExecuting SQL:")
#         print("-" * 50)
#         print(sql)
#         print("-" * 50)
        
#         # Execute the query
#         result = conn.execute(sql).fetchall()
        
#         # Get column names from the last query
#         col_names = [desc[0] for desc in conn.description]
#         return result, col_names
#     except Exception as e:
#         print(f"Error executing query: {str(e)}")
#         return [("Error executing query", str(e))], ["Error", "Details"]
#     finally:
#         conn.close()

# def generate_summary(question, results, headers):
#     """Generate human-readable summary of query results"""
#     if not results or any("Error" in str(h) for h in headers):
#         return "Unable to generate summary - query returned an error."
    
#     try:
#         # Create a simple summary based on the data
#         num_rows = len(results)
        
#         if num_rows == 0:
#             return "No data found matching your query criteria."
        
#         # Basic summary template
#         summary = f"Based on your question '{question}', I found {num_rows} result{'s' if num_rows != 1 else ''} in the Argo float database.\n\n"
        
#         # Add context based on question type
#         question_lower = question.lower()
        
#         if 'temperature' in question_lower and 'profile' in question_lower:
#             if headers and len(results[0]) >= 3:
#                 try:
#                     surface_temp = results[0][0] if results[0][0] is not None else "N/A"
#                     deep_temp = results[0][1] if results[0][1] is not None else "N/A" 
#                     temp_diff = results[0][2] if results[0][2] is not None else "N/A"
                    
#                     summary += f"Temperature Analysis:\n"
#                     summary += f"• Surface Temperature: {surface_temp}°C\n"
#                     summary += f"• Deep Temperature: {deep_temp}°C\n"
#                     summary += f"• Temperature Difference: {temp_diff}°C\n\n"
#                     summary += "This temperature difference reflects the thermal stratification of the ocean, where surface waters are typically warmer due to solar heating, while deeper waters remain cold."
#                 except:
#                     pass
        
#         elif 'float' in question_lower and ('most' in question_lower or 'coverage' in question_lower):
#             summary += "Float Coverage Analysis:\n"
#             if num_rows > 0:
#                 summary += f"Found {num_rows} floats with varying levels of vertical coverage. "
#                 summary += "Floats with better coverage provide more comprehensive oceanographic data from surface to deep waters."
        
#         elif 'quality' in question_lower:
#             summary += "Data Quality Assessment:\n"
#             summary += f"Analyzed quality control statistics across {num_rows} parameter types. "
#             summary += "Quality flags help ensure scientific analyses use only reliable measurements."
        
#         else:
#             # Generic summary
#             if headers:
#                 summary += f"Key findings include data on: {', '.join(headers[:3])}{'...' if len(headers) > 3 else ''}.\n\n"
#             summary += "The results show oceanographic measurements from Argo autonomous floats, which provide critical data for understanding ocean conditions and climate patterns."
        
#         return summary
        
#     except Exception as e:
#         return f"Summary generation failed: {str(e)}"
#     """Format query results for display"""
#     if not results:
#         return "No results found."
    
#     # If headers aren't provided, use generic column names
#     if not headers:
#         headers = [f"Column_{i}" for i in range(len(results[0]))]
    
#     # Limit results display to first 20 rows
#     display_results = results[:20]
#     if len(results) > 20:
#         truncated_msg = f"\n... ({len(results) - 20} more rows truncated for display)"
#     else:
#         truncated_msg = ""
    
#     # Calculate column widths (max 50 chars per column for readability)
#     widths = []
#     for i in range(len(headers)):
#         max_width = max(len(str(headers[i])), 
#                        max(len(str(row[i])[:50]) for row in display_results))
#         widths.append(min(max_width, 50))
    
#     # Create the header
#     header = " | ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
#     separator = "-" * len(header)
    
#     # Format each row
#     rows = [header, separator]
#     for row in display_results:
#         formatted_row = " | ".join(f"{str(val)[:50]:<{w}}" for val, w in zip(row, widths))
#         rows.append(formatted_row)
    
#     return "\n".join(rows) + truncated_msg

# def main():
#     """Main interactive function"""
#     print("🌊 Argo FloatBot Query Generator")
#     print("=" * 60)
    
#     # Load semantic model
#     try:
#         semantic_model = load_semantic_model()
#         print("✅ Semantic model loaded successfully")
#     except Exception as e:
#         print(f"❌ Error loading semantic model: {e}")
#         return
    
#     # Create system prompt
#     system_prompt = create_system_prompt(semantic_model)
    
#     # Example questions for reference
#     example_questions = [
#         "Show me all floats and their basic information",
#         "What is the average temperature and salinity for each float with good quality data?",
#         "Find the deepest measurement (highest pressure) for each float",
#         "Which floats have the most profiles?",
#         "Show temperature profiles for float '6903016'",
#         "How much has temperature fallen from surface to deepest point in profile 1 of float 6903016?",
#         "Calculate average temperature at different pressure ranges (0-100, 100-500, 500+ decibars)",
#         "Find profiles with the highest and lowest salinities",
#         "Show data quality statistics for all floats"
#     ]
    
#     print("\n📋 Example Questions:")
#     for i, question in enumerate(example_questions, 1):
#         print(f"{i}. {question}")
    
#     print("\n" + "=" * 60)
    
#     while True:
#         print("\n🤖 Enter your question about Argo float data (or 'quit' to exit):")
#         user_input = input("> ").strip()
        
#         if user_input.lower() in ['quit', 'exit', 'q']:
#             print("👋 Goodbye!")
#             break
            
#         if not user_input:
#             continue
        
#         # Check if user entered a number to select example question
#         if user_input.isdigit():
#             question_num = int(user_input)
#             if 1 <= question_num <= len(example_questions):
#                 user_question = example_questions[question_num - 1]
#                 print(f"\n📝 Selected Question: {user_question}")
#             else:
#                 print("❌ Invalid question number")
#                 continue
#         else:
#             user_question = user_input
        
#         # Generate SQL query
#         print("\n🔍 Generating SQL query...")
#         sql_query = generate_sql_query(user_question, system_prompt)
        
#         print("\n📊 Generated SQL Query:")
#         print("-" * 50)
#         print(sql_query)
#         print("-" * 50)
        
#         # Ask user if they want to execute
#         execute_choice = input("\n⚡ Execute this query? (y/n/edit): ").strip().lower()
        
#         if execute_choice == 'edit':
#             print("\n✏️ Enter your modified SQL query:")
#             modified_sql = input("> ")
#             if modified_sql.strip():
#                 sql_query = modified_sql
        
#         if execute_choice in ['y', 'yes', 'edit']:
#             # Execute query
#             print("\n⚙️ Executing query...")
#             results, headers = execute_query(sql_query)
            
#             print(f"\n📈 Results ({len(results)} rows):")
#             print(format_results(results, headers))
#         else:
#             print("⏭️ Query not executed")

# if __name__ == "__main__":
#     main()



