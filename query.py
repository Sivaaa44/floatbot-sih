import google.generativeai as genai
import yaml
import duckdb

# Configure Gemini API
GOOGLE_API_KEY = ""  # Replace with your actual API key
genai.configure(api_key=GOOGLE_API_KEY)

def load_semantic_model():
    """Load the semantic model from YAML file"""
    with open('argo_semantic_model.yaml', 'r') as file:
        return yaml.safe_load(file)

def create_system_prompt(semantic_model):
    """Create a system prompt that explains the database structure and how to generate SQL queries"""
    
    # Build table descriptions from semantic model
    table_descriptions = []
    for table_name, table_info in semantic_model['tables'].items():
        desc = table_info['description']
        columns = [f"- {col_name}: {col_info['type']} - {col_info['description']}" 
                  for col_name, col_info in table_info['columns'].items()]
        
        table_desc = f"\n{table_name.upper()} Table:\n"
        table_desc += f"Description: {desc}\n"
        table_desc += "Columns:\n" + "\n".join(columns)
        
        # Add primary key info
        if 'primary_key' in table_info:
            if isinstance(table_info['primary_key'], list):
                table_desc += f"\nPrimary Key: ({', '.join(table_info['primary_key'])})"
            else:
                table_desc += f"\nPrimary Key: {table_info['primary_key']}"
        
        # Add foreign key info
        if 'foreign_keys' in table_info:
            table_desc += "\nForeign Keys:\n"
            for fk in table_info['foreign_keys']:
                fk_cols = ', '.join(fk['columns']) if isinstance(fk['columns'], list) else fk['columns']
                table_desc += f"- {fk_cols} → {fk['references']}\n"
        
        table_descriptions.append(table_desc)

    # Add relationship information
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

Important Guidelines:
1. Table names are: float, profiles, measurements (lowercase)
2. Column names are UPPERCASE (FLOAT_ID, PROFILE_NUMBER, PRES, TEMP, PSAL, etc.)
3. Always use proper table joins based on relationships:
   - float.FLOAT_ID = profiles.FLOAT_ID
   - profiles.FLOAT_ID = measurements.FLOAT_ID AND profiles.PROFILE_NUMBER = measurements.PROFILE_NUMBER
4. For complete float data: JOIN all three tables
5. For specific profile data: JOIN float → profiles → measurements
6. Use appropriate aggregations (AVG, COUNT, SUM, MIN, MAX) when summarizing data
7. Include WHERE clauses to filter out NULL values or invalid measurements where appropriate
8. Use clear column aliasing for better readability
9. Handle quality control (QC) flags appropriately:
   - Good measurements: PRES_QC = '1', TEMP_QC = '1', PSAL_QC = '1'
   - Good profiles: POSITION_QC = '1', PROFILE_PRES_QC = '1', etc.
10. Format the SQL query with proper indentation and line breaks
11. For time-based queries, use JULD column (timestamp format)
12. Use PRES column for depth/pressure measurements
13. Use TEMP, PSAL columns for temperature and salinity
14. Consider using adjusted values (*_ADJUSTED columns) for scientific accuracy

Example Query Patterns:
- All data for a float: SELECT * FROM float f JOIN profiles p ON f.FLOAT_ID = p.FLOAT_ID JOIN measurements m ON p.FLOAT_ID = m.FLOAT_ID AND p.PROFILE_NUMBER = m.PROFILE_NUMBER WHERE f.FLOAT_ID = 'floatid'
- Profile summary: SELECT f.FLOAT_ID, COUNT(DISTINCT p.PROFILE_NUMBER) as profiles FROM float f JOIN profiles p ON f.FLOAT_ID = p.FLOAT_ID GROUP BY f.FLOAT_ID

Generate only the SQL query without any explanations. The query should be valid DuckDB SQL syntax."""

    return system_prompt

def generate_sql_query(prompt, system_prompt):
    """Generate SQL query using Gemini"""
    try:
        # Configure the model with safety settings
        generation_config = {
            "temperature": 0.1,  # Very low temperature for precise SQL generation
            "top_p": 0.8,
            "top_k": 40,
            "max_output_tokens": 2048,
        }

        safety_settings = [
            {
                "category": "HARM_CATEGORY_DANGEROUS",
                "threshold": "BLOCK_MEDIUM_AND_ABOVE",
            },
        ]

        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash-exp",  # Updated model name
            generation_config=generation_config,
            safety_settings=safety_settings
        )
        
        # Combine system prompt and user query
        full_prompt = f"{system_prompt}\n\nUser Question: {prompt}\n\nGenerate a valid DuckDB SQL query to answer this question. Return only the SQL query without any explanations or markdown formatting:"
        
        try:
            response = model.generate_content(full_prompt)
            if response.text:
                # Clean up the response - remove any markdown formatting if present
                sql = response.text.strip()
                if sql.startswith("```sql"):
                    sql = sql[6:]
                elif sql.startswith("```"):
                    sql = sql[3:]
                if sql.endswith("```"):
                    sql = sql[:-3]
                
                # Remove any explanatory text before or after the SQL
                lines = sql.split('\n')
                sql_lines = []
                in_sql = False
                
                for line in lines:
                    line = line.strip()
                    if line.upper().startswith(('SELECT', 'WITH', 'CREATE', 'INSERT', 'UPDATE', 'DELETE')):
                        in_sql = True
                    if in_sql and line:
                        sql_lines.append(line)
                    if line.endswith(';') and in_sql:
                        break
                
                if sql_lines:
                    return '\n'.join(sql_lines)
                else:
                    return sql.strip()
            else:
                raise Exception("Empty response from model")
        except Exception as e:
            print(f"Error generating content: {str(e)}")
            # Return a simple test query as fallback
            return "SELECT 'Error: Could not generate query' as error_message;"
            
    except Exception as e:
        print(f"Error initializing model: {str(e)}")
        return "SELECT 'Error: Model initialization failed' as error_message;"

def execute_query(sql):
    """Execute the generated SQL query on DuckDB"""
    conn = duckdb.connect('argo_floats.db')
    try:
        print("\nExecuting SQL:")
        print("-" * 50)
        print(sql)
        print("-" * 50)
        
        # Execute the query
        result = conn.execute(sql).fetchall()
        
        # Get column names from the last query
        col_names = [desc[0] for desc in conn.description]
        return result, col_names
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return [("Error executing query", str(e))], ["Error", "Details"]
    finally:
        conn.close()

def format_results(results, headers=None):
    """Format query results for display"""
    if not results:
        return "No results found."
    
    # If headers aren't provided, use generic column names
    if not headers:
        headers = [f"Column_{i}" for i in range(len(results[0]))]
    
    # Limit results display to first 20 rows
    display_results = results[:20]
    if len(results) > 20:
        truncated_msg = f"\n... ({len(results) - 20} more rows truncated for display)"
    else:
        truncated_msg = ""
    
    # Calculate column widths (max 50 chars per column for readability)
    widths = []
    for i in range(len(headers)):
        max_width = max(len(str(headers[i])), 
                       max(len(str(row[i])[:50]) for row in display_results))
        widths.append(min(max_width, 50))
    
    # Create the header
    header = " | ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
    separator = "-" * len(header)
    
    # Format each row
    rows = [header, separator]
    for row in display_results:
        formatted_row = " | ".join(f"{str(val)[:50]:<{w}}" for val, w in zip(row, widths))
        rows.append(formatted_row)
    
    return "\n".join(rows) + truncated_msg

def main():
    """Main interactive function"""
    print("🌊 Argo FloatBot Query Generator")
    print("=" * 60)
    
    # Load semantic model
    try:
        semantic_model = load_semantic_model()
        print("✅ Semantic model loaded successfully")
    except Exception as e:
        print(f"❌ Error loading semantic model: {e}")
        return
    
    # Create system prompt
    system_prompt = create_system_prompt(semantic_model)
    
    # Example questions for reference
    example_questions = [
        "Show me all floats and their basic information",
        "What is the average temperature and salinity for each float with good quality data?",
        "Find the deepest measurement (highest pressure) for each float",
        "Which floats have the most profiles?",
        "Show temperature profiles for float '6903091'",
        "Calculate average temperature at different pressure ranges (0-100, 100-500, 500+ decibars)",
        "Find profiles with the highest and lowest salinities",
        "Show data quality statistics for all floats"
    ]
    
    print("\n📋 Example Questions:")
    for i, question in enumerate(example_questions, 1):
        print(f"{i}. {question}")
    
    print("\n" + "=" * 60)
    
    while True:
        print("\n🤖 Enter your question about Argo float data (or 'quit' to exit):")
        user_input = input("> ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("👋 Goodbye!")
            break
            
        if not user_input:
            continue
        
        # Check if user entered a number to select example question
        if user_input.isdigit():
            question_num = int(user_input)
            if 1 <= question_num <= len(example_questions):
                user_question = example_questions[question_num - 1]
                print(f"\n📝 Selected Question: {user_question}")
            else:
                print("❌ Invalid question number")
                continue
        else:
            user_question = user_input
        
        # Generate SQL query
        print("\n🔍 Generating SQL query...")
        sql_query = generate_sql_query(user_question, system_prompt)
        
        print("\n📊 Generated SQL Query:")
        print("-" * 50)
        print(sql_query)
        print("-" * 50)
        
        # Ask user if they want to execute
        execute_choice = input("\n⚡ Execute this query? (y/n/edit): ").strip().lower()
        
        if execute_choice == 'edit':
            print("\n✏️ Enter your modified SQL query:")
            modified_sql = input("> ")
            if modified_sql.strip():
                sql_query = modified_sql
        
        if execute_choice in ['y', 'yes', 'edit']:
            # Execute query
            print("\n⚙️ Executing query...")
            results, headers = execute_query(sql_query)
            
            print(f"\n📈 Results ({len(results)} rows):")
            print(format_results(results, headers))
        else:
            print("⏭️ Query not executed")

if __name__ == "__main__":
    main()