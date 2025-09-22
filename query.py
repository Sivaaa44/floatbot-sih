import google.generativeai as genai
import yaml
import duckdb
from dotenv import load_dotenv
import os
from transformers import pipeline
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()

# Configure Gemini API (for SQL only)
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in environment variables")
genai.configure(api_key=GOOGLE_API_KEY)

# Free LLM for report generation (local HuggingFace pipeline)
try:
    # Use a text generation model instead of summarization
    report_generator = pipeline("text-generation", model="microsoft/DialoGPT-medium", max_length=1024)
except Exception as e:
    print(f"Warning: Could not load report generation model: {e}")
    try:
        # Fallback to a smaller model
        report_generator = pipeline("text-generation", model="gpt2", max_length=1024)
    except Exception as e2:
        print(f"Warning: Could not load any text generation model: {e2}")
        report_generator = None

# ---------------------------
# Summary system prompt (new)
# ---------------------------
SUMMARY_SYSTEM_PROMPT = """
You are an expert oceanographic data analyst and scientific writer. 
Your job is to produce a concise, accurate, and data-driven summary report based ONLY on the numeric digest and sample rows provided below.
Do NOT invent or hallucinate any numbers, floats, or locations that are not present in the provided digest.

Output requirements:
- Produce the report in Markdown.
- Structure the report with these sections, in this order:
  1. Executive summary (1-3 short sentences that directly answer the user's question).
  2. Methods & filters used (explicitly list any QC filters, depth ranges and aggregation used; if none were provided, state 'as provided by SQL results').
  3. Key statistics (present numeric summaries for core variables: count, mean, median, std, min, max). Use a small inline table or bullet points.
  4. Spatial summary (lat/lon extent and any clear geographic gradients; mention north/south/east/west trends if present).
  5. Notable floats (list top 3 floats with highest value for the key variable and bottom 3 with lowest value; include float id, latitude, longitude, and the numeric value).
  6. Observed patterns / interpretation (what the numbers suggest scientifically, e.g., warmer near X, possible seasonal/latitudinal gradient).
  7. Outliers and caveats (highlight any extreme values or missing data).
  8. Recommendations & next steps for further analysis (e.g., look at seasonal cycles, compare to climatology).
  9. Confidence statement (briefly state how confident you are in the summary given the sample size and presence/absence of QC flags).

Formatting & style:
- Use the exact column names from the data digest when referring to variables.
- Where you quote a numeric value, use the same number (or rounded to a reasonable number of decimals) as provided.
- If a statistic cannot be computed (e.g., no numeric column present), say so clearly.
- Keep the executive summary short and actionable; the rest of the report can be 6–12 short paragraphs/bulleted lists.
- Do not hallucinate additional rows, floats, or metadata.
"""

# ---------------------------
# Helpers: build digest & fallback summary
# ---------------------------

# Oceanographic data analysis and reporting functions


def create_text_report(question: str, results, headers):
    """
    Create a comprehensive oceanographic analysis report.
    """
    # Initialize output lines
    lines = []
    
    # Extract key dimensions
    total_records = len(results)
    question_lower = question.lower()
    
    # Setup column indices for faster access
    col_indices = {col: idx for idx, col in enumerate(headers)}
    
    # 1. Basic Coverage Analysis
    if 'FLOAT_ID' in col_indices:
        float_id_idx = col_indices['FLOAT_ID']
        unique_floats = {row[float_id_idx] for row in results if row[float_id_idx] is not None}
        float_count = len(unique_floats)
        
        if float_count == 1:
            float_id = next(iter(unique_floats))
            lines.append(f"📊 Analyzing Float **{float_id}**")
        else:
            lines.append(f"📊 Analyzing **{float_count} float{'s' if float_count > 1 else ''}**")
    
    if 'PROFILE_NUMBER' in col_indices:
        profile_idx = col_indices['PROFILE_NUMBER']
        profile_pairs = {(row[float_id_idx], row[profile_idx]) 
                        for row in results 
                        if 'FLOAT_ID' in col_indices
                        and row[float_id_idx] is not None 
                        and row[profile_idx] is not None}
        profile_count = len(profile_pairs)
        
        if profile_count == 1:
            profile_num = next(iter(profile_pairs))[1]
            lines.append(f"🌊 Examining Profile **#{profile_num}**")
        else:
            lines.append(f"🌊 Covering **{profile_count} profile{'s' if profile_count > 1 else ''}**")
    
    lines.append(f"📈 Total measurements: **{total_records}**\n")
    
    # 2. Project and Platform Analysis
    if 'PROJECT_NAME' in col_indices:
        proj_idx = col_indices['PROJECT_NAME']
        projects = list({row[proj_idx] for row in results if row[proj_idx] is not None})
        
        if projects:
            if len(projects) == 1:
                lines.append(f"🏢 Project: **{projects[0]}**")
            else:
                main_projects = projects[:3]
                lines.append(f"🏢 Projects: **{len(projects)}** total ({', '.join(main_projects)})")
    
    # 3. Temperature Profile Analysis
    if 'TEMP' in col_indices and 'PRES' in col_indices:
        temp_idx = col_indices['TEMP']
        pres_idx = col_indices['PRES']
        
        # Group by pressure levels
        temp_data = []
        for row in results:
            if row[temp_idx] is not None and row[pres_idx] is not None:
                try:
                    temp_data.append((float(row[pres_idx]), float(row[temp_idx])))
                except (ValueError, TypeError):
                    continue
        
        if temp_data:
            temp_data.sort()  # Sort by pressure (depth)
            
            # Basic statistics
            surface_data = [(p, t) for p, t in temp_data if p <= 50]
            thermocline_data = [(p, t) for p, t in temp_data if 50 <= p <= 300]
            deep_data = [(p, t) for p, t in temp_data if p > 1000]
            
            lines.append("\n### 🌡️ Temperature Profile")
            
            # Surface layer
            if surface_data:
                surface_temps = [t for _, t in surface_data]
                lines.append(f"**Surface Layer** (0-50m):")
                lines.append(f"- Range: {min(surface_temps):.2f}°C to {max(surface_temps):.2f}°C")
                lines.append(f"- Average: {sum(surface_temps)/len(surface_temps):.2f}°C")
            
            # Thermocline
            if len(thermocline_data) >= 2:
                temp_change = thermocline_data[-1][1] - thermocline_data[0][1]
                depth_change = thermocline_data[-1][0] - thermocline_data[0][0]
                gradient = temp_change / depth_change
                lines.append(f"\n**Thermocline** (50-300m):")
                lines.append(f"- Temperature gradient: {gradient:.3f}°C/m")
                lines.append(f"- {'Strong' if abs(gradient) > 0.01 else 'Weak'} thermal stratification")
            
            # Deep water
            if deep_data:
                deep_temps = [t for _, t in deep_data]
                lines.append(f"\n**Deep Layer** (>1000m):")
                lines.append(f"- Range: {min(deep_temps):.2f}°C to {max(deep_temps):.2f}°C")
    
    # 4. Quality Control
    qc_cols = [col for col in headers if col.endswith('_QC')]
    if qc_cols:
        lines.append("\n### 🎯 Data Quality")
        for col in qc_cols:
            qc_idx = col_indices[col]
            good_qc = sum(1 for row in results if row[qc_idx] in ['1', '2'])
            qc_pct = (good_qc / total_records) * 100 if total_records > 0 else 0
            param = col.replace('_QC', '')
            lines.append(f"- {param}: **{qc_pct:.1f}%** good quality")
    
    # 5. Context-based Summary
    lines.append("\n### 💡 Analysis Notes")
    if total_records == 0:
        lines.append("⚠️ No data found matching the query criteria.")
    elif total_records < 10:
        lines.append("⚠️ Limited data points available - consider expanding search criteria.")
    elif 'FLOAT_ID' in col_indices and 'PROFILE_NUMBER' in col_indices:
        if float_count == 1 and profile_count == 1:
            lines.append("📍 Detailed single profile analysis")
            if 'PRES' in col_indices:
                max_depth = max(float(row[col_indices['PRES']]) 
                              for row in results 
                              if row[col_indices['PRES']] is not None)
                lines.append(f"📊 Profile depth coverage: **{max_depth:.0f}** dbar")
    
    # 6. Final Data Quality Statement
    if qc_cols:
        all_good = all(row[col_indices[col]] in ['1', '2'] 
                      for row in results 
                      for col in qc_cols 
                      if col in col_indices)
        if all_good:
            lines.append("\n✅ All measurements pass quality control checks.")
        else:
            lines.append("\n⚠️ Some measurements may have quality issues - check QC flags.")
        lines.append("")
        
        # Analyze projects
        if 'PROJECT_NAME' in headers:
            project_index = headers.index('PROJECT_NAME')
            projects = list(set([row[project_index] for row in results if row[project_index] is not None]))
            
            if len(projects) == 1:
                lines.append(f"All floats belong to the **{projects[0]}** project.")
            elif len(projects) <= 5:
                lines.append(f"These floats are part of **{len(projects)} different projects**: {', '.join(projects)}.")
            else:
                main_projects = projects[:3]
                lines.append(f"The floats span **{len(projects)} different projects**, with the main ones being {', '.join(main_projects)} and {len(projects)-3} others.")
        
        # Latest and earliest deployments
        if 'LAUNCH_DATE' in headers:
            date_index = headers.index('LAUNCH_DATE')
            float_id_index = headers.index('FLOAT_ID') if 'FLOAT_ID' in headers else 0
            
            dates_with_floats = [(row[date_index], row[float_id_index]) for row in results if row[date_index] is not None]
            if dates_with_floats:
                dates_with_floats.sort()
                earliest_date, earliest_float = dates_with_floats[0]
                latest_date, latest_float = dates_with_floats[-1]
                
                lines.append("")
                lines.append(f"The **most recent float** deployed is **{latest_float}** on {format_date(latest_date)}.")
                lines.append(f"The **earliest float** in this dataset is **{earliest_float}**, deployed on {format_date(earliest_date)}.")
        
        # Geographic coverage
        if 'LAUNCH_LATITUDE' in headers and 'LAUNCH_LONGITUDE' in headers:
            lat_index = headers.index('LAUNCH_LATITUDE')
            lon_index = headers.index('LAUNCH_LONGITUDE')
            
            lats = [float(row[lat_index]) for row in results if row[lat_index] is not None and is_numeric(row[lat_index])]
            lons = [float(row[lon_index]) for row in results if row[lon_index] is not None and is_numeric(row[lon_index])]
            
            if lats and lons:
                lines.append("")
                lat_range = max(lats) - min(lats)
                lon_range = max(lons) - min(lons)
                
                if lat_range > 50 or lon_range > 50:
                    coverage = "global coverage"
                elif lat_range > 20 or lon_range > 20:
                    coverage = "regional coverage"
                else:
                    coverage = "local coverage"
                
                avg_lat = sum(lats) / len(lats)
                hemisphere = "Northern Hemisphere" if avg_lat > 0 else "Southern Hemisphere"
                
                lines.append(f"The floats provide **{coverage}** with deployments ranging from **{min(lats):.2f}° to {max(lats):.2f}°** latitude and **{min(lons):.2f}° to {max(lons):.2f}°** longitude, primarily in the **{hemisphere}**.")
        
        # Platform types
        if 'PLATFORM_TYPE' in headers:
            platform_index = headers.index('PLATFORM_TYPE')
            platforms = [row[platform_index] for row in results if row[platform_index] is not None]
            platform_counts = {}
            for platform in platforms:
                platform_counts[platform] = platform_counts.get(platform, 0) + 1
            
            if len(platform_counts) == 1:
                platform_name = list(platform_counts.keys())[0]
                lines.append("")
                lines.append(f"All floats use the **{platform_name}** platform technology.")
            else:
                lines.append("")
                most_common = max(platform_counts, key=platform_counts.get)
                lines.append(f"The most common platform type is **{most_common}** ({platform_counts[most_common]} floats), with other types including {', '.join([k for k in platform_counts.keys() if k != most_common])}.")
    
    elif 'temperature' in question_lower and ('profile' in question_lower):
        # Temperature profile analysis
        lines.append("**Temperature Profile Analysis:**")
        lines.append("")
        
        if 'TEMP' in headers and 'PRES' in headers:
            temp_index = headers.index('TEMP')
            pres_index = headers.index('PRES')
            
            # Group by pressure levels
            temp_data = []
            for row in results:
                if row[temp_index] is not None and row[pres_index] is not None:
                    try:
                        temp_data.append((float(row[pres_index]), float(row[temp_index])))
                    except (ValueError, TypeError):
                        continue
            
            if temp_data:
                temp_data.sort()  # Sort by pressure (depth)
                
                surface_temp = temp_data[0][1] if temp_data else None
                deep_temp = temp_data[-1][1] if temp_data else None
                surface_depth = temp_data[0][0] if temp_data else None
                deep_depth = temp_data[-1][0] if temp_data else None
                
                if surface_temp is not None and deep_temp is not None:
                    temp_drop = surface_temp - deep_temp
                    
                    lines.append(f"At the **surface level** (~{surface_depth:.0f} dbar), the temperature was **{surface_temp:.1f}°C**.")
                    lines.append(f"As the float descended, the temperature **gradually decreased** due to the natural thermal stratification of the ocean.")
                    lines.append(f"At the **deepest point** ({deep_depth:.0f} dbar), the temperature dropped to **{deep_temp:.1f}°C**.")
                    lines.append(f"This represents a **total temperature decrease of {temp_drop:.1f}°C** over {deep_depth-surface_depth:.0f} decibars of depth.")
                    
                    # Analyze thermocline
                    thermocline_data = [(p, t) for p, t in temp_data if 50 <= p <= 300]
                    if len(thermocline_data) > 2:
                        thermocline_start = thermocline_data[0]
                        thermocline_end = thermocline_data[-1]
                        thermocline_gradient = (thermocline_end[1] - thermocline_start[1]) / (thermocline_end[0] - thermocline_start[0])
                        
                        lines.append("")
                        lines.append(f"The **thermocline region** (50-300 dbar) shows a temperature gradient of **{thermocline_gradient:.3f}°C per dbar**, indicating {'strong' if abs(thermocline_gradient) > 0.01 else 'weak'} thermal stratification.")
    
    else:
        # General data description
        lines.append(f"The query returned **{total_records} records** from the oceanographic database.")
        
        # Try to identify key variables and provide insights
        if 'FLOAT_ID' in headers:
            float_index = headers.index('FLOAT_ID')
            unique_floats = len(set([row[float_index] for row in results if row[float_index] is not None]))
            lines.append(f"This data represents **{unique_floats} different Argo floats**.")
        
        # Look for measurement data
        measurement_vars = [h for h in headers if h in ['TEMP', 'PSAL', 'PRES', 'DOXY']]
        if measurement_vars:
            lines.append("")
            lines.append("The dataset includes oceanographic measurements for:")
            var_descriptions = {
                'TEMP': 'sea water temperature',
                'PSAL': 'practical salinity', 
                'PRES': 'pressure (depth)',
                'DOXY': 'dissolved oxygen'
            }
            for var in measurement_vars:
                lines.append(f"- **{var_descriptions.get(var, var)}**")
    
    # Add dataset size context
    if total_records > 0:
        lines.append("\n### 📈 Dataset Coverage")
        if total_records > 100:
            lines.append("This substantial dataset is suitable for comprehensive oceanographic analysis.")
        elif total_records > 20:
            lines.append("This moderate-sized dataset provides good coverage for profile analysis.")
        else:
            lines.append("This focused dataset provides detailed insight into specific measurements.")
    
    return "\n".join(lines)

def format_date(date_str):
    """Convert YYYYMMDDHHMMSS to readable format"""
    try:
        if len(str(date_str)) >= 8:
            date_part = str(date_str)[:8]
            year = date_part[:4]
            month = date_part[4:6]
            day = date_part[6:8]
            return f"{day}/{month}/{year}"
    except:
        pass
    return str(date_str)

def is_numeric(value):
    """Check if value can be converted to float"""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False
# Removed the complex fallback_summary function since we're using simple text reports


def load_semantic_model():
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


def generate_sql_query(prompt, system_prompt):
    """Generate SQL query using Gemini Flash 2.5"""
    model = genai.GenerativeModel("gemini-2.0-flash-exp")
    full_prompt = f"{system_prompt}\n\nUser Question: {prompt}\n\nGenerate a valid DuckDB SQL query. Return ONLY SQL."

    try:
        response = model.generate_content(full_prompt)
        sql = response.text.strip()
        # cleanup markdown ```sql fences if present
        if sql.startswith("```"):
            sql = sql.split("```")[1].replace("sql", "", 1).strip()
        return sql
    except Exception as e:
        print(f"Error from Gemini: {e}")
        return "SELECT 'Error generating SQL' AS error;"


def execute_query(sql):
    """Run SQL on local DuckDB database"""
    conn = duckdb.connect('argo_floats.db')
    try:
        result = conn.execute(sql).fetchall()
        headers = [desc[0] for desc in conn.description]
        return result, headers
    except Exception as e:
        return [("Error", str(e))], ["Error", "Details"]
    finally:
        conn.close()


def analyze_results_with_free_llm(question: str, results, headers):
    """
    Create a human-readable oceanographic report from SQL results with proper context.
    """
    if not results or not headers or ("Error" in headers[0]):
        return "## Analysis Results\n\nNo valid results to analyze."
    
    # Get column indices for key metrics
    col_indices = {col: idx for idx, col in enumerate(headers)}
    
    # Extract key dimensions
    total_records = len(results)
    float_count = len({row[col_indices['FLOAT_ID']] 
                      for row in results 
                      if 'FLOAT_ID' in col_indices 
                      and row[col_indices['FLOAT_ID']] is not None})
    
    profile_count = len({(row[col_indices['FLOAT_ID']], row[col_indices['PROFILE_NUMBER']]) 
                        for row in results 
                        if 'FLOAT_ID' in col_indices and 'PROFILE_NUMBER' in col_indices 
                        and row[col_indices['FLOAT_ID']] is not None 
                        and row[col_indices['PROFILE_NUMBER']] is not None})
    
    # Create proper analysis
    report = create_text_report(question, results, headers)
    
    # Add analysis confidence based on data quality
    if float_count == 1 and profile_count == 1:
        qc_cols = [col for col in headers if col.endswith('_QC')]
        good_data = all(any(row[col_indices[col]] in ['1', '2'] for col in qc_cols)
                       for row in results if all(col in col_indices for col in qc_cols))
        
        if good_data:
            report += "\n\n💡 **Data Quality Note:** All measurements pass quality control checks."
        else:
            report += "\n\n⚠️ **Data Quality Note:** Some measurements may have quality issues. Check QC flags."
    
    return report


def orchestrator(user_question):
    """End-to-end: SQL generation -> execution -> summarization"""
    try:
        semantic_model = load_semantic_model()
        system_prompt = create_system_prompt(semantic_model)

        sql = generate_sql_query(user_question, system_prompt)
        results, headers = execute_query(sql)
        summary = analyze_results_with_free_llm(user_question, results, headers)

        return {
            "question": user_question,
            "sql": sql,
            "results": results,
            "headers": headers,
            "analysis": summary,
            "success": not (headers and "Error" in headers[0])
        }
    except Exception as e:
        return {
            "question": user_question,
            "sql": f"-- Error: {str(e)}",
            "results": [],
            "headers": [],
            "analysis": f"Error in processing: {str(e)}",
            "success": False
        }