
# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import numpy as np
# from datetime import datetime
# from query import load_semantic_model, create_system_prompt, generate_sql_query, execute_query, generate_summary

# def analyze_oceanographic_question(question):
#     """Analyze question to determine oceanographic context"""
#     question_lower = question.lower()
    
#     contexts = {
#         'water_mass': ['water mass', 't-s diagram', 'temperature salinity', 'potential temperature'],
#         'vertical_structure': ['thermocline', 'temperature gradient', 'stratification', 'mixed layer'],
#         'spatial_analysis': ['geographic', 'spatial', 'location', 'regional', 'map'],
#         'temporal_analysis': ['time series', 'seasonal', 'temporal', 'over time', 'trend'],
#         'quality_control': ['quality', 'qc', 'data quality', 'flags'],
#         'profile_analysis': ['profile', 'depth', 'vertical', 'pressure', 'surface', 'deepest', 'temperature difference']
#     }
    
#     detected_contexts = []
#     for context, keywords in contexts.items():
#         if any(keyword in question_lower for keyword in keywords):
#             detected_contexts.append(context)
    
#     return detected_contexts if detected_contexts else ['general']

# def main():
#     st.set_page_config(page_title="Argo Oceanographic Analyst", page_icon="🌊", layout="wide")
    
#     st.title("🌊 Argo Oceanographic Analyst")
#     st.markdown("*Professional oceanographic data analysis*")
    
#     # Load components
#     try:
#         semantic_model = load_semantic_model()
#         system_prompt = create_system_prompt(semantic_model)
#     except Exception as e:
#         st.error(f"System initialization error: {e}")
#         return
    
#     # Sidebar with oceanographic context
#     with st.sidebar:
#         st.header("🔬 Oceanographic Context")
        
#         with st.expander("📚 Quality Control Guide"):
#             st.text("""
# QC Flags:
# • 1: Excellent data
# • 2: Good data  
# • 3-4: Poor/bad data
# • 9: Missing data

# Use flags 1 & 2 for analysis
#             """)
        
#         with st.expander("🌊 Key Depth Ranges"):
#             st.text("""
# • Surface: 0-50 dbar
# • Thermocline: 10-300 dbar  
# • Intermediate: 300-1000 dbar
# • Deep: 1000+ dbar
#             """)
    
#     # Initialize chat history
#     if 'chat_history' not in st.session_state:
#         st.session_state.chat_history = []
    
#     # Example oceanographic questions
#     with st.expander("🧭 Example Oceanographic Questions"):
#         examples = [
#             "Show me all floats and their basic information",
#             "What is the average temperature and salinity for each float with good quality data?",
#             "Show temperature profiles for float '6903016'",
#             "How much has temperature fallen from surface to deepest point in profile 1 of float 6903016?",
#             "Find the deepest measurement (highest pressure) for each float",
#             "Which floats have the most profiles?",
#             "Calculate average temperature at different pressure ranges (0-100, 100-500, 500+ decibars)",
#             "Show data quality statistics for temperature and salinity measurements"
#         ]
        
#         cols = st.columns(2)
#         for i, example in enumerate(examples):
#             with cols[i % 2]:
#                 if st.button(f"📋 {example}", key=f"ex_{i}"):
#                     st.session_state.user_input = example
    
#     # Chat interface
#     if user_input := st.chat_input("Ask an oceanographic question..."):
#         # Add user message
#         st.session_state.chat_history.append(("user", user_input))
        
#         # Analyze question context
#         contexts = analyze_oceanographic_question(user_input)
        
#         # Generate SQL query
#         with st.spinner("🔍 Generating SQL query..."):
#             sql_query = generate_sql_query(user_input, system_prompt)
        
#         # Store response
#         st.session_state.chat_history.append(("assistant", {
#             'sql': sql_query,
#             'contexts': contexts,
#             'question': user_input
#         }))
    
#     # Display conversation
#     for i, (msg_type, content) in enumerate(st.session_state.chat_history):
#         if msg_type == "user":
#             st.chat_message("user").write(content)
#         else:
#             with st.chat_message("assistant"):
#                 sql_query = content['sql']
#                 contexts = content['contexts']
                
#                 # Display oceanographic context tags
#                 if contexts:
#                     context_tags = " ".join([f"`{ctx}`" for ctx in contexts])
#                     st.markdown(f"**Oceanographic Context:** {context_tags}")
                
#                 # Display SQL query
#                 st.markdown("### 📝 Generated Query")
#                 st.code(sql_query, language="sql")
                
#                 # Add query edit option with unique key
#                 edit_mode = st.checkbox("✏️ Edit Query", key=f"edit_{i}")
#                 if edit_mode:
#                     sql_query = st.text_area("Edit SQL Query", value=sql_query, height=200, 
#                                            key=f"sql_edit_{i}")
                
#                 # Execute query automatically
#                 with st.spinner("🔍 Executing query..."):
#                     try:
#                         results, headers = execute_query(sql_query)
                        
#                         if results and not any("Error" in str(h) for h in headers):
#                             df = pd.DataFrame(results, columns=headers)
                            
#                             # Generate human-readable summary
#                             summary = generate_summary(content['question'], results, headers)
                            
#                             st.markdown("### 💡 Analysis Summary")
#                             st.markdown(summary)
                            
#                             st.markdown("### 📊 Detailed Results")
                            
#                             # Show results
#                             st.dataframe(df.head(50))
                            
#                             if len(df) > 50:
#                                 st.info(f"Showing 50 of {len(df)} rows")
                            
#                             # Add visualization for numeric data
#                             numeric_cols = df.select_dtypes(include=[np.number]).columns
#                             if len(numeric_cols) >= 2:
#                                 st.markdown("### 📈 Visualization")
#                                 col1, col2 = st.columns(2)
#                                 with col1:
#                                     x_col = st.selectbox("X-axis", numeric_cols, key=f"x_{i}")
#                                 with col2:
#                                     y_col = st.selectbox("Y-axis", numeric_cols, key=f"y_{i}")
                                
#                                 viz_type = st.selectbox("Chart Type", ["Scatter", "Line"], key=f"viz_{i}")
                                
#                                 if viz_type == "Scatter":
#                                     fig = px.scatter(df, x=x_col, y=y_col, 
#                                                    title=f"{y_col} vs {x_col}")
#                                 else:
#                                     fig = px.line(df, x=x_col, y=y_col, 
#                                                 title=f"{y_col} vs {x_col}")
                                
#                                 st.plotly_chart(fig, use_container_width=True)
                            
#                             # Download option
#                             csv = df.to_csv(index=False)
#                             st.download_button(
#                                 "💾 Download CSV",
#                                 csv,
#                                 f"argo_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
#                                 "text/csv",
#                                 key=f"download_{i}"
#                             )
#                         else:
#                             st.error("Query execution failed")
#                             if results and len(results[0]) > 1:
#                                 st.error(f"Error: {results[0][1]}")
                            
#                             # Provide helpful tips
#                             if results and len(results[0]) > 1:
#                                 error_msg = str(results[0][1]).lower()
#                                 if "syntax error" in error_msg:
#                                     st.info("💡 Tip: Check SQL syntax - missing semicolons, parentheses, or incorrect formatting")
#                                 elif "table not found" in error_msg:
#                                     st.info("💡 Tip: Table names should be lowercase: 'float', 'profiles', 'measurements'")
#                                 elif "column not found" in error_msg:
#                                     st.info("💡 Tip: Column names should be UPPERCASE: FLOAT_ID, TEMP, PRES, etc.")
#                     except Exception as e:
#                         st.error(f"Execution error: {str(e)}")
#                         st.info("Check console logs for detailed error information")
    
#     # Clear chat button
#     if st.sidebar.button("🗑️ Clear Chat"):
#         st.session_state.chat_history = []
#         st.rerun()

# if __name__ == "__main__":
#     main()

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime
from query import orchestrator


def analyze_oceanographic_question(question):
    """Analyze question to determine oceanographic context"""
    if not question:
        return ['general']
        
    question_lower = question.lower()
    
    contexts = {
        'water_mass': ['water mass', 't-s diagram', 'temperature salinity', 'potential temperature'],
        'vertical_structure': ['thermocline', 'temperature gradient', 'stratification', 'mixed layer'],
        'spatial_analysis': ['geographic', 'spatial', 'location', 'regional', 'map', 'across', 'vary'],
        'temporal_analysis': ['time series', 'seasonal', 'temporal', 'over time', 'trend'],
        'quality_control': ['quality', 'qc', 'data quality', 'flags'],
        'profile_analysis': ['profile', 'depth', 'vertical', 'pressure', 'surface', 'deepest', 'temperature difference']
    }
    
    detected_contexts = []
    for context, keywords in contexts.items():
        if any(keyword in question_lower for keyword in keywords):
            detected_contexts.append(context)
    
    return detected_contexts if detected_contexts else ['general']


def main():
    st.set_page_config(page_title="Argo Oceanographic Analyst", page_icon="🌊", layout="wide")
    
    st.title("🌊 Argo Oceanographic Analyst")
    st.markdown("*Professional oceanographic data analysis powered by AI*")
    
    # Sidebar
    with st.sidebar:
        st.header("🔬 System Info")
        st.info("SQL Generation: Gemini Flash 2.5")
        st.info("Result Analysis: Free Text Model")
        
        with st.expander("📚 Quality Control Guide"):
            st.text("""
QC Flags:
• 1: Excellent data
• 2: Good data  
• 3-4: Poor/bad data
• 9: Missing data

Use flags 1 & 2 for analysis
            """)
        
        with st.expander("🌊 Key Depth Ranges"):
            st.text("""
• Surface: 0-50 dbar
• Thermocline: 10-300 dbar  
• Intermediate: 300-1000 dbar
• Deep: 1000+ dbar
            """)
    
    # Chat history
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Example questions
    with st.expander("🧭 Example Oceanographic Questions"):
        examples = [
            "How does surface temperature vary across different float locations?",
            "Show me all floats and their basic information",
            "What is the average temperature and salinity for each float with good quality data?",
            "Show temperature profiles for float '6903016'",
            "How much has temperature fallen from surface to deepest point in profile 1 of float 6903016?",
            "Find the deepest measurement (highest pressure) for each float",
            "Which floats have the most profiles?",
            "Show data quality statistics for temperature and salinity measurements"
        ]
        
        cols = st.columns(2)
        for i, example in enumerate(examples):
            with cols[i % 2]:
                if st.button(f"📋 {example}", key=f"ex_{i}"):
                    st.session_state.user_input = example
    
    # Chat input
    if user_input := st.chat_input("Ask an oceanographic question..."):
        st.session_state.chat_history.append(("user", user_input))
        
        with st.spinner("🔄 Processing your oceanographic query..."):
            result = orchestrator(user_input)
        
        st.session_state.chat_history.append(("assistant", result))
    
    # Display conversation
    for i, (msg_type, content) in enumerate(st.session_state.chat_history):
        if msg_type == "user":
            st.chat_message("user").write(content)
        else:
            with st.chat_message("assistant"):
                sql_query = content['sql']
                results = content['results']
                headers = content['headers']
                analysis = content['analysis']
                success = content['success']
                question = content.get('question', '')
                
                # Oceanographic context tags
                contexts = analyze_oceanographic_question(question)
                if contexts:
                    context_tags = " ".join([f"`{ctx}`" for ctx in contexts])
                    st.markdown(f"**Oceanographic Context:** {context_tags}")
                
                # Analysis
                st.markdown("### 💡 Oceanographic Analysis")
                st.markdown(analysis)
                
                # SQL query
                with st.expander("📝 View Generated SQL Query"):
                    st.code(sql_query, language="sql")
                    
                    edit_mode = st.checkbox("✏️ Edit Query", key=f"edit_{i}")
                    if edit_mode:
                        edited_sql = st.text_area("Edit SQL Query", value=sql_query, height=200, key=f"sql_edit_{i}")
                        if st.button("🔄 Re-run with edited query", key=f"rerun_{i}"):
                            from query import execute_query, analyze_results_with_free_llm
                            with st.spinner("Re-executing query..."):
                                new_results, new_headers = execute_query(edited_sql)
                                new_analysis = analyze_results_with_free_llm(question, new_results, new_headers)
                                
                                st.markdown("### 🔄 Updated Analysis")
                                st.markdown(new_analysis)
                                
                                if new_results and not any("Error" in str(h) for h in new_headers):
                                    df_new = pd.DataFrame(new_results, columns=new_headers)
                                    st.dataframe(df_new.head(50))
                
                # Results
                if success:
                    df = pd.DataFrame(results, columns=headers)
                    
                    st.markdown("### 📊 Detailed Data")
                    st.dataframe(df.head(50))
                    
                    if len(df) > 50:
                        st.info(f"Showing 50 of {len(df)} rows. Total: {len(df)}")
                    
                    # Visualization
                    numeric_cols = df.select_dtypes(include=[np.number]).columns
                    if len(numeric_cols) >= 2:
                        st.markdown("### 📈 Interactive Visualization")
                        col1, col2 = st.columns(2)
                        with col1:
                            x_col = st.selectbox("X-axis", numeric_cols, key=f"x_{i}")
                        with col2:
                            y_col = st.selectbox("Y-axis", numeric_cols, key=f"y_{i}")
                        
                        viz_type = st.selectbox("Chart Type", ["Scatter", "Line"], key=f"viz_{i}")
                        
                        if viz_type == "Scatter":
                            fig = px.scatter(df, x=x_col, y=y_col, 
                                            title=f"Ocean Data: {y_col} vs {x_col}")
                        else:
                            fig = px.line(df, x=x_col, y=y_col, 
                                        title=f"Ocean Data: {y_col} vs {x_col}")
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Download
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "💾 Download Complete Dataset",
                        csv,
                        f"argo_oceanographic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        "text/csv",
                        key=f"download_{i}"
                    )
                else:
                    st.error("Query execution failed - see analysis section for details")
    
    # Clear chat
    if st.sidebar.button("🗑️ Clear Chat History"):
        st.session_state.chat_history = []
        st.rerun()


if __name__ == "__main__":
    main()
