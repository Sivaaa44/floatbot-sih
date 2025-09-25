# # import streamlit as st
# # import pandas as pd
# # import plotly.express as px
# # import numpy as np
# # from datetime import datetime
# # from query import orchestrator


# # def analyze_oceanographic_question(question):
# #     """Analyze question to determine oceanographic context"""
# #     if not question:
# #         return ['general']
        
# #     question_lower = question.lower()
    
# #     contexts = {
# #         'water_mass': ['water mass', 't-s diagram', 'temperature salinity', 'potential temperature'],
# #         'vertical_structure': ['thermocline', 'temperature gradient', 'stratification', 'mixed layer'],
# #         'spatial_analysis': ['geographic', 'spatial', 'location', 'regional', 'map', 'across', 'vary'],
# #         'temporal_analysis': ['time series', 'seasonal', 'temporal', 'over time', 'trend'],
# #         'quality_control': ['quality', 'qc', 'data quality', 'flags'],
# #         'profile_analysis': ['profile', 'depth', 'vertical', 'pressure', 'surface', 'deepest', 'temperature difference']
# #     }
    
# #     detected_contexts = []
# #     for context, keywords in contexts.items():
# #         if any(keyword in question_lower for keyword in keywords):
# #             detected_contexts.append(context)
    
# #     return detected_contexts if detected_contexts else ['general']


# # def main():
# #     st.set_page_config(page_title="Argo Oceanographic Analyst", page_icon="🌊", layout="wide")
    
# #     st.title("🌊 Argo Oceanographic Analyst")
# #     st.markdown("*Professional oceanographic data analysis powered by AI*")
    
# #     # Sidebar
# #     with st.sidebar:
# #         st.header("🔬 System Info")
# #         st.info("SQL Generation: Gemini Flash 2.5")
# #         st.info("Result Analysis: Free Text Model")
        
# #         with st.expander("📚 Quality Control Guide"):
# #             st.text("""
# # QC Flags:
# # • 1: Excellent data
# # • 2: Good data  
# # • 3-4: Poor/bad data
# # • 9: Missing data

# # Use flags 1 & 2 for analysis
# #             """)
        
# #         with st.expander("🌊 Key Depth Ranges"):
# #             st.text("""
# # • Surface: 0-50 dbar
# # • Thermocline: 10-300 dbar  
# # • Intermediate: 300-1000 dbar
# # • Deep: 1000+ dbar
# #             """)
    
# #     # Chat history
# #     if 'chat_history' not in st.session_state:
# #         st.session_state.chat_history = []
    
# #     # Example questions
# #     with st.expander("🧭 Example Oceanographic Questions"):
# #         examples = [
# #             "How does surface temperature vary across different float locations?",
# #             "Show me all floats and their basic information",
# #             "What is the average temperature and salinity for each float with good quality data?",
# #             "Show temperature profiles for float '6903016'",
# #             "How much has temperature fallen from surface to deepest point in profile 1 of float 6903016?",
# #             "Find the deepest measurement (highest pressure) for each float",
# #             "Which floats have the most profiles?",
# #             "Show data quality statistics for temperature and salinity measurements"
# #         ]
        
# #         cols = st.columns(2)
# #         for i, example in enumerate(examples):
# #             with cols[i % 2]:
# #                 if st.button(f"📋 {example}", key=f"ex_{i}"):
# #                     st.session_state.user_input = example
    
# #     # Chat input
# #     if user_input := st.chat_input("Ask an oceanographic question..."):
# #         st.session_state.chat_history.append(("user", user_input))
        
# #         with st.spinner("🔄 Processing your oceanographic query..."):
# #             result = orchestrator(user_input)
        
# #         st.session_state.chat_history.append(("assistant", result))
    
# #     # Display conversation
# #     for i, (msg_type, content) in enumerate(st.session_state.chat_history):
# #         if msg_type == "user":
# #             st.chat_message("user").write(content)
# #         else:
# #             with st.chat_message("assistant"):
# #                 sql_query = content['sql']
# #                 results = content['results']
# #                 headers = content['headers']
# #                 analysis = content['analysis']
# #                 success = content['success']
# #                 question = content.get('question', '')
                
# #                 # Oceanographic context tags
# #                 contexts = analyze_oceanographic_question(question)
# #                 if contexts:
# #                     context_tags = " ".join([f"`{ctx}`" for ctx in contexts])
# #                     st.markdown(f"**Oceanographic Context:** {context_tags}")
                
# #                 # Analysis
# #                 st.markdown("### 💡 Oceanographic Analysis")
# #                 st.markdown(analysis)
                
# #                 # SQL query
# #                 with st.expander("📝 View Generated SQL Query"):
# #                     st.code(sql_query, language="sql")
                    
# #                     edit_mode = st.checkbox("✏️ Edit Query", key=f"edit_{i}")
# #                     if edit_mode:
# #                         edited_sql = st.text_area("Edit SQL Query", value=sql_query, height=200, key=f"sql_edit_{i}")
# #                         if st.button("🔄 Re-run with edited query", key=f"rerun_{i}"):
# #                             from query import execute_query, analyze_results_with_free_llm
# #                             with st.spinner("Re-executing query..."):
# #                                 new_results, new_headers = execute_query(edited_sql)
# #                                 new_analysis = analyze_results_with_free_llm(question, new_results, new_headers)
                                
# #                                 st.markdown("### 🔄 Updated Analysis")
# #                                 st.markdown(new_analysis)
                                
# #                                 if new_results and not any("Error" in str(h) for h in new_headers):
# #                                     df_new = pd.DataFrame(new_results, columns=new_headers)
# #                                     st.dataframe(df_new.head(50))
                
# #                 # Results
# #                 if success:
# #                     df = pd.DataFrame(results, columns=headers)
                    
# #                     st.markdown("### 📊 Detailed Data")
# #                     st.dataframe(df.head(50))
                    
# #                     if len(df) > 50:
# #                         st.info(f"Showing 50 of {len(df)} rows. Total: {len(df)}")
                    
# #                     # Visualization
# #                     numeric_cols = df.select_dtypes(include=[np.number]).columns
# #                     if len(numeric_cols) >= 2:
# #                         st.markdown("### 📈 Interactive Visualization")
# #                         col1, col2 = st.columns(2)
# #                         with col1:
# #                             x_col = st.selectbox("X-axis", numeric_cols, key=f"x_{i}")
# #                         with col2:
# #                             y_col = st.selectbox("Y-axis", numeric_cols, key=f"y_{i}")
                        
# #                         viz_type = st.selectbox("Chart Type", ["Scatter", "Line"], key=f"viz_{i}")
                        
# #                         if viz_type == "Scatter":
# #                             fig = px.scatter(df, x=x_col, y=y_col, 
# #                                             title=f"Ocean Data: {y_col} vs {x_col}")
# #                         else:
# #                             fig = px.line(df, x=x_col, y=y_col, 
# #                                         title=f"Ocean Data: {y_col} vs {x_col}")
                        
# #                         st.plotly_chart(fig, use_container_width=True)
                    
# #                     # Download
# #                     csv = df.to_csv(index=False)
# #                     st.download_button(
# #                         "💾 Download Complete Dataset",
# #                         csv,
# #                         f"argo_oceanographic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
# #                         "text/csv",
# #                         key=f"download_{i}"
# #                     )
# #                 else:
# #                     st.error("Query execution failed - see analysis section for details")
    
# #     # Clear chat
# #     if st.sidebar.button("🗑️ Clear Chat History"):
# #         st.session_state.chat_history = []
# #         st.rerun()


# # if __name__ == "__main__":
# #     main()



# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import numpy as np
# from datetime import datetime
# from query import orchestrator

# def create_chart_from_config(df, chart_config):
#     """Create a Plotly chart based on configuration"""
#     if not chart_config or df.empty:
#         return None
    
#     chart_type = chart_config.get('type', 'scatter')
#     title = chart_config.get('title', 'Chart')
    
#     try:
#         if chart_type == 'line':
#             x_col = chart_config['x']
#             y_col = chart_config['y']
            
#             fig = px.line(df, x=x_col, y=y_col, title=title)
            
#             # Reverse y-axis for depth profiles
#             if chart_config.get('y_reversed', False):
#                 fig.update_yaxes(autorange="reversed")
            
#             # Update labels
#             if 'x_label' in chart_config:
#                 fig.update_xaxes(title_text=chart_config['x_label'])
#             if 'y_label' in chart_config:
#                 fig.update_yaxes(title_text=chart_config['y_label'])
                
#         elif chart_type == 'scatter':
#             x_col = chart_config['x']
#             y_col = chart_config['y']
            
#             fig = px.scatter(df, x=x_col, y=y_col, title=title)
            
#             if 'x_label' in chart_config:
#                 fig.update_xaxes(title_text=chart_config['x_label'])
#             if 'y_label' in chart_config:
#                 fig.update_yaxes(title_text=chart_config['y_label'])
                
#         elif chart_type == 'scatter_geo':
#             lat_col = chart_config['lat']
#             lon_col = chart_config['lon']
            
#             fig = px.scatter_geo(df, lat=lat_col, lon=lon_col, title=title)
#             fig.update_geos(projection_type="natural earth")
            
#         else:
#             # Default scatter plot
#             numeric_cols = df.select_dtypes(include=[np.number]).columns
#             if len(numeric_cols) >= 2:
#                 fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title=title)
#             else:
#                 return None
        
#         return fig
        
#     except Exception as e:
#         st.error(f"Error creating chart: {str(e)}")
#         return None

# def analyze_oceanographic_question(question):
#     """Analyze question to determine oceanographic context"""
#     if not question:
#         return ['general']
        
#     question_lower = question.lower()
    
#     contexts = {
#         'water_mass': ['water mass', 't-s diagram', 'temperature salinity', 'potential temperature'],
#         'vertical_structure': ['thermocline', 'temperature gradient', 'stratification', 'mixed layer'],
#         'spatial_analysis': ['geographic', 'spatial', 'location', 'regional', 'map', 'across', 'vary'],
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
#     st.markdown("*Professional oceanographic data analysis powered by AI*")
    
#     # Sidebar
#     with st.sidebar:
#         st.header("🔬 System Info")
#         st.info("SQL Generation: Gemini Flash 2.5")
#         st.info("Analysis: Structured + LLM")
        
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
    
#     # Chat history
#     if 'chat_history' not in st.session_state:
#         st.session_state.chat_history = []
    
#     # Example questions
#     with st.expander("🧭 Example Oceanographic Questions"):
#         examples = [
#             "How does surface temperature vary across different float locations?",
#             "Show me all floats and their basic information",
#             "What is the average temperature and salinity for each float with good quality data?",
#             "Show temperature profiles for float '6903016'",
#             "How much has temperature fallen from surface to deepest point in profile 1 of float 6903016?",
#             "Find the deepest measurement (highest pressure) for each float",
#             "Which floats have the most profiles?",
#             "Show data quality statistics for temperature and salinity measurements"
#         ]
        
#         cols = st.columns(2)
#         for i, example in enumerate(examples):
#             with cols[i % 2]:
#                 if st.button(f"📋 {example}", key=f"ex_{i}"):
#                     st.session_state.user_input = example
    
#     # Chat input
#     if user_input := st.chat_input("Ask an oceanographic question..."):
#         st.session_state.chat_history.append(("user", user_input))
        
#         with st.spinner("🔄 Processing your oceanographic query..."):
#             result = orchestrator(user_input)
        
#         st.session_state.chat_history.append(("assistant", result))
    
#     # Display conversation
#     for i, (msg_type, content) in enumerate(st.session_state.chat_history):
#         if msg_type == "user":
#             st.chat_message("user").write(content)
#         else:
#             with st.chat_message("assistant"):
#                 sql_query = content['sql']
#                 results = content['results']
#                 headers = content['headers']
#                 analysis = content['analysis']
#                 chart_config = content.get('chart_config')
#                 success = content['success']
#                 question = content.get('question', '')
                
#                 # Oceanographic context tags
#                 contexts = analyze_oceanographic_question(question)
#                 if contexts:
#                     context_tags = " ".join([f"`{ctx}`" for ctx in contexts])
#                     st.markdown(f"**Oceanographic Context:** {context_tags}")
                
#                 # Main analysis response
#                 st.markdown("### 💡 Analysis")
#                 st.markdown(analysis)
                
#                 # Chart visualization
#                 if success and chart_config:
#                     st.markdown("### 📈 Visualization")
#                     df = pd.DataFrame(results, columns=headers)
                    
#                     chart = create_chart_from_config(df, chart_config)
#                     if chart:
#                         st.plotly_chart(chart, use_container_width=True)
#                     else:
#                         st.info("Chart configuration generated but visualization failed. Check data types.")
                
#                 # Interactive data exploration (only for successful queries)
#                 if success and results:
#                     df = pd.DataFrame(results, columns=headers)
                    
#                     st.markdown("### 📊 Data Explorer")
                    
#                     # Show basic info
#                     col1, col2, col3 = st.columns(3)
#                     with col1:
#                         st.metric("Records", len(df))
#                     with col2:
#                         st.metric("Columns", len(df.columns))
#                     with col3:
#                         numeric_cols = len(df.select_dtypes(include=[np.number]).columns)
#                         st.metric("Numeric Columns", numeric_cols)
                    
#                     # Show data
#                     st.dataframe(df.head(50))
                    
#                     if len(df) > 50:
#                         st.info(f"Showing 50 of {len(df)} rows.")
                    
#                     # Custom visualization option
#                     if len(df.select_dtypes(include=[np.number]).columns) >= 2:
#                         st.markdown("#### Create Custom Chart")
#                         numeric_cols = df.select_dtypes(include=[np.number]).columns
                        
#                         col1, col2, col3 = st.columns(3)
#                         with col1:
#                             x_col = st.selectbox("X-axis", numeric_cols, key=f"custom_x_{i}")
#                         with col2:
#                             y_col = st.selectbox("Y-axis", numeric_cols, key=f"custom_y_{i}")
#                         with col3:
#                             chart_type = st.selectbox("Chart Type", ["Scatter", "Line"], key=f"custom_type_{i}")
                        
#                         if st.button("Create Chart", key=f"create_chart_{i}"):
#                             if chart_type == "Scatter":
#                                 custom_fig = px.scatter(df, x=x_col, y=y_col, 
#                                                       title=f"Custom: {y_col} vs {x_col}")
#                             else:
#                                 custom_fig = px.line(df, x=x_col, y=y_col, 
#                                                    title=f"Custom: {y_col} vs {x_col}")
                            
#                             st.plotly_chart(custom_fig, use_container_width=True)
                    
#                     # Download data
#                     csv = df.to_csv(index=False)
#                     st.download_button(
#                         "💾 Download Dataset",
#                         csv,
#                         f"argo_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
#                         "text/csv",
#                         key=f"download_{i}"
#                     )
                
#                 # SQL Query section
#                 with st.expander("📝 View SQL Query"):
#                     st.code(sql_query, language="sql")
                    
#                     # SQL editing option
#                     if st.checkbox("✏️ Edit Query", key=f"edit_{i}"):
#                         edited_sql = st.text_area("Edit SQL Query", 
#                                                  value=sql_query, 
#                                                  height=150, 
#                                                  key=f"sql_edit_{i}")
                        
#                         if st.button("🔄 Re-run Query", key=f"rerun_{i}"):
#                             from query import execute_query, DataAnalyzer, ResponseGenerator
                            
#                             with st.spinner("Re-executing query..."):
#                                 new_results, new_headers = execute_query(edited_sql)
                                
#                                 if new_results and not any("Error" in str(h) for h in new_headers):
#                                     from query import QueryResult
                                    
#                                     # Create new query result
#                                     new_query_result = QueryResult(
#                                         sql=edited_sql,
#                                         data=new_results,
#                                         headers=new_headers,
#                                         success=True
#                                     )
                                    
#                                     # Re-analyze
#                                     analyzer = DataAnalyzer()
#                                     new_analysis = analyzer.analyze_data(question, new_query_result)
                                    
#                                     response_gen = ResponseGenerator()
#                                     new_text_response = response_gen.generate_response(question, new_analysis)
                                    
#                                     st.markdown("### 🔄 Updated Analysis")
#                                     st.markdown(new_text_response)
                                    
#                                     # Show updated data
#                                     new_df = pd.DataFrame(new_results, columns=new_headers)
#                                     st.dataframe(new_df.head(50))
#                                 else:
#                                     st.error("Query execution failed")
                
#                 # Error handling for failed queries
#                 if not success:
#                     st.error("❌ Query execution failed - check the SQL query or database connection")
    
#     # Clear chat history
#     if st.sidebar.button("🗑️ Clear Chat History"):
#         st.session_state.chat_history = []
#         st.rerun()
    
#     # Footer with system info
#     st.sidebar.markdown("---")
#     st.sidebar.markdown("### 🔧 System Components")
#     st.sidebar.markdown("""
#     - **SQL Gen**: Gemini 2.0 Flash
#     - **Analysis**: Structured extraction
#     - **Response**: LLM-generated  
#     - **Charts**: Auto-suggested
#     - **Database**: DuckDB + Argo data
#     """)

# if __name__ == "__main__":
#     main()