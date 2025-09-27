import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime
from typing import List, Optional
from models import OceanographicResponse, ChartConfig

class UIComponents:
    @staticmethod
    def select_mode():
        """Sidebar mode selector"""
        with st.sidebar:
            st.header("Select Mode")
            if "mode" not in st.session_state:
                st.session_state.mode = "explore"  # default
            st.session_state.mode = st.radio(
                "Choose analysis mode:",
                options=["explore", "research"],
                index=0
            )
            st.markdown(f"**Current Mode:** `{st.session_state.mode}`")
    @staticmethod
    def show_oceanographic_contexts(question: str) -> List[str]:
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
        
        if detected_contexts:
            context_tags = " ".join([f"`{ctx}`" for ctx in detected_contexts])
            st.markdown(f"**Oceanographic Context:** {context_tags}")
        
        return detected_contexts if detected_contexts else ['general']
    
    @staticmethod
    def create_chart(df: pd.DataFrame, chart_config: ChartConfig) -> Optional[go.Figure]:
        if chart_config is None or df.empty:
            return None
        
        try:
            if chart_config.chart_type == 'line':
                fig = px.line(df, x=chart_config.x_axis, y=chart_config.y_axis, 
                             title=chart_config.title)
                
                if chart_config.y_reversed:
                    fig.update_yaxes(autorange="reversed")
                
                if chart_config.x_label:
                    fig.update_xaxes(title_text=chart_config.x_label)
                if chart_config.y_label:
                    fig.update_yaxes(title_text=chart_config.y_label)
                    
            elif chart_config.chart_type == 'scatter':
                fig = px.scatter(df, x=chart_config.x_axis, y=chart_config.y_axis, 
                               title=chart_config.title)
                
                if chart_config.x_label:
                    fig.update_xaxes(title_text=chart_config.x_label)
                if chart_config.y_label:
                    fig.update_yaxes(title_text=chart_config.y_label)
                    
            elif chart_config.chart_type == 'scatter_geo':
                fig = px.scatter_geo(df, lat=chart_config.lat_column, lon=chart_config.lon_column, 
                                   title=chart_config.title)
                fig.update_geos(projection_type="natural earth")
                
            else:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) >= 2:
                    fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], 
                                   title=chart_config.title)
                else:
                    return None
            
            return fig
            
        except Exception as e:
            st.error(f"Error creating chart: {str(e)}")
            return None
    
    @staticmethod
    def show_data_metrics(df: pd.DataFrame):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Records", len(df))
        with col2:
            st.metric("Columns", len(df.columns))
        with col3:
            numeric_cols = len(df.select_dtypes(include=[np.number]).columns)
            st.metric("Numeric Columns", numeric_cols)
    
    @staticmethod
    def create_custom_chart_selector(df: pd.DataFrame, i: int) -> Optional[go.Figure]:
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            st.info("Need at least 2 numeric columns for custom charts")
            return None
        
        st.markdown("#### Create Custom Chart")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            x_col = st.selectbox("X-axis", numeric_cols, key=f"custom_x_{i}")
        with col2:
            y_col = st.selectbox("Y-axis", numeric_cols, key=f"custom_y_{i}")
        with col3:
            chart_type = st.selectbox("Chart Type", ["Scatter", "Line"], key=f"custom_type_{i}")
        
        if st.button("Create Chart", key=f"create_chart_{i}"):
            if chart_type == "Scatter":
                custom_fig = px.scatter(df, x=x_col, y=y_col, 
                                      title=f"Custom: {y_col} vs {x_col}")
            else:
                custom_fig = px.line(df, x=x_col, y=y_col, 
                                   title=f"Custom: {y_col} vs {x_col}")
            
            return custom_fig
        
        return None
    
    @staticmethod
    def show_sql_editor(sql: str, question: str, i: int):
        with st.expander("View SQL Query"):
            st.code(sql, language="sql")
            
            if st.checkbox("Edit Query", key=f"edit_{i}"):
                edited_sql = st.text_area("Edit SQL Query", 
                                         value=sql, 
                                         height=150, 
                                         key=f"sql_edit_{i}")
                
                if st.button("Re-run Query", key=f"rerun_{i}"):
                    return edited_sql
        
        return None
    
    @staticmethod
    def show_example_questions():
        with st.expander("Example Oceanographic Questions"):
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
                    if st.button(f"{example}", key=f"ex_{i}"):
                        st.session_state.user_input = example
    
    @staticmethod
    def show_sidebar_info():
        with st.sidebar:
            st.header("System Info")
            st.info("SQL Generation: Gemini Flash 2.5")
            st.info("Analysis: Structured + LLM")
            
            with st.expander("Quality Control Guide"):
                st.text("""
QC Flags:
- 1: Excellent data
- 2: Good data  
- 3-4: Poor/bad data
- 9: Missing data

Use flags 1 & 2 for analysis
                """)
            
            with st.expander("Key Depth Ranges"):
                st.text("""
- Surface: 0-50 dbar
- Thermocline: 10-300 dbar  
- Intermediate: 300-1000 dbar
- Deep: 1000+ dbar
                """)
            
            st.markdown("---")
            st.markdown("### System Components")
            st.markdown("""
            - **SQL Gen**: Gemini 2.0 Flash
            - **Analysis**: Structured extraction
            - **Response**: LLM-generated  
            - **Charts**: Auto-suggested
            - **Database**: DuckDB + Argo data
            """)
    
    @staticmethod
    def create_download_button(df: pd.DataFrame, i: int):
        csv = df.to_csv(index=False)
        st.download_button(
            "Download Dataset",
            csv,
            f"argo_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv",
            key=f"download_{i}"
        )