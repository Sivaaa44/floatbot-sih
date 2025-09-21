import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv
import os
import json
import re
from datetime import datetime
import numpy as np

# Import your existing functions
from query import generate_sql_query, create_system_prompt, load_semantic_model

# Load environment variables
load_dotenv()

# Configure page
st.set_page_config(
    page_title="Argo FloatBot",
    page_icon="🌊",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .stApp {
        max-width: 1400px;
        margin: 0 auto;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .sql-container {
        background-color: #1e1e1e;
        color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 12px;
    }
    .insight-box {
        background-color: #e8f4f8;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #17a2b8;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

class ArgoFloatAnalyzer:
    """Enhanced analyzer with smart query caching and visualization"""
    
    def __init__(self):
        self.db_path = 'argo_floats.db'
        self.query_cache = {}  # Simple in-memory cache
        
        # Predefined smart queries for common patterns
        self.smart_queries = {
            'all_floats': {
                'sql': "SELECT FLOAT_ID, PLATFORM_TYPE, PROJECT_NAME, PI_NAME, LAUNCH_LATITUDE, LAUNCH_LONGITUDE FROM float ORDER BY FLOAT_ID",
                'description': "Basic float information"
            },
            'float_summary': {
                'sql': """SELECT f.FLOAT_ID, f.PLATFORM_TYPE, f.PROJECT_NAME, 
                         COUNT(DISTINCT p.PROFILE_NUMBER) as total_profiles,
                         COUNT(m.LEVEL) as total_measurements,
                         MIN(p.JULD) as first_profile,
                         MAX(p.JULD) as last_profile
                         FROM float f 
                         LEFT JOIN profiles p ON f.FLOAT_ID = p.FLOAT_ID
                         LEFT JOIN measurements m ON p.FLOAT_ID = m.FLOAT_ID AND p.PROFILE_NUMBER = m.PROFILE_NUMBER
                         GROUP BY f.FLOAT_ID, f.PLATFORM_TYPE, f.PROJECT_NAME
                         ORDER BY total_profiles DESC""",
                'description': "Float summary with profile counts"
            },
            'temperature_depth_profile': {
                'sql': """SELECT m.FLOAT_ID, p.PROFILE_NUMBER, m.LEVEL, m.PRES, m.TEMP, m.PSAL,
                         p.LATITUDE, p.LONGITUDE, p.JULD
                         FROM measurements m
                         JOIN profiles p ON m.FLOAT_ID = p.FLOAT_ID AND m.PROFILE_NUMBER = p.PROFILE_NUMBER
                         WHERE m.TEMP IS NOT NULL AND m.PRES IS NOT NULL
                         AND m.TEMP_QC IN ('1', '2') AND m.PRES_QC IN ('1', '2')
                         ORDER BY m.FLOAT_ID, p.PROFILE_NUMBER, m.PRES""",
                'description': "Temperature-depth profiles with quality data"
            },
            'quality_stats': {
                'sql': """SELECT f.FLOAT_ID, f.PLATFORM_TYPE,
                         COUNT(m.LEVEL) as total_measurements,
                         SUM(CASE WHEN m.TEMP_QC IN ('1','2') THEN 1 ELSE 0 END) as good_temp_measurements,
                         SUM(CASE WHEN m.PSAL_QC IN ('1','2') THEN 1 ELSE 0 END) as good_sal_measurements,
                         ROUND(100.0 * SUM(CASE WHEN m.TEMP_QC IN ('1','2') THEN 1 ELSE 0 END) / COUNT(m.LEVEL), 2) as temp_quality_pct
                         FROM float f
                         JOIN profiles p ON f.FLOAT_ID = p.FLOAT_ID
                         JOIN measurements m ON p.FLOAT_ID = m.FLOAT_ID AND p.PROFILE_NUMBER = m.PROFILE_NUMBER
                         GROUP BY f.FLOAT_ID, f.PLATFORM_TYPE
                         ORDER BY temp_quality_pct DESC""",
                'description': "Data quality statistics by float"
            }
        }
    
    def execute_query(self, sql, use_cache=True):
        """Execute query with optional caching"""
        cache_key = hash(sql)
        
        if use_cache and cache_key in self.query_cache:
            return self.query_cache[cache_key]
        
        conn = duckdb.connect(self.db_path)
        try:
            result = conn.execute(sql).fetchall()
            col_names = [desc[0] for desc in conn.description]
            
            if use_cache:
                self.query_cache[cache_key] = (result, col_names)
            
            return result, col_names
        except Exception as e:
            return [("Error", str(e))], ["Error", "Details"]
        finally:
            conn.close()
    
    def detect_query_type(self, user_question):
        """Detect what type of query the user is asking for"""
        question_lower = user_question.lower()
        
        patterns = {
            'profile_data': ['temperature profile', 'depth profile', 'profile for float', 'temp.*depth', 'salinity profile'],
            'float_summary': ['all floats', 'float information', 'basic info', 'list floats'],
            'quality_stats': ['quality', 'data quality', 'qc', 'statistics'],
            'time_series': ['over time', 'time series', 'temporal', 'trend'],
            'spatial': ['map', 'location', 'geographic', 'latitude', 'longitude'],
            'depth_analysis': ['depth', 'pressure', 'vertical', 'deep']
        }
        
        for query_type, keywords in patterns.items():
            if any(re.search(keyword, question_lower) for keyword in keywords):
                return query_type
        
        return 'general'
    
    def create_visualization(self, df, query_type, user_question):
        """Create appropriate visualization based on data and query type"""
        if df.empty:
            return None, "No data available for visualization"
        
        try:
            if query_type == 'profile_data' and 'TEMP' in df.columns and 'PRES' in df.columns:
                return self.create_temperature_depth_plot(df)
            elif query_type == 'spatial' and 'LATITUDE' in df.columns and 'LONGITUDE' in df.columns:
                return self.create_spatial_plot(df)
            elif query_type == 'time_series' and 'JULD' in df.columns:
                return self.create_time_series_plot(df)
            elif 'total_profiles' in df.columns or 'total_measurements' in df.columns:
                return self.create_summary_plot(df)
            else:
                return self.create_generic_plot(df)
        except Exception as e:
            return None, f"Visualization error: {str(e)}"
    
    def create_temperature_depth_plot(self, df):
        """Create temperature vs depth profile plot"""
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Temperature vs Depth', 'Salinity vs Depth'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Group by float and profile for different traces
        for float_id in df['FLOAT_ID'].unique():
            float_data = df[df['FLOAT_ID'] == float_id]
            
            # Temperature plot
            fig.add_trace(
                go.Scatter(
                    x=float_data['TEMP'], 
                    y=float_data['PRES'],
                    mode='lines+markers',
                    name=f'Float {float_id}',
                    hovertemplate='Temp: %{x}°C<br>Pressure: %{y} dbar<br>Float: ' + str(float_id),
                    showlegend=True
                ),
                row=1, col=1
            )
            
            # Salinity plot (if available)
            if 'PSAL' in df.columns and not float_data['PSAL'].isna().all():
                fig.add_trace(
                    go.Scatter(
                        x=float_data['PSAL'], 
                        y=float_data['PRES'],
                        mode='lines+markers',
                        name=f'Float {float_id} Salinity',
                        hovertemplate='Salinity: %{x} PSU<br>Pressure: %{y} dbar<br>Float: ' + str(float_id),
                        showlegend=False
                    ),
                    row=1, col=2
                )
        
        # Invert y-axis for depth representation
        fig.update_yaxes(autorange="reversed", title_text="Pressure (dbar)")
        fig.update_xaxes(title_text="Temperature (°C)", row=1, col=1)
        fig.update_xaxes(title_text="Salinity (PSU)", row=1, col=2)
        
        fig.update_layout(
            height=600,
            title="Ocean Profile Data",
            hovermode='closest'
        )
        
        return fig, "Temperature and salinity profiles showing vertical ocean structure"
    
    def create_spatial_plot(self, df):
        """Create geographic plot of float locations"""
        fig = px.scatter_mapbox(
            df,
            lat="LATITUDE", 
            lon="LONGITUDE",
            hover_data=['FLOAT_ID', 'PLATFORM_TYPE'] if 'PLATFORM_TYPE' in df.columns else ['FLOAT_ID'],
            color='FLOAT_ID' if len(df['FLOAT_ID'].unique()) <= 10 else None,
            mapbox_style="open-street-map",
            height=600,
            title="Float Locations"
        )
        
        fig.update_layout(
            mapbox_zoom=2,
            mapbox_center_lat=df['LATITUDE'].mean(),
            mapbox_center_lon=df['LONGITUDE'].mean()
        )
        
        return fig, "Geographic distribution of Argo floats"
    
    def create_summary_plot(self, df):
        """Create summary bar plots"""
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Profiles per Float', 'Measurements per Float')
        )
        
        if 'total_profiles' in df.columns:
            fig.add_trace(
                go.Bar(x=df['FLOAT_ID'], y=df['total_profiles'], name='Profiles'),
                row=1, col=1
            )
        
        if 'total_measurements' in df.columns:
            fig.add_trace(
                go.Bar(x=df['FLOAT_ID'], y=df['total_measurements'], name='Measurements'),
                row=2, col=1
            )
        
        fig.update_layout(height=600, title="Float Data Summary")
        return fig, "Summary statistics showing data volume per float"
    
    def create_generic_plot(self, df):
        """Create a generic plot for other data"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if len(numeric_cols) >= 2:
            fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], 
                           hover_data=df.columns.tolist()[:5],
                           title=f"{numeric_cols[1]} vs {numeric_cols[0]}")
            return fig, f"Relationship between {numeric_cols[0]} and {numeric_cols[1]}"
        elif len(numeric_cols) == 1:
            fig = px.histogram(df, x=numeric_cols[0], 
                             title=f"Distribution of {numeric_cols[0]}")
            return fig, f"Distribution analysis of {numeric_cols[0]}"
        else:
            return None, "No suitable numeric columns found for visualization"
    
    def generate_insights(self, df, query_type, user_question):
        """Generate intelligent insights from the data"""
        if df.empty:
            return "No data found matching your query."
        
        insights = []
        
        # Basic statistics
        insights.append(f"**Data Overview:** Found {len(df)} records")
        
        if 'FLOAT_ID' in df.columns:
            unique_floats = df['FLOAT_ID'].nunique()
            insights.append(f"**Coverage:** {unique_floats} unique float(s)")
        
        # Query-specific insights
        if query_type == 'profile_data':
            if 'TEMP' in df.columns:
                temp_data = df['TEMP'].dropna()
                if not temp_data.empty:
                    insights.append(f"**Temperature Range:** {temp_data.min():.2f}°C to {temp_data.max():.2f}°C")
                    insights.append(f"**Average Temperature:** {temp_data.mean():.2f}°C")
            
            if 'PRES' in df.columns:
                pres_data = df['PRES'].dropna()
                if not pres_data.empty:
                    insights.append(f"**Depth Range:** Surface to {pres_data.max():.0f} decibars")
        
        elif query_type == 'float_summary':
            if 'total_profiles' in df.columns:
                total_profiles = df['total_profiles'].sum()
                avg_profiles = df['total_profiles'].mean()
                insights.append(f"**Total Profiles:** {total_profiles:,}")
                insights.append(f"**Average Profiles per Float:** {avg_profiles:.1f}")
                
                most_active = df.loc[df['total_profiles'].idxmax()]
                insights.append(f"**Most Active Float:** {most_active['FLOAT_ID']} ({most_active['total_profiles']} profiles)")
        
        elif 'quality' in user_question.lower():
            if 'temp_quality_pct' in df.columns:
                avg_quality = df['temp_quality_pct'].mean()
                insights.append(f"**Average Data Quality:** {avg_quality:.1f}% of measurements pass QC")
        
        # Time-based insights
        if 'first_profile' in df.columns and 'last_profile' in df.columns:
            try:
                date_range = pd.to_datetime(df['last_profile']) - pd.to_datetime(df['first_profile'])
                avg_duration = date_range.dt.days.mean()
                insights.append(f"**Average Mission Duration:** {avg_duration:.0f} days")
            except:
                pass
        
        return "\n\n".join(insights)

# Initialize the analyzer
@st.cache_resource
def get_analyzer():
    return ArgoFloatAnalyzer()

@st.cache_resource
def load_system_components():
    """Load semantic model and system prompt once"""
    semantic_model = load_semantic_model()
    system_prompt = create_system_prompt(semantic_model)
    return semantic_model, system_prompt

# Main app
def main():
    st.title("🌊 Argo FloatBot - Enhanced Analytics")
    st.markdown("Ask questions about oceanographic data and get intelligent insights with visualizations!")
    
    # Load components
    analyzer = get_analyzer()
    semantic_model, system_prompt = load_system_components()
    
    # Sidebar with quick actions
    st.sidebar.header("Quick Actions")
    
    # Predefined queries
    if st.sidebar.button("📊 Float Summary"):
        st.session_state.selected_query = 'float_summary'
        st.session_state.user_input = "Show me a summary of all floats with profile counts"
    
    if st.sidebar.button("🌡️ Temperature Profiles"):
        st.session_state.selected_query = 'temperature_depth_profile'
        st.session_state.user_input = "Show temperature depth profiles for all floats"
    
    if st.sidebar.button("✅ Data Quality"):
        st.session_state.selected_query = 'quality_stats'
        st.session_state.user_input = "Show data quality statistics for all floats"
    
    # Initialize session state
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Example questions
    with st.expander("📋 Example Questions"):
        examples = [
            "Show temperature profiles for float 6903091",
            "Which floats have the most complete data?",
            "Create a map showing all float locations",
            "What's the average temperature at different depths?",
            "Show data quality statistics for all platforms"
        ]
        for i, ex in enumerate(examples):
            if st.button(f"{i+1}. {ex}", key=f"ex_{i}"):
                st.session_state.user_input = ex
    
    # Chat interface
    if user_input := st.chat_input("Ask about Argo float data..."):
        # Add user message
        st.session_state.chat_history.append(("user", user_input))
        
        # Detect query type
        query_type = analyzer.detect_query_type(user_input)
        
        # Check if we can use a predefined query
        if hasattr(st.session_state, 'selected_query') and st.session_state.selected_query in analyzer.smart_queries:
            sql_query = analyzer.smart_queries[st.session_state.selected_query]['sql']
            st.session_state.selected_query = None  # Reset
        else:
            # Generate SQL using AI
            with st.spinner("🤖 Generating SQL..."):
                sql_query = generate_sql_query(user_input, system_prompt)
        
        # Execute query
        with st.spinner("⚡ Executing query..."):
            results, headers = analyzer.execute_query(sql_query)
        
        # Store results
        st.session_state.chat_history.append(("bot", {
            'sql': sql_query,
            'results': results,
            'headers': headers,
            'query_type': query_type,
            'question': user_input
        }))
    
    # Display chat history
    for idx, (msg_type, content) in enumerate(st.session_state.chat_history):
        if msg_type == "user":
            st.chat_message("user").write(content)
        
        elif msg_type == "bot":
            with st.chat_message("assistant"):
                sql_query = content['sql']
                results = content['results']
                headers = content['headers']
                query_type = content['query_type']
                question = content['question']
                
                # Create DataFrame
                df = pd.DataFrame(results, columns=headers)
                
                # Show insights first
                insights = analyzer.generate_insights(df, query_type, question)
                st.markdown("### 📈 Analysis")
                st.markdown(insights)
                
                # Create and show visualization
                if len(df) > 0 and not (len(df) == 1 and 'Error' in headers):
                    fig, fig_description = analyzer.create_visualization(df, query_type, question)
                    
                    if fig:
                        st.markdown("### 📊 Visualization")
                        st.plotly_chart(fig, use_container_width=True)
                        st.caption(fig_description)
                
                # Show data table
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("### 📋 Data Results")
                with col2:
                    # Create CSV data outside button to avoid repeated computation
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        label="📁 Download CSV",
                        data=csv_data,
                        file_name=f"argo_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key=f"download_csv_{idx}"
                    )
                
                # Limit display rows
                display_rows = min(20, len(df))
                st.dataframe(df.head(display_rows), use_container_width=True)
                
                if len(df) > display_rows:
                    st.info(f"Showing {display_rows} of {len(df)} rows")
                
                # Show SQL in expander
                with st.expander("🔍 View SQL Query"):
                    st.code(sql_query, language="sql")
    
    # Clear chat button
    if st.sidebar.button("🗑️ Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()

if __name__ == "__main__":
    main()