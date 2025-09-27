import streamlit as st
import pandas as pd
from orchestrator import OrchestratorService
from models import ChartConfig
from ui_components import UIComponents

def main():
    st.set_page_config(page_title="Argo Oceanographic Analyst", page_icon="🌊", layout="wide")
    st.title("🌊 Argo Oceanographic Analyst")
    orchestrator = OrchestratorService()
    
    # Initialize UI components
    ui = UIComponents()
    
    # Show sidebar information
    ui.show_sidebar_info()
        
    # Sidebar: select LLM mode
    mode = st.sidebar.selectbox(
        "Select LLM Analysis Mode",
        ["Research", "Explore"],
        index=0
    )
    st.sidebar.markdown(f"**Current Mode:** {mode}")
    
    # Show example questions
    ui.show_example_questions()
    
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    if user_input := st.chat_input("Ask an oceanographic question..."):
        st.session_state.chat_history.append(("user", user_input))
        
        with st.spinner("Processing..."):
            response = orchestrator.process_question(user_input, mode.lower())
        
        st.session_state.chat_history.append(("assistant", response))
    
    for i, (msg_type, content) in enumerate(st.session_state.chat_history):
        if msg_type == "user":
            st.chat_message("user").write(content)
            # Show oceanographic contexts for the question
            ui.show_oceanographic_contexts(content)
        else:
            with st.chat_message("assistant"):
                st.markdown("### Analysis")
                st.markdown(content.analysis)
                
                if content.success:
                    df = pd.DataFrame(content.results, columns=content.headers)
                    
                    # Show data metrics
                    ui.show_data_metrics(df)
                    
                    # Show SQL query with edit option
                    edited_sql = ui.show_sql_editor(content.sql, content.question, i)
                    if edited_sql:
                        # Handle re-running query with edited SQL
                        pass
                    
                    # Create and show chart if config exists
                    if content.chart_config:
                        chart = ui.create_chart(df, content.chart_config)
                        if chart:
                            st.plotly_chart(chart, use_container_width=True, key=f"main_chart_{i}")
                    
                    # Show data table
                    st.dataframe(df.head(50))
                    
                    # Allow custom chart creation
                    custom_chart = ui.create_custom_chart_selector(df, i)
                    if custom_chart:
                        st.plotly_chart(custom_chart, use_container_width=True, key=f"custom_chart_{i}")
                    
                    # Add download button
                    ui.create_download_button(df, i)

if __name__ == "__main__":
    main()