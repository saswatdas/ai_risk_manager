# frontend/app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

# Excel file configuration
EXCEL_FILE_PATH = "/Users/saswatdas/ai_agent_project/risk_analysis_report.xlsx"

def load_data_from_excel():
    """Load data from Excel file."""
    try:
        df = pd.read_excel(EXCEL_FILE_PATH)
        return df
    except Exception as e:
        st.error(f"Error loading data from Excel: {e}")
        return pd.DataFrame()

def fetch_projects():
    """Fetch list of all projects from Excel data."""
    df = load_data_from_excel()
    if df.empty:
        return []
    
    projects = df[['project_id', 'project_name']].drop_duplicates().to_dict('records')
    return projects

def fetch_all_assessments():
    """Fetch all assessments to get available dates."""
    df = load_data_from_excel()
    if df.empty:
        return []
    
    all_dates = sorted(df['rating_date'].unique().tolist(), reverse=True)
    return all_dates

def fetch_latest_assessments(project_id):
    """Fetch latest risk assessments for a project."""
    df = load_data_from_excel()
    if df.empty:
        return []
    
    project_data = df[df['project_id'] == project_id]
    if project_data.empty:
        return []
    
    # Get the latest date for this project
    latest_date = project_data['rating_date'].max()
    latest_assessments = project_data[project_data['rating_date'] == latest_date]
    
    return latest_assessments.to_dict('records')

def fetch_project_assessments(project_id):
    """Fetch all historical assessments for a project."""
    df = load_data_from_excel()
    if df.empty:
        return []
    
    project_assessments = df[df['project_id'] == project_id]
    return project_assessments.to_dict('records')

def fetch_assessments_by_date(assessment_date):
    """Fetch all assessments for a specific date across all projects."""
    df = load_data_from_excel()
    if df.empty:
        return []
    
    date_assessments = df[df['rating_date'] == assessment_date]
    return date_assessments.to_dict('records')

def fetch_projects_with_assessment_date(assessment_date):
    """Fetch projects that have assessments on a specific date."""
    assessments = fetch_assessments_by_date(assessment_date)
    if not assessments:
        return []
    
    project_ids = set(assessment['project_id'] for assessment in assessments)
    projects = fetch_projects()
    filtered_projects = [p for p in projects if p['project_id'] in project_ids]
    
    return filtered_projects

def get_available_dates(assessments):
    """Get unique assessment dates from assessments."""
    if not assessments:
        return []
    
    dates = sorted(set(assessment['rating_date'] for assessment in assessments), reverse=True)
    return dates

def calculate_overall_health(assessments):
    """Calculate overall project health based on ratings."""
    if not assessments:
        return "Unknown"
    
    ratings = [assessment['rating'] for assessment in assessments]
    
    if 'Red' in ratings:
        return 'Red'
    elif 'Amber' in ratings:
        return 'Amber'
    elif all(rating == 'Green' for rating in ratings):
        return 'Green'
    else:
        return 'Unknown'

def calculate_historical_health_trend(assessments):
    """Calculate overall health for each assessment date."""
    if not assessments:
        return []
    
    # Group assessments by date
    assessments_by_date = {}
    for assessment in assessments:
        date_str = assessment['rating_date']
        if date_str not in assessments_by_date:
            assessments_by_date[date_str] = []
        assessments_by_date[date_str].append(assessment)
    
    # Calculate overall health for each date
    health_trend = []
    for date_str, date_assessments in assessments_by_date.items():
        overall_health = calculate_overall_health(date_assessments)
        health_trend.append({
            'date': date_str,
            'overall_health': overall_health,
            'assessment_count': len(date_assessments)
        })
    
    # Sort by date
    health_trend.sort(key=lambda x: x['date'])
    return health_trend

def get_health_icon(rating):
    """Get icon for health rating."""
    if rating == 'Green':
        return '✅'
    elif rating == 'Amber':
        return '🟡'
    elif rating == 'Red':
        return '🔴'
    else:
        return '❓'

def get_health_color(rating):
    """Get color for health rating."""
    if rating == 'Green':
        return '#6BCB77'
    elif rating == 'Amber':
        return '#FFD93D'
    elif rating == 'Red':
        return '#FF6B6B'
    else:
        return '#CCCCCC'

def get_health_numeric(rating):
    """Convert health rating to numeric value for plotting."""
    if rating == 'Green':
        return 1
    elif rating == 'Amber':
        return 2
    elif rating == 'Red':
        return 3
    else:
        return 0

def create_health_trend_chart(health_trend):
    """Create a line chart showing health trend over time."""
    if not health_trend:
        return None
    
    df = pd.DataFrame(health_trend)
    df['date'] = pd.to_datetime(df['date'])
    df['health_numeric'] = df['overall_health'].apply(get_health_numeric)
    
    # Create the trend chart
    fig = go.Figure()
    
    # Add line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['health_numeric'],
        mode='lines+markers',
        line=dict(color='#0072B2', width=3),
        marker=dict(size=8),
        name='Overall Health Trend'
    ))
    
    # Add colored background regions
    fig.add_hrect(y0=0.5, y1=1.5, fillcolor="#6BCB77", opacity=0.2, line_width=0)
    fig.add_hrect(y0=1.5, y1=2.5, fillcolor="#FFD93D", opacity=0.2, line_width=0)
    fig.add_hrect(y0=2.5, y1=3.5, fillcolor="#FF6B6B", opacity=0.2, line_width=0)
    
    # Update layout
    fig.update_layout(
        title="Overall Health Trend Over Time",
        xaxis_title="Assessment Date",
        yaxis_title="Health Status",
        yaxis=dict(
            tickmode='array',
            tickvals=[1, 2, 3],
            ticktext=['Green', 'Amber', 'Red'],
            range=[0.5, 3.5]
        ),
        height=400,
        showlegend=False
    )
    
    return fig

def main():
    st.set_page_config(
        page_title="Project Risk Dashboard",
        page_icon="📊", 
        initial_sidebar_state="expanded"
    )

    # Initialize session state
    if 'selected_project_id' not in st.session_state:
        st.session_state.selected_project_id = None
    if 'selected_date' not in st.session_state:
        st.session_state.selected_date = None
    if 'all_dates' not in st.session_state:
        st.session_state.all_dates = []
    if 'filtered_projects' not in st.session_state:
        st.session_state.filtered_projects = []

    # Check Excel data connection
    try:
        df = load_data_from_excel()
        data_loaded = not df.empty
        if not data_loaded:
            st.error("❌ No data found in Excel file. Please ensure the file exists and contains data.")
            return
    except Exception as e:
        st.error(f"❌ Error loading Excel file: {e}")
        return

    # Sidebar for filters
    with st.sidebar:
        st.header("📅 Assessment Date Filter")
        
        if not data_loaded:
            st.error("❌ Cannot load data from Excel file.")
            return
        
        # Fetch all available dates
        if not st.session_state.all_dates:
            st.session_state.all_dates = fetch_all_assessments()
        
        # Date selection
        date_options = ["All Dates"] + st.session_state.all_dates
        selected_date = st.selectbox(
            "Select Assessment Date:",
            options=date_options,
            index=0
        )
        
        if selected_date == "All Dates":
            st.session_state.selected_date = None
            # Show all projects
            all_projects = fetch_projects()
            st.session_state.filtered_projects = all_projects
        else:
            st.session_state.selected_date = selected_date
            # Filter projects for selected date
            st.session_state.filtered_projects = fetch_projects_with_assessment_date(selected_date)
        
        st.markdown("---")
        st.header("📋 Project Selection")
        
        # Project selection based on date filter
        if not st.session_state.filtered_projects:
            st.warning("No projects available for the selected date.")
            project_options = {"Select a project...": None}
        else:
            # Create dropdown options with health status
            project_options = {"Select a project...": None}
            
            for project in st.session_state.filtered_projects:
                # Get health status for the selected date or latest
                if st.session_state.selected_date:
                    assessments = fetch_assessments_by_date(st.session_state.selected_date)
                    project_assessments = [a for a in assessments if a['project_id'] == project['project_id']]
                    overall_health = calculate_overall_health(project_assessments)
                else:
                    assessments = fetch_latest_assessments(project['project_id'])
                    overall_health = calculate_overall_health(assessments)
                
                health_icon = get_health_icon(overall_health)
                project_options[f"{health_icon} {project['project_id']} - {project['project_name']}"] = project['project_id']
        
        # Project selection dropdown
        selected_option = st.selectbox(
            "Choose Project:",
            options=list(project_options.keys()),
            index=0,
            key="project_selector"
        )
        
        st.session_state.selected_project_id = project_options[selected_option]
        
        st.markdown("---")
        st.header("📊 Portfolio Health")
        
        # Calculate overall statistics for filtered projects
        if st.session_state.filtered_projects:
            health_counts = {'Green': 0, 'Amber': 0, 'Red': 0, 'Unknown': 0}
            
            for project in st.session_state.filtered_projects:
                if st.session_state.selected_date:
                    assessments = fetch_assessments_by_date(st.session_state.selected_date)
                    project_assessments = [a for a in assessments if a['project_id'] == project['project_id']]
                    overall_health = calculate_overall_health(project_assessments)
                else:
                    assessments = fetch_latest_assessments(project['project_id'])
                    overall_health = calculate_overall_health(assessments)
                
                health_counts[overall_health] += 1
            
            # Display health metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("🔴 At Risk", health_counts['Red'])
            with col2:
                st.metric("🟡 Warning", health_counts['Amber'])
            with col3:
                st.metric("✅ Healthy", health_counts['Green'])
            
            # Health distribution pie chart
            total_projects = len(st.session_state.filtered_projects)
            if total_projects > 0:
                health_data = {
                    'Status': ['Healthy', 'Warning', 'At Risk'],
                    'Count': [health_counts['Green'], health_counts['Amber'], health_counts['Red']],
                    'Color': ['#6BCB77', '#FFD93D', '#FF6B6B']
                }
                
                fig = px.pie(
                    health_data,
                    values='Count',
                    names='Status',
                    title="Portfolio Health Distribution",
                    color='Status',
                    color_discrete_map={'Healthy': '#6BCB77', 'Warning': '#FFD93D', 'At Risk': '#FF6B6B'}
                )
                st.plotly_chart(fig, use_container_width=True)

    # Main content area
    if st.session_state.selected_project_id is None:
        # Show overall health for filtered projects
        st.subheader("🏢 Project Portfolio Dashboard")
        
        if st.session_state.selected_date:
            st.markdown(f"##### Showing assessments for: {st.session_state.selected_date}")
        else:
            st.markdown("##### Showing latest assessments across all dates")
        
        st.markdown("---")
        
        if not st.session_state.filtered_projects:
            st.info("No projects available for the selected filters.")
            return
        
        # Project health overview table
        st.subheader("📋 Project Health Overview")
        
        # Create overview data
        overview_data = []
        for project in st.session_state.filtered_projects:
            if st.session_state.selected_date:
                assessments = fetch_assessments_by_date(st.session_state.selected_date)
                project_assessments = [a for a in assessments if a['project_id'] == project['project_id']]
                overall_health = calculate_overall_health(project_assessments)
                assessment_date = st.session_state.selected_date
            else:
                assessments = fetch_latest_assessments(project['project_id'])
                overall_health = calculate_overall_health(assessments)
                assessment_date = assessments[0]['rating_date'] if assessments else 'N/A'
            
            overview_data.append({
                'Project ID': project['project_id'],
                'Project Name': project['project_name'],
                'Overall Health': overall_health,
                'Assessment Date': assessment_date,
                'Health Icon': get_health_icon(overall_health)
            })
        
        overview_df = pd.DataFrame(overview_data)
        
        # Color function for overall health
        def color_health(val):
            if val == 'Green':
                return 'background-color: #90EE90; color: black;'
            elif val == 'Amber':
                return 'background-color: #FFE4B5; color: black;'
            elif val == 'Red':
                return 'background-color: #FFB6C1; color: black;'
            return ''
        
        # Display overview table
        display_df = overview_df[['Project ID', 'Project Name', 'Overall Health', 'Assessment Date']]
        st.dataframe(
            display_df.style.applymap(color_health, subset=['Overall Health']),
            use_container_width=True,
            height=200
        )
        
    else:
        # Project detail page
        selected_project = next((p for p in st.session_state.filtered_projects 
                               if p['project_id'] == st.session_state.selected_project_id), None)
        
        if selected_project:
            selected_project_name = selected_project['project_name']
            
            # Get assessments based on date filter
            if st.session_state.selected_date:
                assessments = fetch_assessments_by_date(st.session_state.selected_date)
                project_assessments = [a for a in assessments if a['project_id'] == st.session_state.selected_project_id]
                assessment_date = st.session_state.selected_date
                date_info = f"Specific Assessment: {assessment_date}"
            else:
                project_assessments = fetch_latest_assessments(st.session_state.selected_project_id)
                assessment_date = project_assessments[0]['rating_date'] if project_assessments else 'N/A'
                date_info = f"Latest Assessment: {assessment_date}"
            
            st.subheader(f"📊 {selected_project_name}")
            st.markdown(f"**Project ID:** {st.session_state.selected_project_id}")
            st.markdown(f"**{date_info}**")
            st.markdown("---")
            
            if project_assessments:
                # Calculate overall health
                overall_health = calculate_overall_health(project_assessments)
                health_icon = get_health_icon(overall_health)
                health_color = get_health_color(overall_health)
                
                # Display overall health banner
                st.markdown(f"""
                    <div style="background-color: {health_color}; padding: 2px; border-radius: 6px; text-align: center; margin-bottom: 2px;">
                        <h5 style="color: white; margin: 0; font-size: 10px;">
                             Overall Project Health: {health_icon} {overall_health}
                        </h5>
            
                    </div>
                """, unsafe_allow_html=True)

                # Risk Assessment Details
                st.subheader("🔍 Risk Assessment Details")
                
                df = pd.DataFrame(project_assessments)
                
                def color_rating(rating):
                    if rating == 'Green':
                        return 'background-color: #90EE90'
                    elif rating == 'Amber':
                        return 'background-color: #FFE4B5'
                    elif rating == 'Red':
                        return 'background-color: #FFB6C1'
                    return ''
                
                display_df = df[['optic_name', 'rating', 'justification', 'rating_date']].copy()
                display_df.columns = ['Optics Name', 'Optics Rating', 'Assessment Rationale', 'Assessment Date']
                
                styled_df = display_df.style.applymap(color_rating, subset=['Optics Rating'])
                
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    height=400
                )
                
                # Health Trend Section
                all_assessments = fetch_project_assessments(st.session_state.selected_project_id)
                health_trend = calculate_historical_health_trend(all_assessments)
                
                if health_trend:
                    st.header("📈 Health Trend Over Time")
                    
                    col_trend1, col_trend2 = st.columns([2, 1])
                    
                    with col_trend1:
                        trend_chart = create_health_trend_chart(health_trend)
                        if trend_chart:
                            st.plotly_chart(trend_chart, use_container_width=True)
                    
                    with col_trend2:
                        st.subheader("Health History")
                        health_df = pd.DataFrame(health_trend)
                        health_summary = health_df['overall_health'].value_counts()
                        
                        for health_status in ['Green', 'Amber', 'Red']:
                            count = health_summary.get(health_status, 0)
                            icon = get_health_icon(health_status)
                            st.metric(f"{icon} {health_status}", count)
                        
                        st.metric("Total Assessments", len(health_trend))
                
                # Risk distribution
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.header("Risk Summary")
                    rating_counts = pd.DataFrame(project_assessments)['rating'].value_counts()
                    
                    metric_col1, metric_col2, metric_col3 = st.columns(3)
                    with metric_col1:
                        st.metric("🔴 Red", rating_counts.get('Red', 0))
                    with metric_col2:
                        st.metric("🟡 Amber", rating_counts.get('Amber', 0))
                    with metric_col3:
                        st.metric("✅ Green", rating_counts.get('Green', 0))
                    
                    if not rating_counts.empty:
                        fig_pie = px.pie(
                            values=rating_counts.values,
                            names=rating_counts.index,
                            title="Risk Distribution",
                            color=rating_counts.index,
                            color_discrete_map={'Red': '#FF6B6B', 'Amber': '#FFD93D', 'Green': '#6BCB77'}
                        )
                        st.plotly_chart(fig_pie, use_container_width=True)
                
                with col2:
                    st.header("Risk Details")
                    red_risks = [r for r in project_assessments if r['rating'] == 'Red']
                    if red_risks:
                        st.subheader("🔴 Critical Risks")
                        for risk in red_risks:
                            with st.expander(f"{risk['optic_name']}"):
                                st.write(f"**Justification:** {risk['justification']}")
                    
                    amber_risks = [r for r in project_assessments if r['rating'] == 'Amber']
                    if amber_risks:
                        st.subheader("🟡 Warning Risks")
                        for risk in amber_risks:
                            with st.expander(f"{risk['optic_name']}"):
                                st.write(f"**Justification:** {risk['justification']}")
                
            else:
                st.warning("No risk assessments found for this project.")
        else:
            st.error("Selected project not found.")

if __name__ == "__main__":
    main()