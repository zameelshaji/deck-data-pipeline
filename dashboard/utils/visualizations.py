"""Reusable visualization components for DECK Analytics Dashboard - Minimal Notion Style"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# Set default template to ensure charts are visible
pio.templates.default = "plotly_white"

# DECK Pink Theme Chart Colors
CHART_COLORS = {
    'primary': '#E91E8C',         # DECK Hot Pink (main)
    'secondary': '#FF6BB5',       # Light Pink
    'accent': '#D4157A',          # Dark Pink
    'success': '#10B981',         # Green
    'warning': '#F59E0B',         # Orange
    'error': '#EF4444',           # Red
    'purple': '#C7177A',          # Deep Pink
    'blue': '#FF2E9C',            # Bright Pink
}

# Pink color sequence for multi-line charts
CHART_COLOR_SEQUENCE = ['#E91E8C', '#FF6BB5', '#D4157A', '#FF2E9C', '#C7177A', '#FFB3D9']

# Default layout template - Clean and Minimal
DEFAULT_LAYOUT = {
    'plot_bgcolor': '#FFFFFF',
    'paper_bgcolor': '#FFFFFF',
    'font': {'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 12},
    'xaxis': {
        'gridcolor': '#E9E9E7',
        'linecolor': '#E9E9E7',
        'linewidth': 1,
    },
    'yaxis': {
        'gridcolor': '#E9E9E7',
        'linecolor': '#E9E9E7',
        'linewidth': 1,
    },
    'title_font': {'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif'},
    'margin': {'l': 50, 'r': 20, 't': 40, 'b': 40},
}


def create_line_chart(df, x, y, title, y_label=None):
    """Create a clean line chart"""
    fig = px.line(df, x=x, y=y, title=title)

    # Clean, thin line with subtle markers
    fig.update_traces(
        line=dict(width=2, color=CHART_COLORS['primary']),
        marker=dict(size=4, color=CHART_COLORS['primary']),
        fill=None
    )

    fig.update_layout(
        title=title if title else None,
        xaxis_title=None,
        yaxis_title=y_label or y,
        hovermode='x unified',
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 11},
        xaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        yaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        title_font={'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'weight': 600},
        height=400,
        margin={'l': 60, 'r': 20, 't': 50, 'b': 50}
    )
    return fig


def create_multi_line_chart(df, x, y_columns, title, y_label=None):
    """Create a multi-line chart with muted colors"""
    fig = go.Figure()

    for idx, col in enumerate(y_columns):
        fig.add_trace(go.Scatter(
            x=df[x],
            y=df[col],
            mode='lines+markers',
            name=col.replace('_', ' ').title(),
            line=dict(color=CHART_COLOR_SEQUENCE[idx % len(CHART_COLOR_SEQUENCE)], width=2),
            marker=dict(size=4, color=CHART_COLOR_SEQUENCE[idx % len(CHART_COLOR_SEQUENCE)])
        ))

    fig.update_layout(
        title=title if title else None,
        xaxis_title=None,
        yaxis_title=y_label or "Value",
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            font=dict(size=10, color='#787774'),
            bgcolor='rgba(255, 255, 255, 0.95)',
            bordercolor='#E9E9E7',
            borderwidth=1,
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        ),
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 11},
        xaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        yaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        title_font={'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'weight': 600},
        height=400,
        margin={'l': 60, 'r': 20, 't': 50, 'b': 50}
    )
    return fig


def create_funnel_chart(stages, values, title):
    """Create a funnel chart with pink gradient"""
    # Pink gradient from dark to light
    gradient_colors = ['#E91E8C', '#FF2E9C', '#FF6BB5', '#FFB3D9']

    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        textposition="inside",
        textfont=dict(size=12, color='white', family='Inter, -apple-system, sans-serif'),
        marker=dict(
            color=gradient_colors[:len(stages)],
            line=dict(width=2, color='white')
        )
    ))

    fig.update_layout(
        title=title if title else None,
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 11},
        title_font={'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'weight': 600},
        height=350,
        margin={'l': 80, 'r': 80, 't': 50, 'b': 40}
    )
    return fig


def create_gauge_chart(value, title, max_value=100):
    """Create a minimal gauge chart"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 14, 'color': '#37352F'}},
        number={'font': {'size': 32, 'color': '#37352F'}},
        gauge={
            'axis': {'range': [None, max_value], 'tickcolor': '#787774'},
            'bar': {'color': CHART_COLORS['primary'], 'thickness': 0.7},
            'steps': [
                {'range': [0, max_value * 0.3], 'color': '#F7F6F3'},
                {'range': [max_value * 0.3, max_value * 0.7], 'color': '#EFEFEF'},
                {'range': [max_value * 0.7, max_value], 'color': '#E9E9E7'}
            ],
            'threshold': {
                'line': {'color': CHART_COLORS['success'], 'width': 2},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))

    fig.update_layout(
        height=250,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif'},
        margin={'l': 20, 'r': 20, 't': 30, 'b': 20}
    )
    return fig


def create_bar_chart(df, x, y, title, orientation='v'):
    """Create a clean bar chart"""
    fig = px.bar(df, x=x, y=y, title=title, orientation=orientation)

    fig.update_traces(marker_color=CHART_COLORS['primary'], marker_line_width=0)

    fig.update_layout(
        title=title if title else None,
        xaxis_title=None if orientation == 'v' else y,
        yaxis_title=y if orientation == 'v' else None,
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 11},
        xaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': False if orientation == 'v' else True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        yaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True if orientation == 'v' else False,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        title_font={'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'weight': 600},
        height=400,
        margin={'l': 60, 'r': 20, 't': 50, 'b': 50}
    )
    return fig


def create_area_chart(df, x, y, title, y_label=None):
    """Create a subtle area chart"""
    fig = px.area(df, x=x, y=y, title=title)

    fig.update_traces(
        fillcolor='rgba(233, 30, 140, 0.1)',  # Light pink fill
        line=dict(color=CHART_COLORS['primary'], width=2)
    )

    fig.update_layout(
        title=title if title else None,
        xaxis_title=None,
        yaxis_title=y_label or y,
        hovermode='x unified',
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 11},
        xaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        yaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        title_font={'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'weight': 600},
        height=400,
        margin={'l': 60, 'r': 20, 't': 50, 'b': 50}
    )
    return fig


def create_stacked_bar_chart(df, x, y_columns, title):
    """Create a stacked bar chart with muted colors"""
    fig = go.Figure()

    colors = [CHART_COLORS['primary'], CHART_COLORS['secondary'], CHART_COLORS['accent'],
              CHART_COLORS['success'], CHART_COLORS['warning'], CHART_COLORS['error']]

    for idx, col in enumerate(y_columns):
        fig.add_trace(go.Bar(
            x=df[x],
            y=df[col],
            name=col.replace('_', ' ').title(),
            marker=dict(color=colors[idx % len(colors)], line=dict(width=0))
        ))

    fig.update_layout(
        title=title if title else None,
        barmode='stack',
        xaxis_title=None,
        yaxis_title="Count",
        hovermode='x unified',
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'size': 11},
        legend=dict(
            font=dict(size=10, color='#787774'),
            bgcolor='rgba(255, 255, 255, 0.95)',
            bordercolor='#E9E9E7',
            borderwidth=1
        ),
        xaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': False,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        yaxis={
            'gridcolor': '#E9E9E7',
            'linecolor': '#E9E9E7',
            'linewidth': 1,
            'showgrid': True,
            'tickfont': {'size': 10, 'color': '#787774'}
        },
        title_font={'size': 14, 'color': '#37352F', 'family': 'Inter, -apple-system, sans-serif', 'weight': 600},
        height=400,
        margin={'l': 60, 'r': 20, 't': 50, 'b': 50}
    )
    return fig

def create_monthly_cohort_heatmap(df):
    """
    Create a monthly cohort retention heatmap
    
    Args:
        df: DataFrame with cohort_month, months_since_signup, and retention_rate columns
    
    Returns:
        Plotly figure
    """
    import pandas as pd
    
    # Pivot data for heatmap
    pivot_df = df.pivot(
        index='cohort_month', 
        columns='months_since_signup', 
        values='retention_rate'
    )
    
    # Format cohort month labels (e.g., "Jan 2025")
    cohort_labels = [pd.to_datetime(idx).strftime('%b %Y') for idx in pivot_df.index]
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=['M' + str(int(col)) for col in pivot_df.columns],
        y=cohort_labels,
        colorscale=[
            [0, '#FFE5F3'],      # Light pink for low retention
            [0.3, '#FFB3D9'],    # Medium light pink
            [0.6, '#FF4FA3'],    # Medium pink
            [1, '#E91E8C']       # DECK pink for high retention
        ],
        text=pivot_df.values,
        texttemplate='%{text:.1f}%',
        textfont={"size": 10, "color": "#1A1A1A"},
        colorbar=dict(
            title="Retention %",
            titleside="right",
            tickmode="linear",
            tick0=0,
            dtick=20,
            titlefont={"size": 12}
        ),
        hovertemplate='Cohort: %{y}<br>Month: %{x}<br>Retention: %{z:.1f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title="Monthly Cohort Retention Heatmap",
        xaxis_title="Months Since Signup",
        yaxis_title="Cohort (Signup Month)",
        height=550,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Poppins, sans-serif', 'size': 11},
        title_font={'size': 16, 'weight': 600, 'color': '#E91E8C'},
        xaxis={
            'gridcolor': '#FFE5F3',
            'linecolor': '#FFE5F3',
            'tickfont': {'size': 11},
            'title_font': {'size': 12, 'weight': 600}
        },
        yaxis={
            'gridcolor': '#FFE5F3',
            'linecolor': '#FFE5F3',
            'tickfont': {'size': 11},
            'title_font': {'size': 12, 'weight': 600},
            'autorange': 'reversed'  # Most recent cohorts at top
        },
        margin={'l': 100, 'r': 50, 't': 50, 'b': 50}
    )
    
    return fig


def create_monthly_retention_curve(df):
    """
    Create monthly retention curve showing average retention over time
    
    Args:
        df: DataFrame with months_since_signup and retention_rate columns
    
    Returns:
        Plotly figure
    """
    import pandas as pd
    
    # Calculate average retention by month
    avg_retention = df.groupby('months_since_signup')['retention_rate'].mean().reset_index()
    
    # Calculate min/max for range
    min_retention = df.groupby('months_since_signup')['retention_rate'].min().reset_index()
    max_retention = df.groupby('months_since_signup')['retention_rate'].max().reset_index()
    
    fig = go.Figure()
    
    # Add range (min to max) as shaded area
    fig.add_trace(go.Scatter(
        x=list(min_retention['months_since_signup']) + list(max_retention['months_since_signup'][::-1]),
        y=list(min_retention['retention_rate']) + list(max_retention['retention_rate'][::-1]),
        fill='toself',
        fillcolor='rgba(233, 30, 140, 0.1)',
        line=dict(color='rgba(255,255,255,0)'),
        name='Range',
        showlegend=True,
        hoverinfo='skip'
    ))
    
    # Add average retention line
    fig.add_trace(go.Scatter(
        x=avg_retention['months_since_signup'],
        y=avg_retention['retention_rate'],
        mode='lines+markers',
        name='Average Retention',
        line=dict(color='#E91E8C', width=3),
        marker=dict(size=10, color='#E91E8C', line=dict(width=2, color='#FFFFFF')),
        hovertemplate='Month %{x}<br>Avg Retention: %{y:.1f}%<extra></extra>'
    ))
    
    # Add individual cohort lines (lighter, for context) - show last 6 cohorts
    unique_cohorts = sorted(df['cohort_month'].unique(), reverse=True)[:6]
    for cohort in unique_cohorts:
        cohort_data = df[df['cohort_month'] == cohort].sort_values('months_since_signup')
        cohort_label = pd.to_datetime(cohort).strftime('%b %Y')
        
        fig.add_trace(go.Scatter(
            x=cohort_data['months_since_signup'],
            y=cohort_data['retention_rate'],
            mode='lines',
            name=f'{cohort_label}',
            line=dict(color='#FFB3D9', width=1.5, dash='dot'),
            opacity=0.4,
            hovertemplate=f'Cohort {cohort_label}<br>Month %{{x}}<br>Retention: %{{y:.1f}}%<extra></extra>'
        ))
    
    fig.update_layout(
        title="Monthly Retention Curve",
        xaxis_title="Months Since Signup",
        yaxis_title="Retention Rate (%)",
        height=450,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Poppins, sans-serif', 'size': 11},
        title_font={'size': 16, 'weight': 600, 'color': '#E91E8C'},
        xaxis={
            'gridcolor': '#FFE5F3',
            'linecolor': '#FFE5F3',
            'dtick': 1,
            'tickfont': {'size': 11},
            'title_font': {'size': 12, 'weight': 600}
        },
        yaxis={
            'gridcolor': '#FFE5F3',
            'linecolor': '#FFE5F3',
            'range': [0, 100],
            'tickfont': {'size': 11},
            'title_font': {'size': 12, 'weight': 600}
        },
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            bgcolor='rgba(255, 255, 255, 0.9)',
            bordercolor='#FFE5F3',
            borderwidth=1
        )
    )
    
    return fig


def create_retention_comparison_chart(weekly_df, monthly_df):
    """
    Create a comparison chart showing both weekly and monthly retention curves
    
    Args:
        weekly_df: DataFrame with weekly retention data
        monthly_df: DataFrame with monthly retention data
    
    Returns:
        Plotly figure
    """
    # Calculate averages
    weekly_avg = weekly_df.groupby('weeks_since_signup')['retention_rate'].mean().reset_index()
    monthly_avg = monthly_df.groupby('months_since_signup')['retention_rate'].mean().reset_index()
    
    # Convert weeks to months for comparison (approximate: 4 weeks = 1 month)
    weekly_avg['months_equivalent'] = weekly_avg['weeks_since_signup'] / 4.33
    
    fig = go.Figure()
    
    # Add weekly retention line
    fig.add_trace(go.Scatter(
        x=weekly_avg['months_equivalent'],
        y=weekly_avg['retention_rate'],
        mode='lines+markers',
        name='Weekly View (Granular)',
        line=dict(color='#FF4FA3', width=2, dash='dot'),
        marker=dict(size=6, color='#FF4FA3'),
        hovertemplate='~Month %{x:.1f}<br>Retention: %{y:.1f}%<extra></extra>'
    ))
    
    # Add monthly retention line
    fig.add_trace(go.Scatter(
        x=monthly_avg['months_since_signup'],
        y=monthly_avg['retention_rate'],
        mode='lines+markers',
        name='Monthly View',
        line=dict(color='#E91E8C', width=3),
        marker=dict(size=10, color='#E91E8C', line=dict(width=2, color='#FFFFFF')),
        hovertemplate='Month %{x}<br>Retention: %{y:.1f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title="Retention Comparison: Weekly vs Monthly View",
        xaxis_title="Months Since Signup",
        yaxis_title="Retention Rate (%)",
        height=400,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Poppins, sans-serif'},
        title_font={'size': 16, 'weight': 600, 'color': '#E91E8C'},
        xaxis={
            'gridcolor': '#FFE5F3',
            'linecolor': '#FFE5F3',
            'dtick': 1
        },
        yaxis={
            'gridcolor': '#FFE5F3',
            'linecolor': '#FFE5F3',
            'range': [0, 100]
        },
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig
