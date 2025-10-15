"""Reusable visualization components for DECK Analytics Dashboard"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# Set default template to ensure charts are visible
pio.templates.default = "plotly_white"

# DECK Brand Colors for charts - BOLD PINK PALETTE
CHART_COLORS = {
    'primary': '#E91E8C',
    'secondary': '#C7177A',
    'accent': '#FF4FA3',
    'success': '#10B981',
    'warning': '#F59E0B',
    'error': '#EF4444',
    'purple': '#A855F7',
    'blue': '#3B82F6',
}

# Bold color sequence for multi-line charts - PINK FIRST
CHART_COLOR_SEQUENCE = ['#E91E8C', '#FF4FA3', '#C7177A', '#A855F7', '#3B82F6', '#10B981']

# Default layout template for all charts - PINK ACCENTS
DEFAULT_LAYOUT = {
    'plot_bgcolor': '#FFFFFF',
    'paper_bgcolor': '#FAFAFA',
    'font': {'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 12},
    'xaxis': {
        'gridcolor': '#F5F5F5',
        'linecolor': '#E91E8C',
        'linewidth': 2,
    },
    'yaxis': {
        'gridcolor': '#F5F5F5',
        'linecolor': '#E91E8C',
        'linewidth': 2,
    },
    'title_font': {'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
    'margin': {'l': 60, 'r': 30, 't': 60, 'b': 50},
}


def create_line_chart(df, x, y, title, y_label=None):
    """Create a simple line chart with BOLD PINK"""
    fig = px.line(df, x=x, y=y, title=title)

    # Make the line thicker and more visible with explicit pink color
    fig.update_traces(
        line=dict(width=4, color=CHART_COLORS['primary']),
        marker=dict(size=8, color=CHART_COLORS['primary'], line=dict(width=2, color='white')),
        fill=None
    )

    fig.update_layout(
        title=title,
        xaxis_title=None,
        yaxis_title=y_label or y,
        hovermode='x unified',
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 13},
        xaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        yaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        title_font={'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
        height=500,
        margin={'l': 80, 'r': 40, 't': 80, 'b': 80}
    )
    return fig


def create_multi_line_chart(df, x, y_columns, title, y_label=None):
    """Create a multi-line chart with BOLD COLORS"""
    fig = go.Figure()

    for idx, col in enumerate(y_columns):
        fig.add_trace(go.Scatter(
            x=df[x],
            y=df[col],
            mode='lines+markers',
            name=col.replace('_', ' ').title(),
            line=dict(color=CHART_COLOR_SEQUENCE[idx % len(CHART_COLOR_SEQUENCE)], width=3),
            marker=dict(size=6, color=CHART_COLOR_SEQUENCE[idx % len(CHART_COLOR_SEQUENCE)], line=dict(width=1, color='white'))
        ))

    fig.update_layout(
        title=title,
        xaxis_title=None,
        yaxis_title=y_label or "Value",
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            font=dict(size=12),
            bgcolor='rgba(255, 255, 255, 0.9)',
            bordercolor=CHART_COLORS['primary'],
            borderwidth=2,
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        ),
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 13},
        xaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        yaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        title_font={'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
        height=500,
        margin={'l': 80, 'r': 40, 't': 80, 'b': 80}
    )
    return fig


def create_funnel_chart(stages, values, title):
    """Create a funnel chart with PINK GRADIENT"""
    # Create gradient colors from light to dark pink
    gradient_colors = ['#FF4FA3', '#E91E8C', '#C7177A', '#A01561']

    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        textposition="inside",
        textfont=dict(size=16, color='white', family='Inter', weight='bold'),
        marker=dict(
            color=gradient_colors[:len(stages)],
            line=dict(width=3, color='white')
        )
    ))

    fig.update_layout(
        title=title,
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 13},
        title_font={'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
        height=450,
        margin={'l': 100, 'r': 100, 't': 60, 'b': 50}
    )
    return fig


def create_gauge_chart(value, title, max_value=100):
    """Create a gauge chart for percentage metrics"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 18, 'color': '#E91E8C'}},
        number={'font': {'size': 40, 'color': '#E91E8C'}},
        gauge={
            'axis': {'range': [None, max_value], 'tickcolor': '#1A1A1A'},
            'bar': {'color': CHART_COLORS['primary'], 'thickness': 0.8},
            'steps': [
                {'range': [0, max_value * 0.3], 'color': '#FEE2E2'},
                {'range': [max_value * 0.3, max_value * 0.7], 'color': '#FEF3C7'},
                {'range': [max_value * 0.7, max_value], 'color': '#D1FAE5'}
            ],
            'threshold': {
                'line': {'color': CHART_COLORS['error'], 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))

    fig.update_layout(
        height=300,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif'},
        margin={'l': 20, 'r': 20, 't': 40, 'b': 20}
    )
    return fig


def create_bar_chart(df, x, y, title, orientation='v'):
    """Create a bar chart"""
    fig = px.bar(df, x=x, y=y, title=title, orientation=orientation)

    fig.update_traces(marker_color=CHART_COLORS['primary'])

    fig.update_layout(
        title=title,
        xaxis_title=None if orientation == 'v' else y,
        yaxis_title=y if orientation == 'v' else None,
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 13},
        xaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        yaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        title_font={'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
        height=500,
        margin={'l': 80, 'r': 40, 't': 80, 'b': 80}
    )
    return fig


def create_area_chart(df, x, y, title, y_label=None):
    """Create an area chart"""
    fig = px.area(df, x=x, y=y, title=title)

    fig.update_traces(
        fillcolor='rgba(233, 30, 140, 0.3)',
        line=dict(color=CHART_COLORS['primary'], width=3)
    )

    fig.update_layout(
        title=title,
        xaxis_title=None,
        yaxis_title=y_label or y,
        hovermode='x unified',
        showlegend=False,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 13},
        xaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        yaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        title_font={'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
        height=500,
        margin={'l': 80, 'r': 40, 't': 80, 'b': 80}
    )
    return fig


def create_stacked_bar_chart(df, x, y_columns, title):
    """Create a stacked bar chart"""
    fig = go.Figure()

    colors = [CHART_COLORS['primary'], CHART_COLORS['secondary'], CHART_COLORS['accent'],
              CHART_COLORS['success'], CHART_COLORS['warning'], CHART_COLORS['error']]

    for idx, col in enumerate(y_columns):
        fig.add_trace(go.Bar(
            x=df[x],
            y=df[col],
            name=col.replace('_', ' ').title(),
            marker=dict(color=colors[idx % len(colors)], line=dict(width=1, color='white'))
        ))

    fig.update_layout(
        title=title,
        barmode='stack',
        xaxis_title=None,
        yaxis_title="Count",
        hovermode='x unified',
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#FFFFFF',
        font={'color': '#1A1A1A', 'family': 'Inter, sans-serif', 'size': 13},
        xaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        yaxis={
            'gridcolor': '#F5F5F5',
            'linecolor': '#E91E8C',
            'linewidth': 2,
            'showgrid': True,
            'tickfont': {'size': 12}
        },
        title_font={'size': 18, 'color': '#E91E8C', 'family': 'Inter, sans-serif'},
        height=500,
        margin={'l': 80, 'r': 40, 't': 80, 'b': 80}
    )
    return fig
