"""Reusable visualization components for DECK Analytics Dashboard - Minimal Notion Style"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# Set default template to ensure charts are visible
pio.templates.default = "plotly_white"

# Notion-Inspired Muted Chart Colors
CHART_COLORS = {
    'primary': '#37352F',         # Warm dark gray (main)
    'secondary': '#787774',       # Medium gray
    'accent': '#2383E2',          # Notion blue (sparingly)
    'success': '#0F7B6C',         # Muted green
    'warning': '#D9730D',         # Muted orange
    'error': '#E03E3E',           # Muted red
    'purple': '#9065B0',          # Muted purple
    'blue': '#2383E2',            # Notion blue
}

# Muted color sequence for multi-line charts
CHART_COLOR_SEQUENCE = ['#37352F', '#787774', '#2383E2', '#9065B0', '#0F7B6C', '#D9730D']

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
    """Create a funnel chart with muted gradient"""
    # Grayscale gradient
    gradient_colors = ['#37352F', '#555550', '#787774', '#9B9A97']

    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        textposition="inside",
        textfont=dict(size=12, color='white', family='Inter, -apple-system, sans-serif'),
        marker=dict(
            color=gradient_colors[:len(stages)],
            line=dict(width=1, color='white')
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
        fillcolor='rgba(55, 53, 47, 0.1)',
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
