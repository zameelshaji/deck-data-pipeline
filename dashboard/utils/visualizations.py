"""Reusable visualization components for DECK Analytics Dashboard"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_line_chart(df, x, y, title, y_label=None):
    """Create a simple line chart"""
    fig = px.line(df, x=x, y=y, title=title)
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=y_label or y,
        hovermode='x unified',
        showlegend=False
    )
    return fig


def create_multi_line_chart(df, x, y_columns, title, y_label=None):
    """Create a multi-line chart"""
    fig = go.Figure()

    for col in y_columns:
        fig.add_trace(go.Scatter(
            x=df[x],
            y=df[col],
            mode='lines',
            name=col.replace('_', ' ').title()
        ))

    fig.update_layout(
        title=title,
        xaxis_title=None,
        yaxis_title=y_label or "Value",
        hovermode='x unified',
        showlegend=True
    )
    return fig


def create_funnel_chart(stages, values, title):
    """Create a funnel chart"""
    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        textposition="inside"
    ))

    fig.update_layout(
        title=title,
        showlegend=False
    )
    return fig


def create_gauge_chart(value, title, max_value=100):
    """Create a gauge chart for percentage metrics"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, max_value * 0.3], 'color': "lightcoral"},
                {'range': [max_value * 0.3, max_value * 0.7], 'color': "lightyellow"},
                {'range': [max_value * 0.7, max_value], 'color': "lightgreen"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))

    fig.update_layout(height=300)
    return fig


def create_bar_chart(df, x, y, title, orientation='v'):
    """Create a bar chart"""
    fig = px.bar(df, x=x, y=y, title=title, orientation=orientation)
    fig.update_layout(
        xaxis_title=None if orientation == 'v' else y,
        yaxis_title=y if orientation == 'v' else None,
        showlegend=False
    )
    return fig


def create_area_chart(df, x, y, title, y_label=None):
    """Create an area chart"""
    fig = px.area(df, x=x, y=y, title=title)
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=y_label or y,
        hovermode='x unified',
        showlegend=False
    )
    return fig


def create_stacked_bar_chart(df, x, y_columns, title):
    """Create a stacked bar chart"""
    fig = go.Figure()

    for col in y_columns:
        fig.add_trace(go.Bar(
            x=df[x],
            y=df[col],
            name=col.replace('_', ' ').title()
        ))

    fig.update_layout(
        title=title,
        barmode='stack',
        xaxis_title=None,
        yaxis_title="Count",
        hovermode='x unified'
    )
    return fig
