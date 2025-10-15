"""Test page to verify charts render correctly"""

import streamlit as st
import plotly.express as px
import pandas as pd

st.title("Chart Rendering Test")

# Create simple test data
df = pd.DataFrame({
    'x': [1, 2, 3, 4, 5],
    'y': [10, 20, 15, 25, 30]
})

st.write("Test data:")
st.dataframe(df)

# Test 1: Simple Plotly Express chart (no custom styling)
st.subheader("Test 1: Simple Line Chart (Plotly Express)")
fig1 = px.line(df, x='x', y='y', title='Simple Line Chart')
fig1.update_traces(line=dict(color='#E91E8C', width=4))
st.plotly_chart(fig1, use_container_width=True)

# Test 2: Explicit styling
st.subheader("Test 2: Line Chart with Explicit Styling")
fig2 = px.line(df, x='x', y='y')
fig2.update_traces(
    line=dict(color='#E91E8C', width=6),
    marker=dict(size=12, color='#E91E8C')
)
fig2.update_layout(
    title='Styled Line Chart',
    plot_bgcolor='white',
    paper_bgcolor='white',
    showlegend=False
)
st.plotly_chart(fig2, use_container_width=True)

# Test 3: Bar chart
st.subheader("Test 3: Bar Chart")
fig3 = px.bar(df, x='x', y='y', title='Bar Chart')
fig3.update_traces(marker_color='#E91E8C')
st.plotly_chart(fig3, use_container_width=True)

st.write("If you can see all three charts above, Plotly rendering works!")
st.write("If not, there's a rendering or browser issue.")
